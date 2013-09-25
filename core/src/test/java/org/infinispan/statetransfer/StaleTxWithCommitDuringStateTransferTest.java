package org.infinispan.statetransfer;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.EntryWrappingInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionTable;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.fail;

@Test(testName = "lock.StaleTxWithCommitDuringStateTransferTest", groups = "functional")
@CleanupAfterMethod
public class StaleTxWithCommitDuringStateTransferTest extends MultipleCacheManagersTest {
   public static final String CACHE_NAME = "testCache";

   @Override
   protected void createCacheManagers() throws Throwable {
      addClusterEnabledCacheManager();
      addClusterEnabledCacheManager();
      waitForClusterToForm();
   }

   public void testSyncCommit() throws Throwable {
      doTest(true, true);
   }

   public void testSyncRollback() throws Throwable {
      doTest(true, false);
   }

   public void testAsyncCommit() throws Throwable {
      doTest(false, true);
   }

   public void testAsyncRollback() throws Throwable {
      doTest(false, false);
   }

   private void doTest(final boolean sync, final boolean commit) throws Throwable {
      ConfigurationBuilder cfg = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      cfg.clustering().cacheMode(CacheMode.DIST_SYNC)
            .stateTransfer().awaitInitialTransfer(false)
            .transaction().lockingMode(LockingMode.PESSIMISTIC).syncCommitPhase(sync).syncRollbackPhase(sync);
      manager(0).defineConfiguration(CACHE_NAME, cfg.build());
      manager(1).defineConfiguration(CACHE_NAME, cfg.build());

      final CheckPoint checkpoint = new CheckPoint();
      final AdvancedCache<Object, Object> cache0 = advancedCache(0, CACHE_NAME);
      final TransactionManager tm0 = cache0.getTransactionManager();

      // Block state request commands on cache 0
      StateProvider stateProvider = TestingUtil.extractComponent(cache0, StateProvider.class);
      StateProvider spyProvider = spy(stateProvider);
      TestingUtil.replaceComponent(cache0, StateProvider.class, spyProvider, true);
      doAnswer(new Answer<Object>() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            Object[] arguments = invocation.getArguments();
            Address source = (Address) arguments[0];
            int topologyId = (Integer) arguments[1];
            Object result = invocation.callRealMethod();
            checkpoint.trigger("post_get_transactions_" + topologyId + "_from_" + source);
            checkpoint.awaitStrict("resume_get_transactions_" + topologyId + "_from_" + source, 10, SECONDS);
            return result;
         }
      }).when(spyProvider).getTransactionsForSegments(any(Address.class), anyInt(), anySetOf(Integer.class));

      // Start a transaction on cache 0, which will block on cache 1
      MagicKey key = new MagicKey("testkey", cache0);
      tm0.begin();
      cache0.put(key, "v0");
      final Transaction tx = tm0.suspend();

      // Start cache 1, but the tx data request will be blocked on cache 0
      StateTransferManager stm0 = TestingUtil.extractComponent(cache0, StateTransferManager.class);
      int initialTopologyId = stm0.getCacheTopology().getTopologyId();
      int rebalanceTopologyId = initialTopologyId + 1;
      AdvancedCache<Object, Object> cache1 = advancedCache(1, CACHE_NAME);
      checkpoint.awaitStrict("post_get_transactions_" + rebalanceTopologyId + "_from_" + address(1), 10, SECONDS);

      // The commit/rollback command should be invoked on cache 1 and it should block until the tx is created there
      Future<Object> future = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            tm0.resume(tx);
            if (commit) {
               tm0.commit();
            } else {
               tm0.rollback();
            }
            return null;
         }
      });

      if (sync) {
         // Check that the rollback command is blocked on cache 1
         try {
            future.get(1, SECONDS);
            fail("Commit/Rollback command should have been blocked");
         } catch (TimeoutException e) {
            // expected;
         }
      } else {
         // Give the rollback command some time to execute on cache 1
         Thread.sleep(1000);
      }

      // Let cache 1 receive the tx from cache 0.
      checkpoint.trigger("resume_get_transactions_" + rebalanceTopologyId + "_from_" + address(1));
      TestingUtil.waitForRehashToComplete(caches(CACHE_NAME));

      // Wait for the tx finish
      future.get(10, SECONDS);

      // Check the key on all caches
      if (commit) {
         assertEquals("v0", TestingUtil.extractComponent(cache0, DataContainer.class).get(key).getValue());
         assertEquals("v0", TestingUtil.extractComponent(cache1, DataContainer.class).get(key).getValue());
      } else {
         assertNull(TestingUtil.extractComponent(cache0, DataContainer.class).get(key));
         assertNull(TestingUtil.extractComponent(cache1, DataContainer.class).get(key));
      }

      // Check for stale locks
      final TransactionTable tt0 = TestingUtil.extractComponent(cache0, TransactionTable.class);
      final TransactionTable tt1 = TestingUtil.extractComponent(cache1, TransactionTable.class);
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return tt0.getLocalTxCount() == 0 && tt1.getRemoteTxCount() == 0;
         }
      });
   }
}