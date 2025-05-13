package org.infinispan.statetransfer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.fail;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.MagicKey;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestException;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.impl.TransactionTable;
import org.testng.annotations.Test;

import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

@Test(testName = "lock.StaleTxWithCommitDuringStateTransferTest", groups = "functional")
@CleanupAfterMethod
public class StaleTxWithCommitDuringStateTransferTest extends MultipleCacheManagersTest {
   public static final String CACHE_NAME = "testCache";

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(TestDataSCI.INSTANCE, new ConfigurationBuilder(), 2);
      waitForClusterToForm();
   }

   public void testCommit() throws Throwable {
      doTest(true);
   }

   public void testRollback() throws Throwable {
      doTest(false);
   }

   private void doTest(final boolean commit) throws Throwable {
      ConfigurationBuilder cfg = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      cfg.clustering().cacheMode(CacheMode.DIST_SYNC)
            .stateTransfer().awaitInitialTransfer(false)
            .transaction().lockingMode(LockingMode.PESSIMISTIC);
      manager(0).defineConfiguration(CACHE_NAME, cfg.build());
      manager(1).defineConfiguration(CACHE_NAME, cfg.build());

      final CheckPoint checkpoint = new CheckPoint();
      final AdvancedCache<Object, Object> cache0 = advancedCache(0, CACHE_NAME);
      final TransactionManager tm0 = cache0.getTransactionManager();

      // Block state request commands on cache 0
      StateProvider stateProvider = TestingUtil.extractComponent(cache0, StateProvider.class);
      StateProvider spyProvider = spy(stateProvider);
      doAnswer(invocation -> {
         Object[] arguments = invocation.getArguments();
         Address source = (Address) arguments[0];
         int topologyId = (Integer) arguments[1];
         CompletionStage<?> result = (CompletionStage<?>) invocation.callRealMethod();
         return result.thenApply(transactions -> {
            try {
               checkpoint.trigger("post_get_transactions_" + topologyId + "_from_" + source);
               checkpoint.awaitStrict("resume_get_transactions_" + topologyId + "_from_" + source, 10, SECONDS);
               return transactions;
            } catch (InterruptedException | TimeoutException e) {
               throw new TestException(e);
            }
         });
      }).when(spyProvider).getTransactionsForSegments(any(Address.class), anyInt(), any());
      TestingUtil.replaceComponent(cache0, StateProvider.class, spyProvider, true);

      // Start a transaction on cache 0, which will block on cache 1
      MagicKey key = new MagicKey("testkey", cache0);
      tm0.begin();
      cache0.put(key, "v0");
      final Transaction tx = tm0.suspend();

      // Start cache 1, but the tx data request will be blocked on cache 0
      DistributionManager dm0 = cache0.getDistributionManager();
      int initialTopologyId = dm0.getCacheTopology().getTopologyId();
      int rebalanceTopologyId = initialTopologyId + 1;
      AdvancedCache<Object, Object> cache1 = advancedCache(1, CACHE_NAME);
      checkpoint.awaitStrict("post_get_transactions_" + rebalanceTopologyId + "_from_" + address(1), 10, SECONDS);

      // The commit/rollback command should be invoked on cache 1 and it should block until the tx is created there
      Future<Object> future = fork(() -> {
         tm0.resume(tx);
         if (commit) {
            tm0.commit();
         } else {
            tm0.rollback();
         }
         return null;
      });

      // Check that the rollback command is blocked on cache 1
      try {
         future.get(1, SECONDS);
         fail("Commit/Rollback command should have been blocked");
      } catch (TimeoutException e) {
         // expected;
      }

      // Let cache 1 receive the tx from cache 0.
      checkpoint.trigger("resume_get_transactions_" + rebalanceTopologyId + "_from_" + address(1));
      TestingUtil.waitForNoRebalance(caches(CACHE_NAME));

      // Wait for the tx finish
      future.get(10, SECONDS);

      // Check the key on all caches
      if (commit) {
         assertEquals("v0", TestingUtil.extractComponent(cache0, InternalDataContainer.class).peek(key).getValue());
         assertEquals("v0", TestingUtil.extractComponent(cache1, InternalDataContainer.class).peek(key).getValue());
      } else {
         assertNull(TestingUtil.extractComponent(cache0, InternalDataContainer.class).peek(key));
         assertNull(TestingUtil.extractComponent(cache1, InternalDataContainer.class).peek(key));
      }

      // Check for stale locks
      final TransactionTable tt0 = TestingUtil.extractComponent(cache0, TransactionTable.class);
      final TransactionTable tt1 = TestingUtil.extractComponent(cache1, TransactionTable.class);
      eventually(() -> tt0.getLocalTxCount() == 0 && tt1.getRemoteTxCount() == 0);
   }
}
