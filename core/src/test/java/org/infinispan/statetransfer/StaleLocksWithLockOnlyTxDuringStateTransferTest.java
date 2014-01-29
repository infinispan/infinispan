package org.infinispan.statetransfer;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.BlockingInterceptor;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.distribution.TxDistributionInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionTable;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import javax.transaction.TransactionManager;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

@Test(testName = "lock.StaleLocksWithLockOnlyTxDuringStateTransferTest", groups = "functional")
@CleanupAfterMethod
public class StaleLocksWithLockOnlyTxDuringStateTransferTest extends MultipleCacheManagersTest {
   public static final String CACHE_NAME = "testCache";

   @Override
   protected void createCacheManagers() throws Throwable {
      addClusterEnabledCacheManager();
      addClusterEnabledCacheManager();
      waitForClusterToForm();
   }

   public void testSync() throws Throwable {
      doTest(CacheMode.DIST_SYNC);
   }

   public void testAsync() throws Throwable {
      doTest(CacheMode.DIST_ASYNC);
   }

   private void doTest(CacheMode cacheMode) throws Throwable {
      ConfigurationBuilder cfg = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      cfg.clustering().cacheMode(cacheMode)
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
      doAnswer(new Answer<Object>() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            Object[] arguments = invocation.getArguments();
            Address source = (Address) arguments[0];
            int topologyId = (Integer) arguments[1];
            checkpoint.trigger("pre_get_transactions_" + topologyId + "_from_" + source);
            checkpoint.awaitStrict("resume_get_transactions_" + topologyId + "_from_" + source, 10, SECONDS);
            return invocation.callRealMethod();
         }
      }).when(spyProvider).getTransactionsForSegments(any(Address.class), anyInt(), anySetOf(Integer.class));
      doAnswer(new Answer<Object>() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            Object[] arguments = invocation.getArguments();
            CacheTopology topology = (CacheTopology) arguments[0];
            checkpoint.trigger("pre_ch_update_" + topology.getTopologyId());
            checkpoint.awaitStrict("pre_ch_update_" + topology.getTopologyId(), 10, SECONDS);
            return invocation.callRealMethod();
         }
      }).when(spyProvider).onTopologyUpdate(any(CacheTopology.class), eq(false));
      TestingUtil.replaceComponent(cache0, StateProvider.class, spyProvider, true);

      // Block prepare commands on cache 0
      CyclicBarrier prepareBarrier = new CyclicBarrier(2);
      cache0.addInterceptorBefore(new BlockingInterceptor(prepareBarrier, PrepareCommand.class, false),
            TxDistributionInterceptor.class);
      StateTransferManager stm0 = TestingUtil.extractComponent(cache0, StateTransferManager.class);
      int initialTopologyId = stm0.getCacheTopology().getTopologyId();

      // Start cache 1, but the state request will be blocked on cache 0
      int rebalanceTopologyId = initialTopologyId + 1;
      AdvancedCache<Object, Object> cache1 = advancedCache(1, CACHE_NAME);
      checkpoint.awaitStrict("pre_get_transactions_" + rebalanceTopologyId + "_from_" + address(1), 10, SECONDS);

      // Start a transaction on cache 0, which will block just before the distribution interceptor
      Future<Object> future = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            MagicKey key = new MagicKey("testkey", cache0);
            tm0.begin();
            cache0.lock(key);
            tm0.commit();
            return null;
         }
      });

      // Wait for the prepare to lock the key
      prepareBarrier.await(10, SECONDS);

      // Let cache 0 push the tx to cache 1. The CH update will block.
      checkpoint.trigger("resume_get_transactions_" + rebalanceTopologyId + "_from_" + address(1));

      // Let the tx finish. Because the CH update is blocked, the topology won't change.
      prepareBarrier.await(10, SECONDS);
      future.get(10, SECONDS);

      // Let the rebalance finish
      int finalTopologyId = rebalanceTopologyId + 1;
      checkpoint.trigger("resume_ch_update_" + finalTopologyId);
      TestingUtil.waitForRehashToComplete(caches(CACHE_NAME));

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
