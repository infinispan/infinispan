package org.infinispan.statetransfer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.infinispan.AdvancedCache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestException;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.impl.TransactionTable;
import org.testng.annotations.Test;

import jakarta.transaction.TransactionManager;

@Test(testName = "lock.ManyTxsDuringStateTransferTest", groups = "functional")
@CleanupAfterMethod
public class ManyTxsDuringStateTransferTest extends MultipleCacheManagersTest {
   public static final String CACHE_NAME = "testCache";
   private static final int NUM_TXS = 20;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultBuilder = new ConfigurationBuilder();
      addClusterEnabledCacheManager(getGlobalConfigurationBuilder(), defaultBuilder);
      addClusterEnabledCacheManager(getGlobalConfigurationBuilder(), defaultBuilder);
      waitForClusterToForm();
   }

   private GlobalConfigurationBuilder getGlobalConfigurationBuilder() {
      return GlobalConfigurationBuilder.defaultClusteredBuilder();
   }

   public void testManyTxs() throws Throwable {
      ConfigurationBuilder cfg = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      cfg.clustering().cacheMode(CacheMode.DIST_SYNC)
            .stateTransfer().awaitInitialTransfer(false)
            .transaction().lockingMode(LockingMode.OPTIMISTIC);
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

      // Start cache 1, but the tx data request will be blocked on cache 0
      DistributionManager dm0 = cache0.getDistributionManager();
      int initialTopologyId = dm0.getCacheTopology().getTopologyId();
      int rebalanceTopologyId = initialTopologyId + 1;
      AdvancedCache<Object, Object> cache1 = advancedCache(1, CACHE_NAME);
      checkpoint.awaitStrict("post_get_transactions_" + rebalanceTopologyId + "_from_" + address(1), 10, SECONDS);

      // Start many transaction on cache 0, which will block on cache 1
      Future<Object>[] futures = new Future[NUM_TXS];
      for (int i = 0; i < NUM_TXS; i++) {
         // The rollback command should be invoked on cache 1 and it should block until the tx is created there
         final int ii = i;
         futures[i] = fork(() -> {
            tm0.begin();
            cache0.put("testkey" + ii, "v" + ii);
            tm0.commit();
            return null;
         });
      }

      // Wait for all (or at least most of) the txs to be replicated to cache 1
      Thread.sleep(1000);

      // Verify that cache 1 is in fact transferring state and transactional segments were requested
      StateConsumer stateConsumer = TestingUtil.extractComponent(cache1, StateConsumer.class);
      assertTrue(stateConsumer.isStateTransferInProgress());
      assertTrue(stateConsumer.inflightTransactionSegmentCount() > 0);

      // Let cache 1 receive the tx from cache 0.
      checkpoint.trigger("resume_get_transactions_" + rebalanceTopologyId + "_from_" + address(1));
      TestingUtil.waitForNoRebalance(caches(CACHE_NAME));

      // State transfer ended on cache 1 and request for transactional segments were received
      assertFalse(stateConsumer.isStateTransferInProgress());
      assertEquals(stateConsumer.inflightTransactionSegmentCount(), 0);

      // Wait for the txs to finish and check the results
      DataContainer dataContainer0 = TestingUtil.extractComponent(cache0, InternalDataContainer.class);
      DataContainer dataContainer1 = TestingUtil.extractComponent(cache1, InternalDataContainer.class);
      for (int i = 0; i < NUM_TXS; i++) {
         futures[i].get(10, SECONDS);
         assertEquals("v" + i, dataContainer0.peek("testkey" + i).getValue());
         assertEquals("v" + i, dataContainer1.peek("testkey" + i).getValue());
      }

      // Check for stale locks
      final TransactionTable tt0 = TestingUtil.extractComponent(cache0, TransactionTable.class);
      final TransactionTable tt1 = TestingUtil.extractComponent(cache1, TransactionTable.class);
      eventuallyEquals(0, tt0::getLocalTxCount);
      eventuallyEquals(0, tt1::getRemoteTxCount);
   }
}
