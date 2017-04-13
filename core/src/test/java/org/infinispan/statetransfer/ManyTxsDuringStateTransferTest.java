package org.infinispan.statetransfer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import javax.transaction.TransactionManager;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.executors.BlockingThreadPoolExecutorFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.impl.TransactionTable;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

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
      GlobalConfigurationBuilder globalBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      BlockingThreadPoolExecutorFactory threadPoolFactory = new BlockingThreadPoolExecutorFactory(1, 1, 0, Thread.NORM_PRIORITY);
      globalBuilder.transport().remoteCommandThreadPool().threadPoolFactory(threadPoolFactory);
      return globalBuilder;
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
      TestingUtil.replaceComponent(cache0, StateProvider.class, spyProvider, true);

      // Start cache 1, but the tx data request will be blocked on cache 0
      StateTransferManager stm0 = TestingUtil.extractComponent(cache0, StateTransferManager.class);
      int initialTopologyId = stm0.getCacheTopology().getTopologyId();
      int rebalanceTopologyId = initialTopologyId + 1;
      AdvancedCache<Object, Object> cache1 = advancedCache(1, CACHE_NAME);
      checkpoint.awaitStrict("post_get_transactions_" + rebalanceTopologyId + "_from_" + address(1), 10, SECONDS);

      // Start many transaction on cache 0, which will block on cache 1
      Future<Object>[] futures = new Future[NUM_TXS];
      for (int i = 0; i < NUM_TXS; i++) {
         // The rollback command should be invoked on cache 1 and it should block until the tx is created there
         final int ii = i;
         futures[i] = fork(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
               tm0.begin();
               cache0.put("testkey" + ii, "v" + ii);
               tm0.commit();
               return null;
            }
         });
      }

      // Wait for all (or at least most of) the txs to be replicated to cache 1
      Thread.sleep(1000);

      // Let cache 1 receive the tx from cache 0.
      checkpoint.trigger("resume_get_transactions_" + rebalanceTopologyId + "_from_" + address(1));
      TestingUtil.waitForStableTopology(caches(CACHE_NAME));

      // Wait for the txs to finish and check the results
      DataContainer dataContainer0 = TestingUtil.extractComponent(cache0, DataContainer.class);
      DataContainer dataContainer1 = TestingUtil.extractComponent(cache1, DataContainer.class);
      for (int i = 0; i < NUM_TXS; i++) {
         futures[i].get(10, SECONDS);
         assertEquals("v" + i, dataContainer0.get("testkey" + i).getValue());
         assertEquals("v" + i, dataContainer1.get("testkey" + i).getValue());
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
