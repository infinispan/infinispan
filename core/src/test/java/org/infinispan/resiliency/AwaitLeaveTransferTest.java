package org.infinispan.resiliency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.infinispan.test.TestingUtil.replaceComponent;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.topology.LocalTopologyManager;
import org.testng.annotations.Test;

@CleanupAfterMethod
@Test(groups = "functional", testName = "resiliency.AwaitLeaveTransferTest")
public class AwaitLeaveTransferTest extends MultipleCacheManagersTest {

   private static final int DATA_SIZE = 100;
   private static final String CACHE_NAME = "await-leave-cache";
   private static final int CLUSTER_SIZE = 4;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering()
            .cacheMode(CacheMode.DIST_SYNC)
            .hash()
               .numOwners(2)
            .stateTransfer()
               .awaitLeaveTransfer(true)
               .timeout(30, TimeUnit.SECONDS);

      for (int i = 0; i < CLUSTER_SIZE; i++) {
         EmbeddedCacheManager manager = addClusterEnabledCacheManager();
         manager.defineConfiguration(CACHE_NAME, builder.build());
      }

      waitForClusterToForm(CACHE_NAME);
   }

   public void testOrderedScaleDownWithAwaitLeaveTransfer() {
      populateCluster();

      for (int i = CLUSTER_SIZE - 1; i > 0; i--) {
         cache(i, CACHE_NAME).stop();
      }

      assertEventuallySingleCache(0);
      assertDataIntegrity(cache(0, CACHE_NAME));
   }

   public void testConcurrentScaleDownWithAwaitLeaveTransfer() throws Exception {
      populateCluster();

      CompletableFuture<?>[] cfs = new CompletableFuture[CLUSTER_SIZE - 1];
      for (int i = CLUSTER_SIZE - 1; i > 0; i--) {
         Cache<?, ?> c = cache(i, CACHE_NAME);
         cfs[i - 1] = CompletableFuture.runAsync(c::stop, testExecutor());
      }

      CompletableFuture.allOf(cfs).get(60, TimeUnit.SECONDS);

      assertEventuallySingleCache(0);
      assertDataIntegrity(cache(0, CACHE_NAME));
   }

   public void testBlockedLeaveVerifiesStopWaits() throws Exception {
      populateCluster();

      EmbeddedCacheManager leavingManager = manager(CLUSTER_SIZE - 1);
      CheckPoint checkPoint = blockLeave(leavingManager, CACHE_NAME);

      CompletableFuture<Void> stopFuture = CompletableFuture.runAsync(
            () -> cache(CLUSTER_SIZE - 1, CACHE_NAME).stop(), testExecutor());

      checkPoint.awaitStrict("leave_before", 10, TimeUnit.SECONDS);

      assertThat(stopFuture).isNotDone();

      checkPoint.trigger("leave_continue");
      stopFuture.get(30, TimeUnit.SECONDS);
   }

   private CheckPoint blockLeave(EmbeddedCacheManager manager, String cacheName) throws Exception {
      CheckPoint checkPoint = new CheckPoint();
      LocalTopologyManager ltm = extractGlobalComponent(manager, LocalTopologyManager.class);
      LocalTopologyManager spyLtm = spy(ltm);

      doAnswer(invocation -> {
         checkPoint.trigger("leave_before");
         checkPoint.awaitStrict("leave_continue", 30, TimeUnit.SECONDS);
         return invocation.callRealMethod();
      }).when(spyLtm).leave(eq(cacheName), anyLong(), eq(true));

      replaceComponent(manager, LocalTopologyManager.class, spyLtm, true);
      return checkPoint;
   }

   private void populateCluster() {
      Cache<String, String> cache = cache(0, CACHE_NAME);
      for (int i = 0; i < DATA_SIZE; i++) {
         cache.put("key-" + i, "value-" + i);
      }
      assertThat(cache).hasSize(DATA_SIZE);
   }

   private void assertDataIntegrity(Cache<String, String> cache) {
      assertThat(cache).hasSize(DATA_SIZE);
      for (int i = 0; i < DATA_SIZE; i++) {
         assertThat(cache.get("key-" + i)).isEqualTo("value-" + i);
      }
   }

   private void assertEventuallySingleCache(int index) {
      eventually(() -> {
         DistributionManager dm = TestingUtil.extractComponent(cache(index, CACHE_NAME), DistributionManager.class);
         return !dm.isRehashInProgress() && dm.getCacheTopology().getMembersSet().size() == 1;
      });
   }
}
