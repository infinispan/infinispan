package org.infinispan.resiliency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.assertj.core.api.SoftAssertions;
import org.infinispan.Cache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.reactive.publisher.impl.commands.batch.InitialPublisherCommand;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.statetransfer.StateTransferTracker;
import org.infinispan.test.Mocks;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.testing.Testing;
import org.infinispan.topology.ClusterTopologyManager;
import org.mockito.Mockito;
import org.testng.annotations.Test;

@CleanupAfterMethod
@Test(groups = "functional", testName = "resiliency.DataResiliencyScalingTest")
public class DataResiliencyScalingTest extends MultipleCacheManagersTest {

   private static final int DATA_SIZE = 100;
   private static final String CACHE_NAME = "resiliency-cache";
   private static final int CLUSTER_SIZE = 6;

   @Override
   protected void createCacheManagers() throws Throwable {
      Util.recursiveFileRemove(Testing.tmpDirectory(this.getClass().getSimpleName()));

      for (int i = 0; i < CLUSTER_SIZE; i++) {
         createStatefulCacheManager(Character.toString('A' + i), true);
      }
   }

   private void createStatefulCacheManager(String id, boolean clear) {
      String stateDirectory = Testing.tmpDirectory(this.getClass().getSimpleName(), id);
      if (clear)
         Util.recursiveFileRemove(stateDirectory);
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable().persistentLocation(stateDirectory);

      EmbeddedCacheManager manager = addClusterEnabledCacheManager(global, null);

      ConfigurationBuilder config = new ConfigurationBuilder();
      applyCacheManagerClusteringConfiguration(id, config);
      manager.defineConfiguration(CACHE_NAME, config.build());
   }

   private void applyCacheManagerClusteringConfiguration(String id, ConfigurationBuilder builder) {
      // Utilize a persistence layer to keep entries after complete restart.
//      builder.persistence().addSoftIndexFileStore();
      builder.persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .shared(false)
            .storeName("store-resiliency-" + id);

      builder.clustering()
            .cacheMode(CacheMode.DIST_SYNC)
            .hash()
               .numOwners(2)
            .stateTransfer()
               .timeout(30, TimeUnit.SECONDS);
   }

   public void testOrderedScalingDownKeepsData() throws Throwable {
      waitForClusterToForm(CACHE_NAME);
      populateCluster();

      for (int i = 0; i < CLUSTER_SIZE; i++) {
         assertDataIntegrity(cache(i, CACHE_NAME));
      }

      // We scale the cluster down to 1 cache.
      for (int i = CLUSTER_SIZE - 1; i > 0; i--) {
         Cache<?, ?> c = cache(i, CACHE_NAME);
         assertThat(c.stop(30, TimeUnit.SECONDS)).isTrue();
      }

      assertEventuallySingleCache(0);
      assertDataIntegrity(cache(0, CACHE_NAME));
   }

   public void testOrderedScalingContainerDown() throws Throwable {
      waitForClusterToForm(CACHE_NAME);
      populateCluster();

      for (int i = 0; i < CLUSTER_SIZE; i++) {
         assertDataIntegrity(cache(i, CACHE_NAME));
      }

      // We scale the cluster down to 1 cache.
      for (int i = CLUSTER_SIZE - 1; i > 0; i--) {
         EmbeddedCacheManager ecm = cacheManagers.get(i);
         assertThat(ecm.stop(30, TimeUnit.SECONDS)).isTrue();
      }

      assertEventuallySingleCache(0);
      assertDataIntegrity(cache(0, CACHE_NAME));
   }

   public void testTimeoutScalingContainer() throws Throwable {
      waitForClusterToForm(CACHE_NAME);
      populateCluster();

      DefaultCacheManager dcm = (DefaultCacheManager) manager(CLUSTER_SIZE - 1);
      assertThat(dcm.stopAllCaches(1, TimeUnit.NANOSECONDS)).isFalse();
   }

   public void testOrderedScalingContainerDownAllCaches() throws Throwable {
      waitForClusterToForm(CACHE_NAME);
      populateCluster();

      for (int i = 0; i < CLUSTER_SIZE; i++) {
         assertDataIntegrity(cache(i, CACHE_NAME));
      }

      // We scale the cluster down to 1 cache.
      for (int i = CLUSTER_SIZE - 1; i > 0; i--) {
         DefaultCacheManager dcm = (DefaultCacheManager) cacheManagers.get(i);
         assertThat(dcm.stopAllCaches(30, TimeUnit.SECONDS)).isTrue();
      }

      assertEventuallySingleCache(0);
      assertDataIntegrity(cache(0, CACHE_NAME));
   }

   public void testConcurrentScaleDownKeepData() throws ExecutionException, InterruptedException, TimeoutException {
      waitForClusterToForm(CACHE_NAME);
      populateCluster();

      for (int i = 0; i < CLUSTER_SIZE; i++) {
         assertDataIntegrity(cache(i, CACHE_NAME));
      }

      // We scale the cluster down to 1 cache.
      CompletableFuture<?>[] cfs = new CompletableFuture[CLUSTER_SIZE - 1];
      for (int i = CLUSTER_SIZE - 1; i > 0; i--) {
         Cache<?, ?> c = cache(i, CACHE_NAME);
         cfs[i - 1] = CompletableFuture.runAsync(() -> {
            try {
               assertThat(c.stop(30, TimeUnit.SECONDS)).isTrue();
            } catch (InterruptedException e) {
               throw new RuntimeException(e);
            }
         }, testExecutor());
      }

      CompletableFuture.allOf(cfs).get(30, TimeUnit.SECONDS);
      assertEventuallySingleCache(0);
      assertDataIntegrity(cache(0, CACHE_NAME));
   }

   public void testScalingUpKeepsData() {
      for (int i = CLUSTER_SIZE - 1; i >= 0; i--) {
         EmbeddedCacheManager ecm = manager(i);
         ecm.stop();
      }

      cacheManagers.clear();
      createStatefulCacheManager("A", true);
      waitForClusterToForm(CACHE_NAME);

      populateCluster();
      assertDataIntegrity(cache(0, CACHE_NAME));

      for (int i = 1; i < CLUSTER_SIZE; i++) {
         createStatefulCacheManager(Character.toString('A' + i), true);
      }
      waitForClusterToForm(CACHE_NAME);
   }

   public void testFullClusterRestartKeepsData() throws Throwable {
      waitForClusterToForm(CACHE_NAME);
      populateCluster();

      for (int i = 0; i < CLUSTER_SIZE; i++) {
         assertDataIntegrity(cache(i, CACHE_NAME));
      }

      for (int i = CLUSTER_SIZE - 1; i >= 0; i--) {
         EmbeddedCacheManager ecm = manager(i);
         assertThat(ecm.stop(30, TimeUnit.SECONDS)).isTrue();
      }

      // Remove all stopped cache managers to create a new cluster.
      cacheManagers.clear();

      // Do not delete the previous data folders.
      for (int i = 0; i < CLUSTER_SIZE; i++) {
         createStatefulCacheManager(Character.toString('A' + i), false);

         waitForClusterToForm(CACHE_NAME);
         assertDataIntegrity(i);
//         assertDataIntegrity(cache(i, CACHE_NAME));
      }

      // Assert topology is correct.
      waitForClusterToForm(CACHE_NAME);

      for (int i = 0; i < CLUSTER_SIZE; i++) {
         assertDataIntegrity(cache(i, CACHE_NAME));
      }
   }

   public void testCoordinatorLeavingKeepsData() throws Throwable {
      waitForClusterToForm(CACHE_NAME);
      populateCluster();

      EmbeddedCacheManager coordinator = manager(0);
      assertThat(coordinator.isCoordinator()).isTrue();

      // Grab a surviving cache reference before stopping the coordinator.
      Cache<String, String> survivingCache = cache(1, CACHE_NAME);

      assertThat(coordinator.stop(30, TimeUnit.SECONDS)).isTrue();

      eventually(() -> {
         DistributionManager dm = TestingUtil.extractComponent(survivingCache, DistributionManager.class);
         return !dm.isRehashInProgress() && dm.getCacheTopology().getMembersSet().size() == CLUSTER_SIZE - 1;
      });

      assertDataIntegrity(survivingCache);
   }

   public void testStopDuringInFlightRebalance() throws Throwable {
      waitForClusterToForm(CACHE_NAME);
      populateCluster();

      Cache<String, String> survivingCache = cache(0, CACHE_NAME);
      // The node we'll gracefully stop while rebalance is in flight.
      Cache<?, ?> leavingCache = cache(CLUSTER_SIZE - 2, CACHE_NAME);

      // Block state transfer on node 0 (provider side).
      CountDownLatch stStarted = new CountDownLatch(1);
      CountDownLatch stProceed = new CountDownLatch(1);
      blockStateTransfer(survivingCache, stStarted, stProceed);

      // Kill the last node to trigger rebalance among survivors.
      TestingUtil.killCacheManagers(manager(CLUSTER_SIZE - 1));
      assertThat(stStarted.await(15, TimeUnit.SECONDS)).isTrue();

      // While rebalance is in flight, gracefully stop another node.
      CompletableFuture<Boolean> stopFuture = CompletableFuture.supplyAsync(() -> {
         try {
            return leavingCache.stop(30, TimeUnit.SECONDS);
         } catch (InterruptedException e) {
            throw new RuntimeException(e);
         }
      }, testExecutor());

      // Release the blocked state transfer so rebalance can complete.
      stProceed.countDown();

      assertThat(stopFuture.get(30, TimeUnit.SECONDS)).isTrue();

      eventually(() -> {
         DistributionManager dm = TestingUtil.extractComponent(survivingCache, DistributionManager.class);
         return !dm.isRehashInProgress();
      });

      assertDataIntegrity(survivingCache);
   }

   public void testStopInterrupted() throws Throwable {
      waitForClusterToForm(CACHE_NAME);
      populateCluster();

      Cache<?, ?> leavingCache = cache(CLUSTER_SIZE - 1, CACHE_NAME);

      CountDownLatch awaitingStop = new CountDownLatch(1);
      EmbeddedCacheManager leavingManager = leavingCache.getCacheManager();
      StateTransferTracker globalTracker = TestingUtil.extractGlobalComponent(leavingManager, StateTransferTracker.class);
      StateTransferTracker.CacheStateTransferTracker spyCacheTracker = Mockito.spy(globalTracker.forCache(CACHE_NAME));
      doAnswer(invocation -> {
         awaitingStop.countDown();
         return new CompletableFuture<Void>();
      }).when(spyCacheTracker).onStateTransferCompleted(any());
      Map<String, StateTransferTracker.CacheStateTransferTracker> trackers = TestingUtil.extractField(globalTracker, "trackers");
      trackers.put(CACHE_NAME, spyCacheTracker);

      AtomicReference<Exception> caught = new AtomicReference<>();
      Thread stopThread = new Thread(() -> {
         try {
            leavingCache.stop(30, TimeUnit.SECONDS);
         } catch (InterruptedException e) {
            caught.set(e);
         }
      });

      stopThread.start();
      assertThat(awaitingStop.await(10, TimeUnit.SECONDS)).isTrue();

      stopThread.interrupt();
      stopThread.join(10_000);

      assertThat(caught.get()).isInstanceOf(InterruptedException.class);
   }

   public void testGracefulLeaveWithRebalanceDisabled() throws Throwable {
      waitForClusterToForm(CACHE_NAME);
      populateCluster();

      // Disable rebalance cluster-wide.
      ClusterTopologyManager ctm = TestingUtil.extractGlobalComponent(manager(0), ClusterTopologyManager.class);
      ctm.setRebalancingEnabled(false).toCompletableFuture().get(10, TimeUnit.SECONDS);

      // Graceful stop should complete even though rebalance is disabled.
      // When rebalance is disabled, doLeave creates a NO_REBALANCE topology (no pendingCH),
      // so the tracker's predicate is satisfied immediately without waiting for data redistribution.
      int expectedSurvivors = 2;
      for (int i = CLUSTER_SIZE - 1; i >= expectedSurvivors; i--) {
         Cache<?, ?> leavingCache = cache(i, CACHE_NAME);
         assertThat(leavingCache.stop(30, TimeUnit.SECONDS)).isTrue();
      }

      eventually(() -> {
         DistributionManager dm = TestingUtil.extractComponent(cache(0, CACHE_NAME), DistributionManager.class);
         return !dm.isRehashInProgress() && dm.getCacheTopology().getMembersSet().size() == expectedSurvivors;
      });
   }

   public void testStopCacheOrderedScaleDown() throws Throwable {
      waitForClusterToForm(CACHE_NAME);
      populateCluster();

      for (int i = CLUSTER_SIZE - 1; i > 0; i--) {
         EmbeddedCacheManager ecm = cacheManagers.get(i);
         assertThat(ecm.stopCache(CACHE_NAME, 30, TimeUnit.SECONDS)).isTrue();
      }

      assertEventuallySingleCache(0);
      assertDataIntegrity(cache(0, CACHE_NAME));
   }

   public void testStopCacheConcurrentScaleDown()
         throws ExecutionException, InterruptedException, TimeoutException {
      waitForClusterToForm(CACHE_NAME);
      populateCluster();

      CompletableFuture<?>[] cfs = new CompletableFuture[CLUSTER_SIZE - 1];
      for (int i = CLUSTER_SIZE - 1; i > 0; i--) {
         EmbeddedCacheManager ecm = cacheManagers.get(i);
         cfs[i - 1] = CompletableFuture.runAsync(() -> {
            try {
               assertThat(ecm.stopCache(CACHE_NAME, 30, TimeUnit.SECONDS)).isTrue();
            } catch (InterruptedException e) {
               throw new RuntimeException(e);
            }
         }, testExecutor());
      }

      CompletableFuture.allOf(cfs).get(60, TimeUnit.SECONDS);

      assertEventuallySingleCache(0);
      assertDataIntegrity(cache(0, CACHE_NAME));
   }

   public void testStopCacheTimeoutReturnsFalse() throws Throwable {
      waitForClusterToForm(CACHE_NAME);
      populateCluster();

      EmbeddedCacheManager ecm = cacheManagers.get(CLUSTER_SIZE - 1);
      assertThat(ecm.stopCache(CACHE_NAME, 1, TimeUnit.NANOSECONDS)).isFalse();
   }

   public void testStopCacheNonExistentReturnsTrue() throws Throwable {
      waitForClusterToForm(CACHE_NAME);

      EmbeddedCacheManager ecm = cacheManagers.get(0);
      assertThat(ecm.stopCache("no-such-cache", 30, TimeUnit.SECONDS)).isTrue();
   }

   public void testConcurrentCacheStopWithCoordinatorLeaving()
         throws ExecutionException, InterruptedException, TimeoutException {
      waitForClusterToForm(CACHE_NAME);
      populateCluster();

      assertThat(manager(0).isCoordinator()).isTrue();

      // Survivors are the last two nodes; the rest (including coordinator's cache) stop concurrently.
      // Only caches are stopped, the transport stays up, no view change, coordinator's handler is still alive.
      int survivors = 2;
      int firstSurvivor = CLUSTER_SIZE - survivors;
      Cache<String, String> survivingCache = cache(firstSurvivor, CACHE_NAME);

      CompletableFuture<?>[] cfs = new CompletableFuture[CLUSTER_SIZE - survivors];
      for (int i = 0; i < CLUSTER_SIZE - survivors; i++) {
         Cache<?, ?> c = cache(i, CACHE_NAME);
         cfs[i] = CompletableFuture.runAsync(() -> {
            try {
               c.stop(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
               throw new RuntimeException(e);
            }
         }, testExecutor());
      }

      CompletableFuture.allOf(cfs).get(60, TimeUnit.SECONDS);

      eventually(() -> {
         DistributionManager dm = TestingUtil.extractComponent(survivingCache, DistributionManager.class);
         return !dm.isRehashInProgress() && dm.getCacheTopology().getMembersSet().size() == survivors;
      });

      assertDataIntegrity(survivingCache);
   }

   public void testConcurrentContainerStopWithCoordinatorLeaving()
         throws ExecutionException, InterruptedException, TimeoutException {
      waitForClusterToForm(CACHE_NAME);
      populateCluster();

      assertThat(manager(0).isCoordinator()).isTrue();

      // Survivors are the last two nodes, the rest (including coordinator) stop concurrently.
      // Cache managers are stopped: transport goes down, view change triggers, new coordinator elected.
      // Handling concurrent leaves reside in the coordinator, we stress the coordinator changes with this test.
      int survivors = 2;
      int firstSurvivor = CLUSTER_SIZE - survivors;
      Cache<String, String> survivingCache = cache(firstSurvivor, CACHE_NAME);

      CompletableFuture<?>[] cfs = new CompletableFuture[CLUSTER_SIZE - survivors];
      for (int i = 0; i < CLUSTER_SIZE - survivors; i++) {
         EmbeddedCacheManager ecm = cacheManagers.get(i);
         cfs[i] = CompletableFuture.runAsync(() -> {
            try {
               ecm.stop(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
               throw new RuntimeException(e);
            }
         }, testExecutor());
      }

      CompletableFuture.allOf(cfs).get(60, TimeUnit.SECONDS);

      eventually(() -> {
         DistributionManager dm = TestingUtil.extractComponent(survivingCache, DistributionManager.class);
         return !dm.isRehashInProgress() && dm.getCacheTopology().getMembersSet().size() == survivors;
      });

      assertDataIntegrity(survivingCache);
   }

   private static void blockStateTransfer(Cache<?, ?> cache, CountDownLatch started, CountDownLatch proceed) {
      PerCacheInboundInvocationHandler handler = Mocks.replaceComponentWithSpy(cache, PerCacheInboundInvocationHandler.class);
      doAnswer(invocation -> {
         Object command = invocation.getArgument(0);
         if (command instanceof InitialPublisherCommand) {
            started.countDown();
            if (!proceed.await(15, TimeUnit.SECONDS)) {
               throw new TimeoutException();
            }
         }
         return invocation.callRealMethod();
      }).when(handler).handle(any(CacheRpcCommand.class), any(Reply.class), any(DeliverOrder.class));
   }

   private void populateCluster() {
      Cache<String, String> cache = cache(0, CACHE_NAME);

      for (int i = 0; i < DATA_SIZE; i++) {
         assertThat(cache.put("key-" + i, "value-" + i))
               .isNull();
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
         DistributionManager dm2 = TestingUtil.extractComponent(cache(index, CACHE_NAME), DistributionManager.class);
         if (dm2.isRehashInProgress())
            return false;

         return dm2.getCacheTopology().getMembersSet().size() == 1;
      });
   }

   private int getKeySegment(String key) {
      DistributionManager dm = TestingUtil.extractComponent(cache(0, CACHE_NAME), DistributionManager.class);
      return dm.getCacheTopology().getSegment(key);
   }

   private String getSegmentOwnership(int segment) {
      DistributionManager dm = TestingUtil.extractComponent(cache(0, CACHE_NAME), DistributionManager.class);
      return dm.getCacheTopology().getSegmentDistribution(segment).toString();
   }

   private void assertDataIntegrity(int clusterSize) {
      final SoftAssertions sa = new SoftAssertions();
      for (int i = 0; i < clusterSize; i++) {
         Cache<String, String> c = cache(i, CACHE_NAME);
         String address = c.getCacheManager().getAddress().toString();

         Set<String> keys = new HashSet<>(DATA_SIZE);
         log.infof("---- KEYSET FROM %s", address);
         for (Map.Entry<String, String> entry : c.entrySet()) {
            if (!keys.add(entry.getKey())) {
               throw new AssertionError("Duplicated key: " + entry.getKey());
            }
         }

         log.infof("---- ASSERTING KEY SET IS COMPLETE AT %s", address);
         sa.assertThat(keys)
               .withFailMessage(() -> {
                  StringBuilder sb = new StringBuilder();
                  sb.append("On node ").append(address).append(System.lineSeparator());
                  sb.append("Expected size: ").append(DATA_SIZE).append(System.lineSeparator());
                  sb.append("Actual size: ").append(keys.size()).append(System.lineSeparator());
                  sb.append("Missing keys: ").append(System.lineSeparator());
                  for (int j = 0; j < DATA_SIZE; j++) {
                     String k = "key-" + j;
                     if (!keys.contains(k)) {
                        sb.append("KEY=").append(k);
                        int segment = getKeySegment(k);
                        sb.append(" // (").append(segment).append(") // ");
                        sb.append(getSegmentOwnership(segment)).append(System.lineSeparator());
                     }
                  }
                  return sb.toString();
               })
               .hasSize(DATA_SIZE);
      }

      sa.assertAll();
   }
}
