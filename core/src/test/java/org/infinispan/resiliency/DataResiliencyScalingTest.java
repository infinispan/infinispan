package org.infinispan.resiliency;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.assertj.core.api.SoftAssertions;
import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.testing.Testing;
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

   public void testScalingDownKeepSomeData() throws ExecutionException, InterruptedException, TimeoutException {
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

      // Stopping many nodes concurrently might lead to data loss.
      // We check the nodes keep at least <num owners> / <cluster size> of the data available.
      // Since the consistent hash is not perfect. That is, it is not guaranteed a node will own and be backup of the entries,
      // we allow some wiggle room.
      Cache<?, ?> c = cache(0, CACHE_NAME);
      float percentage = 1f * c.getAdvancedCache().getCacheConfiguration().clustering().hash().numOwners() / CLUSTER_SIZE - 0.05f;
      assertThat(c).hasSizeBetween((int) (percentage * DATA_SIZE), DATA_SIZE);
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
