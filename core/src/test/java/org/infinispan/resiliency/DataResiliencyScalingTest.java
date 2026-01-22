package org.infinispan.resiliency;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.manager.EmbeddedCacheManager;
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
   private static final int CLUSTER_SIZE = 15;

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
      builder.persistence().addSoftIndexFileStore();

      builder.clustering()
            .cacheMode(CacheMode.DIST_SYNC)
            .hash()
               .numOwners(2)
            .stateTransfer()
               .timeout(30, TimeUnit.SECONDS);
   }

   public void testOrderedScalingDownKeepsData() {
      waitForClusterToForm(CACHE_NAME);
      populateCluster();

      for (int i = 0; i < CLUSTER_SIZE; i++) {
         assertDataIntegrity(cache(i, CACHE_NAME));
      }

      // We scale the cluster down to 1 cache.
      for (int i = CLUSTER_SIZE - 1; i > 0; i--) {
         Cache<?, ?> c = cache(i, CACHE_NAME);
         c.stop(true);
      }

      assertEventuallySingleCache(0);
      assertDataIntegrity(cache(0, CACHE_NAME));
   }

   public void testOrderedScalingContainerDown() {
      waitForClusterToForm(CACHE_NAME);
      populateCluster();

      for (int i = 0; i < CLUSTER_SIZE; i++) {
         assertDataIntegrity(cache(i, CACHE_NAME));
      }

      // We scale the cluster down to 1 cache.
      for (int i = CLUSTER_SIZE - 1; i > 0; i--) {
         EmbeddedCacheManager ecm = cacheManagers.remove(i);
         ecm.stop(true);
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
         cfs[i - 1] = CompletableFuture.runAsync(() -> c.stop(true), testExecutor());
      }

      CompletableFuture.allOf(cfs).get(30, TimeUnit.SECONDS);
      assertEventuallySingleCache(0);

      // Stopping many nodes concurrently might lead to data loss.
      // We ensure the nodes keep at least <num owners> / <cluster size> of the data available.
      Cache<?, ?> c = cache(0, CACHE_NAME);
      float percentage = 1f * c.getAdvancedCache().getCacheConfiguration().clustering().hash().numOwners() / CLUSTER_SIZE;
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
         ecm.stop(true);
      }

      // Remove all stopped cache managers to create a new cluster.
      cacheManagers.clear();

      // Do not delete the previous data folders.
      for (int i = 0; i < CLUSTER_SIZE; i++) {
         createStatefulCacheManager(Character.toString('A' + i), false);
         String address = manager(i).getAddress().toString();
         waitForClusterToForm(CACHE_NAME);
         Cache<String, String> c = cache(i, CACHE_NAME);
         int size = c.size();
//         assertThat(c).hasSize(DATA_SIZE);
         Set<String> keys = new HashSet<>(DATA_SIZE);
         for (Map.Entry<String, String> entry : c.entrySet()) {
            if (!keys.add(entry.getKey())) {
               throw new AssertionError("Duplicated key: " + entry.getKey());
            }
         }
         assertThat(keys)
               .withFailMessage(() -> {
                  StringBuilder sb = new StringBuilder();
                  sb.append("Expected size: ").append(DATA_SIZE).append(System.lineSeparator());
                  sb.append("Actual size: ").append(keys.size()).append(System.lineSeparator());
                  sb.append("Missing keys: ").append(System.lineSeparator());
                  for (int j = 0; j < DATA_SIZE; j++) {
                     if (!keys.contains("key-" + j))
                        sb.append("key-").append(j).append(System.lineSeparator());
                  }
                  return sb.toString();
               })
               .hasSize(DATA_SIZE);
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
}
