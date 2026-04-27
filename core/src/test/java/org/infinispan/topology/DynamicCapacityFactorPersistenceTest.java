package org.infinispan.topology;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.test.TestingUtil.extractCacheTopology;
import static org.infinispan.test.TestingUtil.join;
import static org.infinispan.test.TestingUtil.killCacheManagers;
import static org.infinispan.test.TestingUtil.waitForNoRebalance;
import static org.infinispan.testing.Testing.tmpDirectory;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@CleanupAfterMethod
@Test(groups = "functional", testName = "topology.DynamicCapacityFactorPersistenceTest")
public class DynamicCapacityFactorPersistenceTest extends MultipleCacheManagersTest {

   private static final String CACHE_NAME = "testCache";

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      super.destroy();
      Util.recursiveFileRemove(tmpDirectory(this.getClass().getSimpleName()));
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      Util.recursiveFileRemove(tmpDirectory(this.getClass().getSimpleName()));
      createStatefulCluster(true);
   }

   private void createStatefulCluster(boolean clear) {
      for (int i = 0; i < 3; i++) {
         createStatefulCacheManager(clear, i);
      }
   }

   private void createStatefulCacheManager(boolean clear, int index) {
      String stateDirectory = tmpDirectory(this.getClass().getSimpleName(), Integer.toString(index));
      if (clear) Util.recursiveFileRemove(stateDirectory);

      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.globalState().enable()
            .persistentLocation(stateDirectory)
            .configurationStorage(ConfigurationStorage.OVERLAY);

      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC)
        .hash().numOwners(2);

      EmbeddedCacheManager ecm = addClusterEnabledCacheManager(global, cb);
      if (clear) {
         ecm.administration().getOrCreateCache(CACHE_NAME, cb.build());
      }
   }

   private void awaitCapacityFactor(Cache<?, ?> cache, int nodeIndex, float expected) {
      eventually(() -> {
         ConsistentHash ch = extractCacheTopology(cache).getReadConsistentHash();
         Float actual = ch.getCapacityFactors().get(address(nodeIndex));
         return actual != null && Float.compare(actual, expected) == 0;
      });
   }

   public void testConfigAttributeUpdatedLocally() throws Exception {
      waitForClusterToForm(CACHE_NAME);

      float before = manager(1).getCache(CACHE_NAME).getCacheConfiguration()
            .clustering().hash().capacityFactor();
      assertThat(before).isEqualTo(1.0f);

      join(manager(1).getCache(CACHE_NAME).getAdvancedCache().setCapacityFactor(0.5f));
      awaitCapacityFactor(manager(0).getCache(CACHE_NAME), 1, 0.5f);
      waitForNoRebalance(caches(CACHE_NAME));

      float after = manager(1).getCache(CACHE_NAME).getCacheConfiguration()
            .clustering().hash().capacityFactor();
      assertThat(after).isEqualTo(0.5f);
   }

   public void testConfigAttributeIsolatedPerNode() throws Exception {
      waitForClusterToForm(CACHE_NAME);

      join(manager(1).getCache(CACHE_NAME).getAdvancedCache().setCapacityFactor(0.5f));
      awaitCapacityFactor(manager(0).getCache(CACHE_NAME), 1, 0.5f);
      waitForNoRebalance(caches(CACHE_NAME));

      assertThat(manager(1).getCache(CACHE_NAME).getCacheConfiguration()
            .clustering().hash().capacityFactor()).isEqualTo(0.5f);
      assertThat(manager(0).getCache(CACHE_NAME).getCacheConfiguration()
            .clustering().hash().capacityFactor()).isEqualTo(1.0f);
      assertThat(manager(2).getCache(CACHE_NAME).getCacheConfiguration()
            .clustering().hash().capacityFactor()).isEqualTo(1.0f);
   }

   public void testCapacityFactorSurvivesRestart() throws Exception {
      Cache<Object, Object> cache = manager(2).getCache(CACHE_NAME);
      waitForClusterToForm(CACHE_NAME);

      join(cache.getAdvancedCache().setCapacityFactor(0f));
      awaitCapacityFactor(manager(0).getCache(CACHE_NAME), 2, 0f);
      waitForNoRebalance(caches(CACHE_NAME));

      assertThat(manager(2).getCache(CACHE_NAME).getCacheConfiguration()
            .clustering().hash().capacityFactor()).isEqualTo(0f);

      killCacheManagers(this.cacheManagers);
      this.cacheManagers.clear();

      createStatefulCluster(false);
      waitForClusterToForm(CACHE_NAME);

      float restoredFactor = manager(2).getCache(CACHE_NAME).getCacheConfiguration()
            .clustering().hash().capacityFactor();
      assertThat(restoredFactor).isEqualTo(0f);

      ConsistentHash ch = extractCacheTopology(manager(0).getCache(CACHE_NAME)).getReadConsistentHash();
      assertThat(ch.getCapacityFactors().get(address(2))).isEqualTo(0f);
      assertThat(ch.getSegmentsForOwner(address(2))).isEmpty();
   }

   public void testPerNodeIsolationAfterRestart() throws Exception {
      waitForClusterToForm(CACHE_NAME);

      join(manager(1).getCache(CACHE_NAME).getAdvancedCache().setCapacityFactor(0.5f));
      awaitCapacityFactor(manager(0).getCache(CACHE_NAME), 1, 0.5f);
      waitForNoRebalance(caches(CACHE_NAME));

      killCacheManagers(this.cacheManagers);
      this.cacheManagers.clear();

      createStatefulCluster(false);
      waitForClusterToForm(CACHE_NAME);

      assertThat(manager(0).getCache(CACHE_NAME).getCacheConfiguration()
            .clustering().hash().capacityFactor()).isEqualTo(1.0f);
      assertThat(manager(1).getCache(CACHE_NAME).getCacheConfiguration()
            .clustering().hash().capacityFactor()).isEqualTo(0.5f);
      assertThat(manager(2).getCache(CACHE_NAME).getCacheConfiguration()
            .clustering().hash().capacityFactor()).isEqualTo(1.0f);
   }
}
