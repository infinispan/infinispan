package org.infinispan.stats;


import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.eviction.EvictionType;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "stats.ClusteredStatsTest")
public class ClusteredStatsTest extends SingleStatsTest {

   protected final int CLUSTER_SIZE = 3;
   protected ClusterCacheStats stats;

   @Override
   public Object[] factory() {
      return new Object[]{
            new ClusteredStatsTest().withStorage(StorageType.OBJECT).withEvictionType(EvictionType.COUNT),
            new ClusteredStatsTest().withStorage(StorageType.OFF_HEAP).withEvictionType(EvictionType.COUNT),
      };
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      configure(cfg);
      createCluster(cfg, CLUSTER_SIZE);
      waitForClusterToForm();
      cache = cache(0);
      refreshStats();
   }

   public void testClusteredStats() {
      for (int i = 0; i < TOTAL_ENTRIES; i++) {
         cache.put("key" + i, "value" + i);
      }

      refreshClusterStats();
      assertEquals(CLUSTER_SIZE * (TOTAL_ENTRIES - EVICTION_MAX_ENTRIES), stats.getPassivations());
   }

   protected void refreshClusterStats() {
      stats = cache.getAdvancedCache().getComponentRegistry().getComponent(ClusterCacheStats.class);
   }
}
