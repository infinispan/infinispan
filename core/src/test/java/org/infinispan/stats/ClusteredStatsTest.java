package org.infinispan.stats;


import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.eviction.EvictionType;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "stats.ClusteredStatsTest")
public class ClusteredStatsTest extends SingleStatsTest {

   protected final int CLUSTER_SIZE = 3;
   protected final String CACHE_NAME = ClusteredStatsTest.class.getSimpleName();
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
      GlobalConfigurationBuilder global = defaultGlobalConfigurationBuilder();
      global.metrics().accurateSize(true);
      ConfigurationBuilder configBuilder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      configure(configBuilder);
      Configuration config = configBuilder.build();
      createCluster(global, new ConfigurationBuilder(), CLUSTER_SIZE);
      // ISPN-13022 Define the configuration on a subset of nodes to ensure an exception is not thrown if a cluster member
      // does not contain a cache definition
      for (int i = 0; i < CLUSTER_SIZE - 1; i++) {
         cacheManagers.get(i).createCache(CACHE_NAME, config);
      }
      TestingUtil.blockUntilViewsReceived(30000, cacheManagers);
      TestingUtil.waitForNoRebalance(
            IntStream.range(0, CLUSTER_SIZE - 1).mapToObj(i -> cache(i, CACHE_NAME)).toArray(Cache[]::new)
      );
      cache = cache(0, CACHE_NAME);
      refreshStats();
   }

   public void testClusteredStats() {
      for (int i = 0; i < TOTAL_ENTRIES; i++) {
         cache.put("key" + i, "value" + i);
      }

      eventuallyEquals((CLUSTER_SIZE - 1) * (TOTAL_ENTRIES - EVICTION_MAX_ENTRIES), () -> {
         refreshClusterStats();
         return (int) stats.getPassivations();
      });
   }

   protected void refreshClusterStats() {
      stats = cache.getAdvancedCache().getComponentRegistry().getComponent(ClusterCacheStats.class);
   }
}
