package org.infinispan.stats;


import static org.testng.AssertJUnit.assertEquals;

import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.encoding.DataConversion;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.ClusterTopologyManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "stats.ClusteredStatsTest")
public class ClusteredStatsTest extends SingleStatsTest {

   protected final int CLUSTER_SIZE = 3;
   protected final String CACHE_NAME = ClusteredStatsTest.class.getSimpleName();

   @Override
   public Object[] factory() {
      return new Object[]{
            new ClusteredStatsTest().withStorage(StorageType.HEAP).withCountEviction(true),
            new ClusteredStatsTest().withStorage(StorageType.HEAP).withCountEviction(true).withAccurateSize(false),
            new ClusteredStatsTest().withStorage(StorageType.OFF_HEAP).withCountEviction(true),
            new ClusteredStatsTest().withStorage(StorageType.OFF_HEAP).withCountEviction(true).withAccurateSize(false),
      };
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      GlobalConfigurationBuilder global = defaultGlobalConfigurationBuilder();
      global.metrics().accurateSize(accurateSize);
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

      ClusterCacheStats clusteredStats = TestingUtil.extractComponent(cache, ClusterCacheStats.class);
      clusteredStats.setStaleStatsThreshold(1);

      // Approximate stats count each entry once for each node
      int actualOwners = CLUSTER_SIZE - 1;
      assertEquals(actualOwners * TOTAL_ENTRIES, clusteredStats.getApproximateEntries());
      assertEquals(actualOwners * EVICTION_MAX_ENTRIES, clusteredStats.getApproximateEntriesInMemory());
      assertEquals(TOTAL_ENTRIES, clusteredStats.getApproximateEntriesUnique());

      // Accurate stats try to de-duplicate counts
      if (accurateSize) {
         assertEquals(TOTAL_ENTRIES, clusteredStats.getCurrentNumberOfEntries());
         assertEquals(EVICTION_MAX_ENTRIES, clusteredStats.getCurrentNumberOfEntriesInMemory());
      } else {
         assertEquals(-1, clusteredStats.getCurrentNumberOfEntries());
         assertEquals(-1, clusteredStats.getCurrentNumberOfEntriesInMemory());
      }

      // Eviction stats with passivation can be delayed
      eventuallyEquals((long) actualOwners * (TOTAL_ENTRIES - EVICTION_MAX_ENTRIES), clusteredStats::getPassivations);
   }

   // Reproducer for ISPN-14279.
   // Calling approximate size before joining properly causes a division by 0 with non-segmented store.
   public void testJoinerStats() {
      for (int i = 0; i < TOTAL_ENTRIES; i++) {
         cache.put("key" + i, "value" + i);
      }

      for (int i = 0; i < CLUSTER_SIZE - 1; i++) {
         Cache<?, ?> cache = cacheManagers.get(i).getCache(CACHE_NAME);
         ClusterTopologyManager crmTM = TestingUtil.extractComponent(cache, ClusterTopologyManager.class);
         crmTM.setRebalancingEnabled(false);
      }

      GlobalConfigurationBuilder globalConfigurationBuilder = defaultGlobalConfigurationBuilder();
      globalConfigurationBuilder.metrics().accurateSize(true);
      ConfigurationBuilder cb = new ConfigurationBuilder()
            .read(cache.getAdvancedCache().getCacheConfiguration(), Combine.DEFAULT);
      cb.persistence().clearStores();
      Configuration configuration = cb.persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .segmented(false)
         .clustering()
            .hash().numSegments(1)
         .build();

      EmbeddedCacheManager ecm = addClusterEnabledCacheManager(new GlobalConfigurationBuilder().read(globalConfigurationBuilder.build()), new ConfigurationBuilder());
      Stats stats = ecm.createCache(CACHE_NAME, configuration).getAdvancedCache().getStats();
      assertEquals(0, stats.getApproximateEntries());
   }

   @Override
   protected long primaryKeysCount(Cache<?, ?> cache) {
      DistributionManager dm = TestingUtil.extractComponent(cache, DistributionManager.class);
      long count = 0;
      for (int i = 0; i < TOTAL_ENTRIES; i++) {
         DataConversion keyDataConversion = cache.getAdvancedCache().getKeyDataConversion();
         if (dm.getCacheTopology().getDistribution(keyDataConversion.toStorage("key" + i)).isPrimary()) {
            count++;
         }
      }
      return count;
   }
}
