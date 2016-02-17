package org.infinispan.distribution.ch;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.ch.impl.AffinityPartitioner;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.stream.IntStream;

import static org.infinispan.configuration.cache.CacheMode.DIST_SYNC;
import static org.testng.AssertJUnit.assertEquals;

/**
 * @author gustavonalle
 * @since 8.2
 */
@Test(groups = "functional", testName = "distribution.ch.AffinityPartitionerTest")
public class AffinityPartitionerTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      final ConfigurationBuilder conf = getConfigurationBuilder();
      createCluster(conf, 2);
      waitForClusterToForm();
   }

   @Test
   public void testAffinityPartitioner() throws Exception {
      Cache<AffinityKey, String> cache = cacheManagers.get(0).getCache();
      IntStream.range(0, 10).boxed().forEach(num -> cache.put(new AffinityKey(num), "value"));

      addClusterEnabledCacheManager(getConfigurationBuilder());
      waitForClusterToForm();

      cacheManagers.stream().map(cm -> cm.getCache().getAdvancedCache()).forEach(advancedCache -> {
         ConsistentHash ch = advancedCache.getDistributionManager().getConsistentHash();
         advancedCache.getDataContainer().keySet().forEach(key -> {
            int keySegmentId = ((AffinityKey) key).segmentId;
            assertEquals(ch.getSegment(key), keySegmentId);
         });
      });
   }

   private ConfigurationBuilder getConfigurationBuilder() {
      final ConfigurationBuilder conf = getDefaultClusteredCacheConfig(DIST_SYNC, false);
      conf.clustering().hash().keyPartitioner(new AffinityPartitioner()).numSegments(10).numOwners(1);
      return conf;
   }

   static class AffinityKey implements AffinityTaggedKey, Serializable {
      final int segmentId;

      public AffinityKey(int segmentId) {
         this.segmentId = segmentId;
      }

      @Override
      public int getAffinitySegmentId() {
         return segmentId;
      }
   }
}
