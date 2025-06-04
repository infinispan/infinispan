package org.infinispan.container.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * @author wburns
 * @since 9.3
 */
@Test(groups = "functional", testName = "container.impl.DefaultSegmentedDataContainerTest")
public class DefaultSegmentedDataContainerTest extends MultipleCacheManagersTest {
   private static final String CACHE_NAME = "dist";

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builderUsed = new ConfigurationBuilder();
      builderUsed.clustering().cacheMode(CacheMode.DIST_SYNC);
      createClusteredCaches(3, CACHE_NAME, builderUsed);
   }

   public void ensureOldMapsRemoved() {
      for (Cache<Object, Object> cache : caches(CACHE_NAME)) {
         DataContainer dc = TestingUtil.extractComponent(cache, InternalDataContainer.class);

         assertEquals(DefaultSegmentedDataContainer.class, dc.getClass());

         DefaultSegmentedDataContainer segmentedDataContainer = (DefaultSegmentedDataContainer) dc;

         DistributionManager dm = TestingUtil.extractComponent(cache, DistributionManager.class);
         Address address = cache.getCacheManager().getAddress();
         Set<Integer> segments = dm.getCacheTopology().getReadConsistentHash().getSegmentsForOwner(address);

         int mapCount = 0;

         for (int i = 0; i < segmentedDataContainer.maps.length(); ++i) {
            if (segmentedDataContainer.maps.get(i) != null) {
               assertTrue("Segment " + i + " has non null map, but wasn't owned by node: " + address + "!",
                     segments.contains(i));
               mapCount++;
            }
         }

         assertEquals(mapCount, segments.size());
      }
   }
}
