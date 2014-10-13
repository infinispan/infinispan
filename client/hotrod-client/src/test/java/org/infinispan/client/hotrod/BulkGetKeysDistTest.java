package org.infinispan.client.hotrod;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.testng.annotations.Test;

import java.util.Set;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests functionality related to getting multiple entries from a HotRod server
 * in bulk.
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 * @since 5.2
 */
@Test(testName = "client.hotrod.BulkGetKeysDistTest", groups = "functional")
public class BulkGetKeysDistTest extends BaseBulkGetKeysTest {

   @Override
   protected int numberOfHotRodServers() {
      return 3;
   }

   @Override
   protected ConfigurationBuilder clusterConfig() {
      return hotRodCacheConfiguration(getDefaultClusteredCacheConfig(
            CacheMode.DIST_SYNC, false));
   }

   public void testDistribution() {
      for (int i = 0; i < 100; i++) {
         remoteCache.put(i, i);
      }

      for (int i = 0; i < numberOfHotRodServers(); i++) {
         assertTrue(cache(i).getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).size() < 100);
      }

      Set<Object> set = remoteCache.keySet();
      assertEquals(100, set.size());
      for (int i = 0; i < 100; i++) {
         assertTrue(set.contains(i));
      }
   }
}
