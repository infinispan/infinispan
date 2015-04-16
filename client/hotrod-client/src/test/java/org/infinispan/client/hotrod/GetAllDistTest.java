package org.infinispan.client.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Tests functionality related to getting getting multiple entries using a distributed
 * cache
 *
 * @author William Burns
 * @since 7.2
 */
@Test(testName = "client.hotrod.GetAllDistTest", groups = "functional")
public class GetAllDistTest extends BaseGetAllTest {

   @Override
   protected int numberOfHotRodServers() {
      return 3;
   }

   @Override
   protected ConfigurationBuilder clusterConfig() {
      return hotRodCacheConfiguration(getDefaultClusteredCacheConfig(
            CacheMode.DIST_SYNC, false));
   }
}
