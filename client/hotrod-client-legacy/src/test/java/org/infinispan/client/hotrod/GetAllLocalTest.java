package org.infinispan.client.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Tests functionality related to getting getting multiple entries using a local
 * cache
 *
 * @author William Burns
 * @since 7.2
 */
@Test(testName = "client.hotrod.GetAllLocalTest", groups = "functional")
public class GetAllLocalTest extends BaseGetAllTest {

   @Override
   protected int numberOfHotRodServers() {
      return 1;
   }

   @Override
   protected ConfigurationBuilder clusterConfig() {
      return hotRodCacheConfiguration(getDefaultClusteredCacheConfig(
            CacheMode.LOCAL, false));
   }
}
