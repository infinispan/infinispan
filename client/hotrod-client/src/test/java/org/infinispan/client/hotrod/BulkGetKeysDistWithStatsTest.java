package org.infinispan.client.hotrod;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

@Test(testName = "client.hotrod.BulkGetKeysDistWithStatsTest", groups = "functional")
public class BulkGetKeysDistWithStatsTest extends BulkGetKeysDistTest {

   @Override
   protected ConfigurationBuilder clusterConfig() {
      ConfigurationBuilder builder = super.clusterConfig();
      builder.jmxStatistics().enable();
      return hotRodCacheConfiguration(builder);
   }

}
