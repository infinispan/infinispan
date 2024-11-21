package org.infinispan.client.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.AsymmetricRoutingDefaultLocalCacheTest")
public class AsymmetricRoutingDefaultLocalCacheTest extends AsymmetricRoutingTest {

   @Override
   protected ConfigurationBuilder defaultCacheConfigurationBuilder() {
      return hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.LOCAL));
   }

}
