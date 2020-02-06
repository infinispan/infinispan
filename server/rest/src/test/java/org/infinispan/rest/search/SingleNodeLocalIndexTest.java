package org.infinispan.rest.search;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @since 9.2
 */
@Test(groups = "functional", testName = "rest.SingleNodeLocalIndexTest")
public class SingleNodeLocalIndexTest extends BaseRestSearchTest {

   @Override
   ConfigurationBuilder getConfigBuilder() {
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.clustering().cacheMode(CacheMode.LOCAL);
      configurationBuilder.indexing().enable()
            .addProperty("default.directory_provider", "local-heap");
      return configurationBuilder;
   }

   @Override
   protected int getNumNodes() {
      return 1;
   }
}
