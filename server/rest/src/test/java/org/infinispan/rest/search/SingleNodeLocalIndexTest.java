package org.infinispan.rest.search;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @since 9.2
 */
@Test(groups = "functional", testName = "rest.search.SingleNodeLocalIndexTest")
public class SingleNodeLocalIndexTest extends BaseRestSearchTest {

   @Override
   protected ConfigurationBuilder getConfigBuilder() {
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.clustering().cacheMode(CacheMode.LOCAL);
      configurationBuilder.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("org.infinispan.rest.search.entity.Person");
      return configurationBuilder;
   }

   @Override
   protected int getNumNodes() {
      return 1;
   }
}
