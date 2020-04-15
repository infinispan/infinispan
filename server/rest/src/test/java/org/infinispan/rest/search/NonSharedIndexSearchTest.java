package org.infinispan.rest.search;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Test for indexed search over Rest when using a non-shared index.
 *
 * @since 9.2
 */
@Test(groups = "functional", testName = "rest.search.NonSharedIndexSearchTest")
public class NonSharedIndexSearchTest extends BaseRestSearchTest {

   @Override
   protected ConfigurationBuilder getConfigBuilder() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.indexing().enable()
             .addIndexedEntity("org.infinispan.rest.search.entity.Person")
             .addProperty("directory.type", "local-heap");
      return builder;
   }
}
