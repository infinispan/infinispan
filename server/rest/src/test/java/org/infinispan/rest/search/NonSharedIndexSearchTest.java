package org.infinispan.rest.search;

import static org.infinispan.query.dsl.IndexedQueryMode.BROADCAST;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.testng.annotations.Test;

/**
 * Test for indexed search over Rest when using a non-shared index.
 *
 * @since 9.2
 */
@Test(groups = "functional", testName = "rest.NonSharedIndexSearchTest")
public class NonSharedIndexSearchTest extends BaseRestSearchTest {

   @Override
   ConfigurationBuilder getConfigBuilder() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.indexing().index(Index.PRIMARY_OWNER).addProperty("default.directory_provider", "local-heap");
      return builder;
   }

   @Override
   IndexedQueryMode getQueryMode() {
      return BROADCAST;
   }
}
