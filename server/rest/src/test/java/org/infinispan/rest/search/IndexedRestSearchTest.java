package org.infinispan.rest.search;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.indexmanager.InfinispanIndexManager;
import org.testng.annotations.Test;

/**
 * Tests for search over rest for indexed caches.
 *
 * @since 9.2
 */
@Test(groups = "functional", testName = "rest.IndexedRestSearchTest")
public class IndexedRestSearchTest extends BaseRestSearchTest {

   @Override
   ConfigurationBuilder getConfigBuilder() {
      ConfigurationBuilder configurationBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      configurationBuilder.indexing()
            .index(Index.PRIMARY_OWNER)
            .addProperty("default.indexmanager", InfinispanIndexManager.class.getName());
      return configurationBuilder;
   }

}
