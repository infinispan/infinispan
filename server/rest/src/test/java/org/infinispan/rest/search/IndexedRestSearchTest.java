package org.infinispan.rest.search;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
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
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.indexing()
            .index(Index.PRIMARY_OWNER)
            .addProperty("default.directory_provider", "ram");
      return configurationBuilder;
   }

}
