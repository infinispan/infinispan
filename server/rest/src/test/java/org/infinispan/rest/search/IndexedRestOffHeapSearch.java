package org.infinispan.rest.search;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.testng.annotations.Test;

/**
 * Test for indexed search over Rest when using OFF_HEAP.
 *
 * @since 9.2
 */
@Test(groups = "functional", testName = "rest.search.IndexedRestOffHeapSearch")
public class IndexedRestOffHeapSearch extends BaseRestSearchTest {

   @Override
   protected ConfigurationBuilder getConfigBuilder() {
      ConfigurationBuilder configurationBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      configurationBuilder.indexing().enable()
                          .addIndexedEntity("org.infinispan.rest.search.entity.Person")
                          .addProperty("default.directory_provider", "local-heap");
      configurationBuilder.memory().storageType(StorageType.OFF_HEAP);
      return configurationBuilder;
   }
}
