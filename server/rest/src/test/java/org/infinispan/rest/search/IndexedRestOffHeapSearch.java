package org.infinispan.rest.search;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;

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
      configurationBuilder.indexing().enable().storage(LOCAL_HEAP)
            .addIndexedEntity("org.infinispan.rest.search.entity.Person");
      configurationBuilder.memory().storageType(StorageType.OFF_HEAP);
      return configurationBuilder;
   }
}
