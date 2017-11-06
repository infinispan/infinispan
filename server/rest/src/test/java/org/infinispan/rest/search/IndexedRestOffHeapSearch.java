package org.infinispan.rest.search;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.cache.StorageType;
import org.testng.annotations.Test;

/**
 * Test for indexed search over Rest when using OFF_HEAP.
 *
 * @since 9.2
 */
@Test(groups = "functional", testName = "rest.IndexedRestOffHeapSearch")
public class IndexedRestOffHeapSearch extends BaseRestSearchTest {

   @Override
   ConfigurationBuilder getConfigBuilder() {
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.indexing()
            .index(Index.PRIMARY_OWNER)
            .addProperty("default.directory_provider", "ram");
      configurationBuilder.memory().storageType(StorageType.OFF_HEAP);
      return configurationBuilder;
   }
}
