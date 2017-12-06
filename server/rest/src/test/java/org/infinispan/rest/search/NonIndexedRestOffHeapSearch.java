package org.infinispan.rest.search;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.testng.annotations.Test;

/**
 * @since 9.2
 */
@Test(groups = "functional", testName = "rest.NonIndexedRestOffHeapSearch")
public class NonIndexedRestOffHeapSearch extends BaseRestSearchTest {

   @Override
   ConfigurationBuilder getConfigBuilder() {
      ConfigurationBuilder configurationBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      configurationBuilder.encoding().key().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);
      configurationBuilder.encoding().value().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);
      configurationBuilder.memory().storageType(StorageType.OFF_HEAP);
      return configurationBuilder;
   }
}
