package org.infinispan.rest.search;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @since 9.2
 */
@Test(groups = "functional", testName = "rest.NonIndexedRestSearchTest")
public class NonIndexedRestSearchTest extends BaseRestSearchTest {

   @Override
   ConfigurationBuilder getConfigBuilder() {
      ConfigurationBuilder configurationBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      configurationBuilder.encoding().key().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);
      configurationBuilder.encoding().value().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);
      return configurationBuilder;
   }

}
