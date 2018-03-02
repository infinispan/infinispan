package org.infinispan.rest.search;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Test for querying embedded cache managers and embedded REST servers, storing
 * protobuf and JSON on the API side.
 */
@Test(groups = "functional", testName = "rest.EmbeddedRestSearchTest")
public class EmbeddedRestSearchTest extends SingleNodeLocalIndexTest {

   @Override
   ConfigurationBuilder getConfigBuilder() {
      ConfigurationBuilder config = super.getConfigBuilder();
      config.encoding().key().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);
      config.encoding().value().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);
      return config;
   }

   @Override
   protected boolean isServerMode() {
      return false;
   }
}
