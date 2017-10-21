package org.infinispan.it.compatibility;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Test for json/hotrod interop without indexing.
 *
 * @since 9.2
 */
@Test(groups = "functional", testName = "it.compatibility.NonIndexJsonTest")
public class NonIndexJsonTest extends JsonIndexingProtobufStoreTest {

   @Override
   protected ConfigurationBuilder getIndexCacheConfiguration() {
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.encoding().key().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);
      configurationBuilder.encoding().value().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);
      return configurationBuilder;
   }

}
