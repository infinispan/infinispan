package org.infinispan.rest.search;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Test for search via rest when using compat mode, with the default marshaller.
 *
 * @since 9.2
 */
@Test(groups = "functional", testName = "rest.CompatNonIndexedDefaultMarshallerTest")
public class CompatNonIndexedDefaultMarshallerTest extends BaseRestSearchTest {

   @Override
   ConfigurationBuilder getConfigBuilder() {
      ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
      configurationBuilder.compatibility().enable();
      return configurationBuilder;
   }

   @Override
   protected void registerProtobuf(String protoFileName, String protoFileContents) throws Exception {
      // Not needed
   }

   @Override
   protected boolean needType() {
      return true;
   }

}
