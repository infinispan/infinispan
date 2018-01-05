package org.infinispan.rest.search;

import org.eclipse.jetty.client.api.ContentResponse;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.rest.assertion.ResponseAssertion;
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
      ConfigurationBuilder configurationBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
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

   @Override
   public void testReadDocumentFromBrowser() throws Exception {
      ContentResponse fromBrowser = get("2", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");

      ResponseAssertion.assertThat(fromBrowser).isOk();
      ResponseAssertion.assertThat(fromBrowser).bodyNotEmpty();
   }
}
