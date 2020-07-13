package org.infinispan.rest.search;

import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.testng.annotations.Test;

/**
 * Test for search via rest when storing java objects in the cache.
 *
 * @since 9.2
 */
@Test(groups = "functional", testName = "rest.search.NonIndexedPojoQueryTest")
public class NonIndexedPojoQueryTest extends BaseRestSearchTest {

   @Override
   protected ConfigurationBuilder getConfigBuilder() {
      ConfigurationBuilder configurationBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      configurationBuilder.encoding().key().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      configurationBuilder.encoding().value().mediaType(MediaType.APPLICATION_OBJECT_TYPE);
      return configurationBuilder;
   }

   @Override
   protected void createCacheManagers() throws Exception {
      super.createCacheManagers();
      cacheManagers.forEach(cm -> cm.getClassAllowList().addRegexps("org.infinispan.rest.search.entity.*"));
   }

   @Override
   protected void registerProtobuf(String protoFileContents) {
      // Not needed
   }

   @Override
   protected boolean needType() {
      return true;
   }

   @Override
   public void testReadDocumentFromBrowser() {
      CompletionStage<RestResponse> fromBrowser = get("2", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");

      ResponseAssertion.assertThat(fromBrowser).isOk();
      ResponseAssertion.assertThat(fromBrowser).bodyNotEmpty();
   }
}
