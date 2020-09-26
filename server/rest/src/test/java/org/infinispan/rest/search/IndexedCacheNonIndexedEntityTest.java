package org.infinispan.rest.search;

import static org.infinispan.commons.api.CacheContainerAdmin.AdminFlag.VOLATILE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM_TYPE;

import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.configuration.JsonWriter;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * @since 12.0
 */
@Test(groups = "functional", testName = "rest.search.IndexedCacheNonIndexedEntityTest")
public class IndexedCacheNonIndexedEntityTest extends SingleCacheManagerTest {
   private static final String CACHE_NAME = "IndexedCacheNonIndexedEntitiesTest";
   private static final String SCHEMA = "message NonIndexed { required string name = 1; }";

   protected RestClient client;
   protected RestCacheClient cacheClient;
   private RestServerHelper restServer;

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder().nonClusteredDefault();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(gcb, getDefaultStandaloneCacheConfig(false));
      restServer = new RestServerHelper(cm);
      restServer.start(TestResourceTracker.getCurrentTestShortName() + "-" + cm.getAddress());

      RestClientConfigurationBuilder clientConfigurationBuilder = new RestClientConfigurationBuilder();
      clientConfigurationBuilder.addServer().host(restServer.getHost()).port(restServer.getPort());
      client = RestClient.forConfiguration(clientConfigurationBuilder.build());

      return cm;
   }

   @AfterClass
   public void tearDown() throws Exception {
      client.close();
      restServer.stop();
   }

   @Test
   public void shouldPreventNonIndexedEntities() {
      CompletionStage<RestResponse> response = client.schemas().post("customer", SCHEMA);
      ResponseAssertion.assertThat(response).isOk();

      ConfigurationBuilder configurationBuilder = getDefaultStandaloneCacheConfig(false);
      configurationBuilder.encoding().mediaType(APPLICATION_PROTOSTREAM_TYPE).indexing().enable()
            .addIndexedEntity("NonIndexed")
            .addProperty("directory.type", "local-heap");
      String config = new JsonWriter().toJSON(configurationBuilder.build());
      RestEntity configEntity = RestEntity.create(MediaType.APPLICATION_JSON, config);

      RestCacheClient cacheClient = client.cache(CACHE_NAME);
      response = cacheClient.createWithConfiguration(configEntity, VOLATILE);
      ResponseAssertion.assertThat(response).isBadRequest();
      ResponseAssertion.assertThat(response).containsReturnedText("The configured indexed-entity type 'NonIndexed' must be indexed");
   }
}
