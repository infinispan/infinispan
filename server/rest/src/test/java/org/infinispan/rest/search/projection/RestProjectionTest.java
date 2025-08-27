package org.infinispan.rest.search.projection;

import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.assertj.core.api.Assertions;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.query.json.JsonQueryResponse;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.search.projection.RestProjectionSearchTest")
public class RestProjectionTest extends SingleCacheManagerTest {

   private static final String CACHE_NAME = "items";
   private static final int ENTRIES = 10;

   private RestServerHelper restServer;
   private RestClient restClient;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager();
      ConfigurationBuilder config = new ConfigurationBuilder();
      config
         .encoding()
            .mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE)
         .indexing()
            .enable()
            .storage(IndexStorage.LOCAL_HEAP)
            .addIndexedEntity("Game");

      cacheManager.createCache(CACHE_NAME, config.build());

      restServer = new RestServerHelper(cacheManager);
      restServer.start(TestResourceTracker.getCurrentTestShortName());
      restClient = RestClient.forConfiguration(new RestClientConfigurationBuilder().addServer()
            .host(restServer.getHost()).port(restServer.getPort()).protocol(Protocol.HTTP_20)
            .build());

      return cacheManager;
   }

   @Override
   protected void teardown() {
      try {
         restClient.close();
      } catch (Exception ex) {
         // ignore it
      } finally {
         try {
            restServer.stop();
         } finally {
            super.teardown();
         }
      }
   }

   @Test
   public void test() throws Exception {
      RestCacheClient cacheClient = restClient.cache(CACHE_NAME);
      writeEntries(cacheClient);

      CompletionStage<RestResponse> response = cacheClient.query("from Game g where g.description : 'bla3'", 10, 0);
      assertThat(response).isOk();
      Json body = Json.read(response.toCompletableFuture().get().body());
      Assertions.assertThat(body.at("hits").asJsonList())
            .extracting(json -> json.at("hit").at("name").asString()).containsExactly("bla3");

      response = cacheClient.query("select g.description from Game g where g.description : 'bla3'", 10, 0);
      assertThat(response).isOk();
      body = Json.read(response.toCompletableFuture().get().body());
      Assertions.assertThat(body.at("hits").asJsonList())
            .extracting(json -> json.at("hit").at("description").asString()).containsExactly("bla bla3");

      response = cacheClient.query("select g from Game g where g.description : 'bla3'", 10, 0);
      assertThat(response).isOk();
      body = Json.read(response.toCompletableFuture().get().body());
      Assertions.assertThat(body.at("hits").asJsonList())
            .extracting(json -> json.at("hit").at(JsonQueryResponse.ENTITY_PROJECTION_KEY)
                  .at("name").asString()).containsExactly("bla3");

      response = cacheClient.query("select g, g.description from Game g where g.description : 'bla3'", 10, 0);
      assertThat(response).isOk();
      body = Json.read(response.toCompletableFuture().get().body());
      Assertions.assertThat(body.at("hits").asJsonList())
            .extracting(json -> json.at("hit").at("description").asString()).containsExactly("bla bla3");
      Assertions.assertThat(body.at("hits").asJsonList())
            .extracting(json -> json.at("hit").at(JsonQueryResponse.ENTITY_PROJECTION_KEY)
                  .at("name").asString()).containsExactly("bla3");
   }

   private static void writeEntries(RestCacheClient cacheClient) {
      List<CompletionStage<RestResponse>> responses = new ArrayList<>(ENTRIES);
      for (byte i = 1; i <= ENTRIES; i++) {
         Json game = Json.object()
               .set("_type", "Game")
               .set("name", "bla" + i)
               .set("description", "bla bla" + i);

         String json = game.toString();
         responses.add(cacheClient.put("g" + i, RestEntity.create(MediaType.APPLICATION_JSON, json)));
      }
      for (CompletionStage<RestResponse> response : responses) {
         assertThat(response).isOk();
      }
   }
}
