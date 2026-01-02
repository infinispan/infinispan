package org.infinispan.rest.search.vector;

import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;

import java.util.ArrayList;
import java.util.Base64;
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
import org.infinispan.query.model.Item;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.server.core.query.json.JsonQueryResponse;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.search.vector.RestVectorSearchTest")
public class RestVectorSearchTest extends SingleCacheManagerTest {

   private static final String[] BUGGY_OPTIONS =
         {"cat lover", "code lover", "mystical", "philologist", "algorithm designer", "decisionist", "philosopher"};

   private static final String CACHE_NAME = "items";
   private static final int ENTRIES = 50;
   private static final int ROUNDS = 5;

   private RestServerHelper restServer;
   private RestClient restClient;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(Item.ItemSchema.INSTANCE);
      ConfigurationBuilder config = new ConfigurationBuilder();
      config
         .encoding()
            .mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE)
         .indexing()
            .enable()
            .storage(IndexStorage.LOCAL_HEAP)
            .addIndexedEntity("Item");

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

      CompletionStage<RestResponse> response = cacheClient.query("from Item i where i.byteVector <-> [7,6,7]~3", 10, 0);
      assertThat(response).isOk();
      Json body = Json.read(response.toCompletableFuture().get().body());
      Assertions.assertThat(body.at("hits").asJsonList())
            .extracting(json -> json.at("hit").at("code").asString()).containsExactly("c7", "c6", "c8");

      response = cacheClient.query("from Item i where i.floatVector <-> [7.1,7.0,3.1]~3", 10, 0);
      assertThat(response).isOk();
      body = Json.read(response.toCompletableFuture().get().body());
      Assertions.assertThat(body.at("hits").asJsonList())
            .extracting(json -> json.at("hit").at("code").asString()).containsExactly("c5", "c6", "c4");

      response = cacheClient.query("select i, score(i) from Item i where i.floatVector <-> [7.1,7.0,3.1]~3", 10, 0);
      assertThat(response).isOk();
      body = Json.read(response.toCompletableFuture().get().body());
      Assertions.assertThat(body.at("hits").asJsonList())
            .extracting(json -> json.at("hit").at(JsonQueryResponse.ENTITY_PROJECTION_KEY)
                  .at("code").asString()).containsExactly("c5", "c6", "c4");
      Assertions.assertThat(body.at("hits").asJsonList())
            .extracting(json -> json.at("hit").at(JsonQueryResponse.SCORE_PROJECTION_KEY).asString())
            .hasSize(3);

      response = cacheClient.query("from Item i where i.floatVector <-> [7,7,7]~3 filtering i.buggy : 'cat'", 10, 0);
      assertThat(response).isOk();
      body = Json.read(response.toCompletableFuture().get().body());
      Assertions.assertThat(body.at("hits").asJsonList())
            .extracting(json -> json.at("hit").at("code").asString()).containsExactly("c7", "c14", "c21");

      response = cacheClient.query("from Item i where i.floatVector <-> [7,7,7]~3 filtering (i.buggy : 'cat' or i.buggy : 'code')", 10, 0);
      assertThat(response).isOk();
      body = Json.read(response.toCompletableFuture().get().body());
      Assertions.assertThat(body.at("hits").asJsonList())
            .extracting(json -> json.at("hit").at("code").asString()).containsExactly("c7", "c8", "c1");

      response = cacheClient.query("select score(i), i from Item i where i.floatVector <-> [7,7,7]~3 filtering i.buggy : 'cat'", 10, 0);
      assertThat(response).isOk();
      body = Json.read(response.toCompletableFuture().get().body());
      Assertions.assertThat(body.at("hits").asJsonList())
            .extracting(json -> json.at("hit").at(JsonQueryResponse.ENTITY_PROJECTION_KEY)
                  .at("code").asString()).containsExactly("c7", "c14", "c21");

      response = cacheClient.query("select score(i), i from Item i where i.floatVector <-> [7,7,7]~3 filtering (i.buggy : 'cat' or i.buggy : 'code')", 10, 0);
      assertThat(response).isOk();
      body = Json.read(response.toCompletableFuture().get().body());
      Assertions.assertThat(body.at("hits").asJsonList())
            .extracting(json -> json.at("hit").at(JsonQueryResponse.ENTITY_PROJECTION_KEY)
                  .at("code").asString()).containsExactly("c7", "c8", "c1");
   }

   private static void writeEntries(RestCacheClient cacheClient) {
      for (int round = 0; round < ROUNDS; round++) {
         List<CompletionStage<RestResponse>> responses = new ArrayList<>(ENTRIES / ROUNDS);
         for (int i = 1; i <= ENTRIES / ROUNDS; i++) {
            byte item = (byte) (round * ENTRIES / ROUNDS + i);
            String buggy = BUGGY_OPTIONS[item % 7];

            Json game = Json.object()
                  .set("_type", "Item")
                  .set("code", "c" + item)
                  .set("byteVector", byteArray(item))
                  .set("floatVector", new float[]{1.1f * item, 1.1f * item, 1.1f * item})
                  .set("buggy", buggy);
            String json = game.toString();
            responses.add(cacheClient.put("item-" + item, RestEntity.create(MediaType.APPLICATION_JSON, json)));
         }
         for (CompletionStage<RestResponse> response : responses) {
            assertThat(response).isOk();
         }
      }
   }

   private static Object byteArray(byte item) {
      // we support both the formats
      if (item % 2 == 0) {
         return Json.array(item, item, item);
      }

      byte[] byteArray = {item, item, item};
      return Base64.getEncoder().encodeToString(byteArray);
   }
}
