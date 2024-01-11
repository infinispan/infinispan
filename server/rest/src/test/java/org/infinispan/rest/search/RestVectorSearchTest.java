package org.infinispan.rest.search;

import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;
import static org.testng.Assert.assertFalse;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.assertj.core.api.Assertions;
import org.infinispan.Cache;
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
import org.infinispan.query.model.Game;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.search.RestVectorSearchTest")
public class RestVectorSearchTest extends SingleCacheManagerTest {

   private static final String CACHE_NAME = "items";

   private RestServerHelper restServer;
   private RestClient restClient;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager();

      // Register proto schema
      Cache<String, String> metadataCache = cacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.putIfAbsent(Game.GameSchema.INSTANCE.getProtoFileName(), Game.GameSchema.INSTANCE.getProtoFile());
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

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
      writeEntries(10, cacheClient);

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
   }

   private static void writeEntries(int entries, RestCacheClient cacheClient) {
      List<CompletionStage<RestResponse>> responses = new ArrayList<>(entries);
      for (byte item = 1; item <= entries; item++) {
         byte[] byteArray = {item, item, item};

         // At the moment the transcoding for byte[] fields/bytes properties requires a JSON field filled with a Base64
         // See https://issues.redhat.com/browse/IPROTO-283 that allows to supply equivalent JSON arrays
         String encodeByteArray = Base64.getEncoder().encodeToString(byteArray);

         Json game = Json.object()
               .set("_type", "Item")
               .set("code", "c" + item)
               .set("byteVector", encodeByteArray)
               .set("floatVector", new float[]{1.1f * item, 1.1f * item, 1.1f * item})
               .set("buggy", "bla " + item);

         responses.add(cacheClient.put("item-" + item, RestEntity.create(MediaType.APPLICATION_JSON, game.toString())));
      }
      for (CompletionStage<RestResponse> response : responses) {
         assertThat(response).isOk();
      }
   }
}
