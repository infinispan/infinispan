package org.infinispan.rest.search;

import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;
import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.test.annotation.TestForIssue;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.model.Game;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.search.RestHitCountAccuracyTest")
@TestForIssue(jiraKey = "ISPN-14195")
public class RestHitCountAccuracyTest extends SingleCacheManagerTest {

   private static final String CACHE_NAME = "games";
   private static final int ENTRIES = 5_000;

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
            .addIndexedEntity("Game")
         .query()
            .hitCountAccuracy(10); // lower the default accuracy

      cacheManager.createCache(CACHE_NAME, config.build());

      restServer = new RestServerHelper(cacheManager);
      restServer.start(TestResourceTracker.getCurrentTestShortName());
      restClient = RestClient.forConfiguration(new RestClientConfigurationBuilder().addServer()
            .host(restServer.getHost()).port(restServer.getPort())
            .build());

      return cacheManager;
   }

   @Override
   protected void teardown() {
      try {
         restClient.close();
      } catch (IOException ex) {
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
   @TestForIssue(jiraKey = "ISPN-14189")
   public void test() throws Exception {
      RestCacheClient cacheClient = restClient.cache(CACHE_NAME);

      writeEntries(ENTRIES, cacheClient);
      assertEquals(ENTRIES, count(cacheClient));

      CompletionStage<RestResponse> response = cacheClient.query("from Game where description : 'game'", 10, 0);
      assertThat(response).isOk();

      Json body = Json.read(response.toCompletableFuture().get().getBody());
      Object hitCountExact = body.at("hit_count_exact").getValue();
      assertEquals(hitCountExact, false);

      // raise the default accuracy
      response = cacheClient.query("from Game where description : 'game'", 10, 0, ENTRIES);
      assertThat(response).isOk();

      body = Json.read(response.toCompletableFuture().get().getBody());
      hitCountExact = body.at("hit_count_exact").getValue();
      assertEquals(hitCountExact, true);
      assertEquals(body.at("hit_count").asInteger(), ENTRIES);
   }

   private static void writeEntries(int entries, RestCacheClient cacheClient) {
      List<CompletionStage<RestResponse>> responses = new ArrayList<>(entries);
      for (int i = 0; i < entries; i++) {
         Json game = Json.object()
               .set("_type", "Game")
               .set("name", "Game n." + i)
               .set("description", "This is the game #" + i);

         responses.add(cacheClient.put("game-" + i, RestEntity.create(MediaType.APPLICATION_JSON, game.toString())));
      }
      for (CompletionStage<RestResponse> response : responses) {
         assertThat(response).isOk();
      }
   }

   private int count(RestCacheClient cacheClient) {
      RestResponse response = join(cacheClient.searchStats());
      Json stat = Json.read(response.getBody());
      Json indexGame = stat.at("index").at("types").at("Game");
      return indexGame.at("count").asInteger();
   }
}
