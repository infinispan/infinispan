package org.infinispan.rest.expiration;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;
import static org.testng.AssertJUnit.assertFalse;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.assertj.core.api.Assertions;
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
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.model.Game;
import org.infinispan.query.remote.client.ProtobufMetadataManagerConstants;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.expiration.DistributedQueryExpiredEntitiesTest")
@TestForIssue(jiraKey = "ISPN-14119")
public class DistributedQueryExpiredEntitiesTest extends MultipleCacheManagersTest {

   private static final String CACHE_NAME = "games";
   private static final int TIME = 100;

   private static final ControlledTimeService timeService = new ControlledTimeService();

   private RestServerHelper restServer;
   private RestClient restClient;

   @Override
   protected void createCacheManagers() {
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);

      createClusteredCaches(3, global, config, true, "default");
      EmbeddedCacheManager cacheManager = cacheManagers.get(0); // use the first one

      TestingUtil.replaceComponent(cacheManager, TimeService.class, timeService, true);

      Cache<String, String> metadataCache = cacheManager.getCache(ProtobufMetadataManagerConstants.PROTOBUF_METADATA_CACHE_NAME);
      metadataCache.putIfAbsent(Game.GameSchema.INSTANCE.getProtoFileName(), Game.GameSchema.INSTANCE.getProtoFile());
      assertFalse(metadataCache.containsKey(ProtobufMetadataManagerConstants.ERRORS_KEY_SUFFIX));

      config.encoding().mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE);
      config.expiration()
            .lifespan(TIME, TimeUnit.MILLISECONDS)
            .maxIdle(TIME, TimeUnit.MILLISECONDS);
      config.indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity("Game");
      config.statistics().enable();

      cacheManager.createCache(CACHE_NAME, config.build());

      restServer = new RestServerHelper(cacheManager);
      restServer.start(TestResourceTracker.getCurrentTestShortName());
      restClient = RestClient.forConfiguration(new RestClientConfigurationBuilder().addServer()
            .host(restServer.getHost()).port(restServer.getPort())
            .build());
   }

   @Test
   public void testQueryExpiredEntities() throws Exception {
      RestCacheClient cacheClient = restClient.cache(CACHE_NAME);

      Json game = Json.object()
            .set("_type", "Game")
            .set("name", "Ultima IV: Quest of the Avatar")
            .set("description", "It is the first in the \"Age of Enlightenment\" trilogy ...");

      CompletionStage<RestResponse> response = cacheClient.put(
            "ultima-iv", RestEntity.create(MediaType.APPLICATION_JSON, game.toString()));
      assertThat(response).isOk();

      response = cacheClient.query("from Game g where g.description : 'enlightenment'", 5, 0);
      assertThat(response).isOk();

      Json body = Json.read(response.toCompletableFuture().get().getBody());
      List<?> hits = (List<?>) body.at("hits").getValue();
      Assertions.assertThat(hits).isNotEmpty();

      timeService.advance(TIME * 2);

      response = cacheClient.query("from Game g where g.description : 'enlightenment'", 5, 0);
      assertThat(response).isOk();

      body = Json.read(response.toCompletableFuture().get().getBody());
      hits = (List<?>) body.at("hits").getValue();
      Assertions.assertThat(hits).isEmpty();
   }

   @AfterClass(alwaysRun = true)
   public void tearDown() throws Exception {
      try {
         restClient.close();
      } catch (IOException ex) {
         // ignore it
      } finally {
         restServer.stop();
      }
   }
}
