package org.infinispan.rest.search.aggregation;

import static org.infinispan.query.aggregation.QueryAggregationCountTest.CHUNK_SIZE;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.NUMBER_OF_DAYS;
import static org.infinispan.query.aggregation.QueryAggregationCountTest.chunk;
import static org.infinispan.rest.assertion.ResponseAssertion.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.model.Sale;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.testing.TestResourceTracker;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "rest.search.aggregation.RestQueryAggregationCountTest")
public class RestQueryAggregationCountTest extends SingleCacheManagerTest {

   private static final String CACHE_NAME = "items";

   private final Random fixedSeedPseudoRandom = new Random(739);

   private RestServerHelper restServer;
   private RestClient restClient;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(Sale.SaleSchema.INSTANCE);
      ConfigurationBuilder config = new ConfigurationBuilder();
      config
            .encoding()
            .mediaType(MediaType.APPLICATION_PROTOSTREAM_TYPE)
            .indexing()
            .enable()
            .storage(IndexStorage.LOCAL_HEAP)
            .addIndexedEntity("Sale");

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
      Util.close(restClient);
      restServer.stop();
      super.teardown();
   }

   @Test
   public void test() throws Exception {
      RestCacheClient cacheClient = restClient.cache(CACHE_NAME);
      writeEntries(cacheClient);

      CompletionStage<RestResponse> response = cacheClient.query(
            "select status, count(code) from Sale where day >= 45 and day <= 54 group by status order by status", 10, 0);
      assertThat(response).isOk();
      Json body = Json.read(response.toCompletableFuture().get().body());
      String hits = "[{\"hit\":{\"COUNT(code)\":16,\"status\":\"BLOCKED\"}},{\"hit\":{\"COUNT(code)\":13,\"status\":\"CLOSE\"}},{\"hit\":{\"COUNT(code)\":22,\"status\":\"IN_PROGRESS\"}},{\"hit\":{\"COUNT(code)\":20,\"status\":\"OPEN\"}},{\"hit\":{\"COUNT(code)\":9,\"status\":\"WAITING\"}}]";
      Assertions.assertThat(body.at("hits")).isEqualTo(Json.read(hits));

      response = cacheClient.query(
            "select count(code), status from Sale where day >= 45 and day <= 54 group by status order by status", 10, 0);
      assertThat(response).isOk();
      body = Json.read(response.toCompletableFuture().get().body());
      // same expected JSON
      Assertions.assertThat(body.at("hits")).isEqualTo(Json.read(hits));

      response = cacheClient.query(
            "select status, count(code) from Sale where day >= 45 and day <= 54  group by status", 10, 0);
      assertThat(response).isOk();
      body = Json.read(response.toCompletableFuture().get().body());
      // same expected JSON
      Assertions.assertThat(body.at("hits")).isEqualTo(Json.read(hits));

      response = cacheClient.query(
            "select s.status, count(s.code) from Sale s where s.day >= 45 and s.day <= 54  group by s.status order by s.status", 10, 0);
      assertThat(response).isOk();
      body = Json.read(response.toCompletableFuture().get().body());
      // same expected JSON
      Assertions.assertThat(body.at("hits")).isEqualTo(Json.read(hits));

      response = cacheClient.query(
            "select status, count(code) from Sale group by status", 10, 0);
      assertThat(response).isOk();
      body = Json.read(response.toCompletableFuture().get().body());
      hits = "[{\"hit\":{\"COUNT(code)\":184,\"status\":\"BLOCKED\"}},{\"hit\":{\"COUNT(code)\":144,\"status\":\"CLOSE\"}},{\"hit\":{\"COUNT(code)\":163,\"status\":\"IN_PROGRESS\"}},{\"hit\":{\"COUNT(code)\":169,\"status\":\"OPEN\"}},{\"hit\":{\"COUNT(code)\":140,\"status\":\"WAITING\"}}]";
      Assertions.assertThat(body.at("hits")).isEqualTo(Json.read(hits));

      response = cacheClient.query(
            "select s.status, count(s) from Sale s where s.day >= 45 and s.day <= 54 group by s.status order by s.status", 10, 0);
      assertThat(response).isOk();
      body = Json.read(response.toCompletableFuture().get().body());
      hits = "[{\"hit\":{\"COUNT(*)\":21,\"status\":\"BLOCKED\"}},{\"hit\":{\"COUNT(*)\":16,\"status\":\"CLOSE\"}},{\"hit\":{\"COUNT(*)\":26,\"status\":\"IN_PROGRESS\"}},{\"hit\":{\"COUNT(*)\":24,\"status\":\"OPEN\"}},{\"hit\":{\"COUNT(*)\":13,\"status\":\"WAITING\"}}]";
      Assertions.assertThat(body.at("hits")).isEqualTo(Json.read(hits));

      response = cacheClient.query(
            "select status, count(*) from Sale where day >= 45 and day <= 54 group by status", 10, 0);
      assertThat(response).isOk();
      body = Json.read(response.toCompletableFuture().get().body());
      Assertions.assertThat(body.at("hits")).isNotNull();
      // same expected JSON
      Assertions.assertThat(body.at("hits")).isEqualTo(Json.read(hits));
   }

   private void writeEntries(RestCacheClient cacheClient) {
      for (int day = 1; day <= NUMBER_OF_DAYS; day++) {
         HashMap<String, Sale> chunk = chunk(day, fixedSeedPseudoRandom);
         List<CompletionStage<RestResponse>> responses = new ArrayList<>(CHUNK_SIZE);
         for (Map.Entry<String, Sale> entry : chunk.entrySet()) {
            Sale sale = entry.getValue();
            String json = Json.object()
                  .set("_type", "Sale")
                  .set("id", sale.getId())
                  .set("code", sale.getCode())
                  .set("status", sale.getStatus())
                  .set("day", sale.getDay())
                  .toString();
            responses.add(cacheClient.put(entry.getKey(), RestEntity.create(MediaType.APPLICATION_JSON, json)));
         }
         for (CompletionStage<RestResponse> response : responses) {
            assertThat(response).isOk();
         }
      }
   }
}
