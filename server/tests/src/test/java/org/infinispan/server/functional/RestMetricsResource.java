package org.infinispan.server.functional;

import static org.infinispan.server.test.core.Common.assertStatus;
import static org.infinispan.server.test.core.Common.sync;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestMetricsClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

/**
 * Tests the Micrometer metrics exporter.
 *
 * @author anistor@redhat.com
 * @since 10.0
 */
public class RestMetricsResource {

   @ClassRule
   public static InfinispanServerRule SERVERS = ClusteredIT.SERVERS;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void testOpenMetrics() {
      RestMetricsClient metricsClient = SERVER_TEST.rest().create().metrics();

      String metricName = "cache_manager_default_cache_" + SERVER_TEST.getMethodName() + "_statistics_stores";

      try (RestResponse response = sync(metricsClient.metrics(true))) {
         assertEquals(200, response.getStatus());
         assertEquals(MediaType.TEXT_PLAIN, response.contentType());
         String metricsText = response.getBody();
         assertTrue(metricsText.contains("# TYPE vendor_" + metricName + " gauge\n"));
      }
      try (RestResponse response = sync(metricsClient.metrics("vendor/" + metricName, true))) {
         assertEquals(200, response.getStatus());
         String metricsText = response.getBody();
         assertTrue(metricsText.contains("# TYPE vendor_" + metricName + " gauge\n"));
      }
   }

   @Test
   public void testBaseAndVendorMetrics() {
      RestMetricsClient metricsClient = SERVER_TEST.rest().create().metrics();

      try (RestResponse response = sync(metricsClient.metrics("base/classloader.loadedClasses.count"))) {
         assertEquals(200, response.getStatus());

         Json loadedClassesCountNode = Json.read(response.getBody());
         assertNotNull(loadedClassesCountNode.asJsonMap().get("classloader.loadedClasses.count"));
         int loadedClassesCount = loadedClassesCountNode.at("classloader.loadedClasses.count").asInteger();
         assertTrue(loadedClassesCount > 0);
      }

      try (RestResponse response = sync(metricsClient.metrics("vendor/memoryPool.Metaspace.usage"))) {
         assertEquals(200, response.getStatus());

         Json memoryPoolMetaspaceUsageNode = Json.read(response.getBody());
         assertNotNull(memoryPoolMetaspaceUsageNode.asJsonMap().get("memoryPool.Metaspace.usage"));
         int metaspaceUsage = memoryPoolMetaspaceUsageNode.at("memoryPool.Metaspace.usage").asInteger();
         assertTrue(metaspaceUsage > 0);
      }
   }

   @Test
   public void testMicroprofileMetrics() {
      RestClient client = SERVER_TEST.rest().create();
      RestMetricsClient metricsClient = client.metrics();

      String metricName = "cache_manager_default_cache_" + SERVER_TEST.getMethodName() + "_statistics_stores";
      int NUM_PUTS = 10;

      try (RestResponse response = sync(metricsClient.metrics())) {

         assertEquals(200, response.getStatus());
         assertEquals(MediaType.APPLICATION_JSON, response.contentType());

         String metricsJson = response.getBody();
         Json node = Json.read(metricsJson);
         assertNotNull(node.at("base"));
         assertNotNull(node.at("vendor"));
         assertNotNull(node.at("application"));
         assertTrue(metricsJson.contains(metricName));
      }

      try (RestResponse response = sync(metricsClient.metrics("vendor/" + metricName))) {
         assertEquals(200, response.getStatus());

         long totalStoresBefore = streamNodeFields(Json.read(response.getBody()))
               .map(e -> e.getValue().asLong())
               .reduce(0L, Long::sum);

         assertEquals(0, totalStoresBefore);

         // put some entries then check that the stats were updated
         RestCacheClient cache = client.cache(SERVER_TEST.getMethodName());

         for (int i = 0; i < NUM_PUTS; i++) {
            RestResponse putResp = sync(cache.put("k" + i, "v" + i));
            assertEquals(204, putResp.getStatus());
         }
      }

      try (RestResponse response = sync(metricsClient.metrics("vendor/" + metricName))) {
         assertEquals(200, response.getStatus());
         String metricJson = response.getBody();
         long totalStoresAfter = streamNodeFields(Json.read(metricJson))
               .map(e -> e.getValue().asLong())
               .reduce(0L, Long::sum);
         assertEquals(NUM_PUTS, totalStoresAfter);
      }

      // delete cache and check that the metric is gone
      sync(client.cache(SERVER_TEST.getMethodName()).delete());
      assertStatus(404, metricsClient.metricsMetadata("vendor/" + metricName));

      // check that an unexistent metric is correctly reported as missing
      assertStatus(404, metricsClient.metricsMetadata("vendor/no_such_metric"));
   }

   @Test
   public void testMicroprofileTimerMetrics() {
      RestClient client = SERVER_TEST.rest().create();
      RestMetricsClient metricsClient = client.metrics();

      // this is a histogram of write times
      String metricName = "cache_manager_default_cache_" + SERVER_TEST.getMethodName() + "_statistics_store_times";
      int NUM_PUTS = 10;

      try (RestResponse response = sync(metricsClient.metrics("vendor/" + metricName))) {
         assertEquals(200, response.getStatus());
         assertEquals(MediaType.APPLICATION_JSON, response.contentType());

         long meanStoreTimesBefore = streamNodeFields(Json.read(response.getBody()).at(metricName))
               .filter(e -> e.getKey().startsWith("mean;"))
               .map(e -> e.getValue().asLong())
               .reduce(0L, Long::sum);

         assertEquals(0, meanStoreTimesBefore);

         // put some entries then check that the stats were updated
         RestCacheClient cache = client.cache(SERVER_TEST.getMethodName());

         for (int i = 0; i < NUM_PUTS; i++) {
            RestResponse putResp = sync(cache.put("k" + i, "v" + i));
            assertEquals(204, putResp.getStatus());
         }
      }

      try (RestResponse response = sync(metricsClient.metrics("vendor/" + metricName))) {
         assertEquals(200, response.getStatus());

         long meanStoreTimesAfter = streamNodeFields(Json.read(response.getBody()).at(metricName))
               .filter(e -> e.getKey().startsWith("mean;"))
               .map(e -> e.getValue().asLong())
               .reduce(0L, Long::sum);

         assertTrue(meanStoreTimesAfter > 0);
      }
   }

   @Test
   public void testMicroprofileMetricsMetadata() {
      RestClient client = SERVER_TEST.rest().create();
      RestMetricsClient metricsClient = client.metrics();

      String metricName = "cache_manager_default_cache_" + SERVER_TEST.getMethodName() + "_statistics_stores";

      // get all
      try (RestResponse response = sync(metricsClient.metricsMetadata())) {
         assertEquals(200, response.getStatus());
         assertEquals(MediaType.APPLICATION_JSON, response.contentType());
         String metricsMetadataJson = response.getBody();
         assertTrue(metricsMetadataJson.contains(metricName));
      }

      // get one
      try (RestResponse response = sync(metricsClient.metricsMetadata("vendor/" + metricName))) {
         assertEquals(200, response.getStatus());
         Json node = Json.read(response.getBody());
         assertNotNull(node.at(metricName));
         assertEquals("gauge", node.at(metricName).at("type").asString());
         assertEquals("stores", node.at(metricName).at("displayName").asString());
      }

      // delete cache and check that the metric is gone
      sync(client.cache(SERVER_TEST.getMethodName()).delete());
      assertStatus(404, metricsClient.metricsMetadata("vendor/" + metricName));
   }

   /**
    * Stream over the fields of a given JsonNode.
    */
   private static Stream<Map.Entry<String, Json>> streamNodeFields(Json node) {
      if (node == null) {
         throw new IllegalArgumentException("Input node cannot be null");
      }
      return StreamSupport.stream(Spliterators.spliteratorUnknownSize(node.asJsonMap().entrySet().iterator(), Spliterator.IMMUTABLE), false);
   }
}
