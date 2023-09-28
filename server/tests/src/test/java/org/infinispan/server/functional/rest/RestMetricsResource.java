package org.infinispan.server.functional.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.rest.RestResponse.NO_CONTENT;
import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.infinispan.server.test.core.Common.sync;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestMetricsClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.prometheus.client.exporter.common.TextFormat;

/**
 * Tests the Micrometer metrics exporter.
 *
 * @author anistor@redhat.com
 * @since 10.0
 */
public class RestMetricsResource {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS = ClusteredIT.SERVERS;

   @Test
   public void testOpenMetrics() {
      RestMetricsClient metricsClient = SERVERS.rest().create().metrics();

      String metricName = "cache_manager_default_cache_" + SERVERS.getMethodName() + "_statistics_stores";

      try (RestResponse response = sync(metricsClient.metrics(true))) {
         assertEquals(200, response.getStatus());
         checkIsOpenmetrics(response.contentType());
         String metricsText = response.getBody();
         assertTrue(metricsText.contains("# TYPE vendor_" + metricName + " gauge\n"));
         assertTrue(metricsText.contains("vendor_" + metricName + "{cache=\"" + SERVERS.getMethodName()));
      }
   }

   @Test
   public void testBaseAndVendorMetrics() throws Exception {
      RestMetricsClient metricsClient = SERVERS.rest().create().metrics();

      try (RestResponse response = sync(metricsClient.metrics())) {
         assertEquals(200, response.getStatus());
         checkIsPrometheus(response.contentType()); // that is the default

         String body = response.getBody();

         checkRule(body, "base_classloader_loadedClasses_count", (stringValue) -> {
            double parsed = Double.parseDouble(stringValue);
            assertThat(parsed).isPositive();
         });

         checkRule(body, "vendor_memoryPool_Metaspace_usage_bytes", (stringValue) -> {
            double parsed = Double.parseDouble(stringValue);
            assertThat(parsed).isPositive();
         });
      }
   }

   @Test
   public void testMetrics() throws Exception {
      RestClient client = SERVERS.rest().create();
      RestMetricsClient metricsClient = client.metrics();

      String cacheName = SERVERS.getMethodName();
      String metricName = String.format("cache_manager_default_cache_%s_statistics_stores{cache=\"%s\"", cacheName, cacheName);
      int NUM_PUTS = 10;

      try (RestResponse response = sync(metricsClient.metrics())) {
         assertEquals(200, response.getStatus());
         checkIsPrometheus(response.contentType());

         String body = response.getBody();
         assertThat(body).contains("base", "vendor", metricName);

         checkRule(body, "vendor_" + metricName, (stringValue) -> {
            double parsed = Double.parseDouble(stringValue);
            assertThat(parsed).isZero();
         });
      }

      // put some entries then check that the stats were updated
      RestCacheClient cache = client.cache(SERVERS.getMethodName());

      for (int i = 0; i < NUM_PUTS; i++) {
         assertStatus(NO_CONTENT, cache.put("k" + i, "v" + i));
      }

      try (RestResponse response = sync(metricsClient.metrics())) {
         assertEquals(200, response.getStatus());
         checkIsPrometheus(response.contentType());

         String body = response.getBody();
         assertThat(body).contains("base", "vendor", metricName);

         checkRule(body, "vendor_" + metricName, (stringValue) -> {
            double parsed = Double.parseDouble(stringValue);
            assertThat(parsed).isEqualTo(10.0);
         });
      }

      // delete cache and check that the metric is gone
      assertStatus(OK, client.cache(SERVERS.getMethodName()).delete());

      try (RestResponse response = sync(metricsClient.metrics())) {
         assertEquals(200, response.getStatus());
         checkIsPrometheus(response.contentType());

         String body = response.getBody();
         assertThat(body).contains("base", "vendor");

         // metric is not present anymore:
         assertThat(body).doesNotContain(metricName);
      }
   }

   @Test
   public void testTimerMetrics() throws Exception {
      RestClient client = SERVERS.rest().create();
      RestMetricsClient metricsClient = client.metrics();

      // this is a histogram of write times
      String metricName = "cache_manager_default_cache_" + SERVERS.getMethodName() + "_statistics_store_times";
      int NUM_PUTS = 10;

      try (RestResponse response = sync(metricsClient.metrics())) {
         assertEquals(200, response.getStatus());
         checkIsPrometheus(response.contentType());

         String body = response.getBody();
         assertThat(body).contains("base", "vendor", metricName);

         checkRule(body, "vendor_" + metricName, (stringValue) -> {
            double parsed = Double.parseDouble(stringValue);
            assertThat(parsed).isZero();
         });
      }

      // put some entries then check that the stats were updated
      RestCacheClient cache = client.cache(SERVERS.getMethodName());

      for (int i = 0; i < NUM_PUTS; i++) {
         assertStatus(NO_CONTENT, cache.put("k" + i, "v" + i));
      }

      try (RestResponse response = sync(metricsClient.metrics())) {
         assertEquals(200, response.getStatus());
         checkIsPrometheus(response.contentType());

         String body = response.getBody();
         assertThat(body).contains("base", "vendor", metricName);

         checkRule(body, "vendor_" + metricName, (stringValue) -> {
            double parsed = Double.parseDouble(stringValue);
            assertThat(parsed).isPositive();
         });
      }
   }

   @Test
   public void testMetricsMetadata() throws Exception {
      RestClient client = SERVERS.rest().create();
      RestMetricsClient metricsClient = client.metrics();

      String cacheName = SERVERS.getMethodName();
      String metricName = String.format("cache_manager_default_cache_%s_statistics_stores{cache=\"%s\"", cacheName, cacheName);

      try (RestResponse response = sync(metricsClient.metricsMetadata())) {
         assertEquals(200, response.getStatus());
         checkIsPrometheus(response.contentType());

         String body = response.getBody();
         assertThat(body).contains("base", "vendor", metricName);

         checkRule(body, "vendor_" + metricName, (stringValue) -> {
            double parsed = Double.parseDouble(stringValue);
            assertThat(parsed).isZero();
         });
      }

      // delete cache and check that the metric is gone
      assertStatus(OK, client.cache(SERVERS.getMethodName()).delete());

      try (RestResponse response = sync(metricsClient.metricsMetadata())) {
         assertEquals(200, response.getStatus());
         checkIsPrometheus(response.contentType());

         String body = response.getBody();
         assertThat(body).contains("base", "vendor");

         // metric is not present anymore:
         assertThat(body).doesNotContain(metricName);
      }
   }

   @Test
   public void testJGroupsMetrics() throws IOException, URISyntaxException {
      try (RestClient client = SERVERS.rest().create()) {
         RestMetricsClient metricsClient = client.metrics();
         try (RestResponse response = sync(metricsClient.metricsMetadata())) {
            assertEquals(200, response.getStatus());
            checkIsPrometheus(response.contentType());

            String body = response.getBody();
            assertThat(body).contains(Files.readAllLines(Path.of(Thread.currentThread().getContextClassLoader().getResource("jgroups_metrics.txt").toURI())));
         }
      }
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

   public static void checkIsPrometheus(MediaType contentType) {
      String[] expectedContentType = TextFormat.CONTENT_TYPE_004.split(";");
      String[] actualContentType = contentType.toString().split(";");

      assertThat(actualContentType).containsExactlyInAnyOrder(expectedContentType);
   }

   public static void checkIsOpenmetrics(MediaType contentType) {
      String[] expectedContentType = TextFormat.CONTENT_TYPE_OPENMETRICS_100.split(";");
      String[] actualContentType = contentType.toString().split(";");

      assertThat(actualContentType).containsExactlyInAnyOrder(expectedContentType);
   }

   public static void checkRule(String body, String key, Consumer<String> check) throws Exception {
      BufferedReader bufferedReader = new BufferedReader(new StringReader(body));
      String line;
      while ((line = bufferedReader.readLine()) != null) {
         if (line.startsWith(key)) {
            String[] split = line.split(" ");
            assertThat(split).hasSize(2);
            check.accept(split[1]);
            return;
         }
      }
      fail("Key " + key + " not found in body");
   }
}
