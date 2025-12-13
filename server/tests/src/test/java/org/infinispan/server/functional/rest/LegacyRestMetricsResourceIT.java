package org.infinispan.server.functional.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.client.rest.RestResponse.NO_CONTENT;
import static org.infinispan.client.rest.RestResponse.OK;
import static org.infinispan.client.rest.RestResponseInfo.NOT_FOUND;
import static org.infinispan.server.test.core.Common.assertStatus;
import static org.infinispan.server.test.core.Common.sync;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import io.prometheus.metrics.expositionformats.OpenMetricsTextFormatWriter;
import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.AbstractDoubleAssert;
import org.assertj.core.api.AbstractStringAssert;
import org.infinispan.client.rest.RestCacheClient;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestMetricsClient;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.metrics.Constants;
import org.infinispan.server.test.core.ServerRunMode;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.server.test.junit5.InfinispanServerExtensionBuilder;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;


/**
 * Tests the Micrometer metrics exporter.
 *
 * @author anistor@redhat.com
 * @since 10.0
 */
public class LegacyRestMetricsResourceIT {

   // copied from regex101.com
   private static final Pattern PROMETHEUS_PATTERN = Pattern.compile("^(?<metric>[a-zA-Z_:][a-zA-Z0-9_:]*]*)(?<tags>\\{.*})?[\\t ]*(?<value>-?[0-9E.\\-]*)[\\t ]*(?<timestamp>[0-9]+)?$");
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final int NUM_SERVERS = 3;
   private static final String[] OWNERSHIP = new String[]{
         "primary_owner",
         "backup_owner",
         "non_owner"
   };

   // assertions
   private static final Consumer<AbstractDoubleAssert<?>> IS_POSITIVE = AbstractDoubleAssert::isPositive;
   private static final Consumer<AbstractDoubleAssert<?>> IS_ZERO = AbstractDoubleAssert::isZero;
   // between 1ms and 1s
   private static final Consumer<AbstractDoubleAssert<?>> LESS_THAN_ONE = doubleAssert -> doubleAssert.isBetween(0.001, 1.0);

   @RegisterExtension
   public static final InfinispanServerExtension SERVERS =
         InfinispanServerExtensionBuilder.config("configuration/LegacyMetricsClusteredServerTest.xml")
               .numServers(NUM_SERVERS)
               .runMode(ServerRunMode.CONTAINER)
               .build();

   @Test
   public void testOpenMetrics() {
      RestMetricsClient metricsClient = SERVERS.rest().create().metrics();

      String metricName = "statistics_stores";

      try (RestResponse response = sync(metricsClient.metrics(true))) {
         assertEquals(200, response.status());
         checkIsOpenmetrics(response.contentType());
         String metricsText = response.body();
         assertTrue(metricsText.contains("# TYPE vendor_" + metricName + " gauge\n"));
         assertTrue(metricsText.contains("vendor_" + metricName + "{cache=\"" + SERVERS.getMethodName()));
      }
   }

   @Test
   public void testJvmMetrics() {
      var metrics = getMetrics(SERVERS.rest().create().metrics());
      findMetric(metrics, "base_classloader_loadedClasses_count").value().isPositive();
      findMetric(metrics, "vendor_memoryPool_Metaspace_usage_bytes").value().isPositive();
   }

   @Test
   public void testMetrics() {
      RestClient client = SERVERS.rest().create();
      RestMetricsClient metricsClient = client.metrics();

      String cacheName = SERVERS.getMethodName();
      String metricName = "vendor_statistics_stores";
      int numPuts = 10;

      var metric = findCacheMetric(getMetrics(metricsClient), metricName, cacheName);
      metric.value().isZero();
      metric.assertTagPresent("cache", cacheName);

      // put some entries then check that the stats were updated
      RestCacheClient cache = client.cache(SERVERS.getMethodName());

      for (int i = 0; i < numPuts; i++) {
         assertStatus(NO_CONTENT, cache.put("k" + i, "v" + i));
      }

      findCacheMetric(getMetrics(metricsClient), metricName, cacheName).value().isEqualTo(10.0);

      // delete cache and check that the metric is gone
      assertStatus(OK, client.cache(SERVERS.getMethodName()).delete());

      var metrics = getMetrics(metricsClient);
      assertTrue(metrics.stream().noneMatch(m -> m.matches(metricName) && m.containsTag(Constants.CACHE_TAG_NAME, cacheName)));
      assertTrue(metrics.stream().anyMatch(m -> m.name.startsWith("base_")));
      assertTrue(metrics.stream().anyMatch(m -> m.name.startsWith("vendor_")));
   }

   @Test
   public void testTimerMetrics() {
      RestClient client = SERVERS.rest().create();
      RestMetricsClient metricsClient = client.metrics();
      var cacheName = SERVERS.getMethodName();

      // this is a histogram of write times
      String metricName = "vendor_statistics_store_times_seconds_max";
      int NUM_PUTS = 10;

      findCacheMetric(getMetrics(metricsClient), metricName, cacheName).value().isZero();

      // put some entries then check that the stats were updated
      RestCacheClient cache = client.cache(SERVERS.getMethodName());

      for (int i = 0; i < NUM_PUTS; i++) {
         assertStatus(NO_CONTENT, cache.put("k" + i, "v" + i));
      }

      findCacheMetric(getMetrics(metricsClient), metricName, cacheName).value().isPositive();
   }

   @Test
   public void testMetricsMetadata() {
      RestClient client = SERVERS.rest().create();
      RestMetricsClient metricsClient = client.metrics();

      String cacheName = SERVERS.getMethodName();
      String metricName = "vendor_statistics_stores";

      var metric = findCacheMetric(getMetrics(metricsClient), metricName, cacheName);
      metric.value().isZero();
      metric.assertTagPresent(Constants.CACHE_TAG_NAME, cacheName);

      // delete cache and check that the metric is gone
      assertStatus(OK, client.cache(cacheName).delete());

      assertTrue(getMetrics(metricsClient).stream().noneMatch(m -> m.matches(metricName) && m.containsTag(Constants.CACHE_TAG_NAME, cacheName)));
   }

   @Test
   public void testJGroupsMetrics() throws IOException, URISyntaxException {
      var metrics = getMetrics(SERVERS.rest().get().metrics());

      var missingMetrics = new ArrayList<>();
      for (var suffix : Files.readAllLines(Path.of(Thread.currentThread().getContextClassLoader().getResource("jgroups_metrics.txt").toURI()))) {
         if (metrics.stream().noneMatch(metric -> metric.name.contains(suffix))) {
            missingMetrics.add(suffix);
         }
      }
      assertThat(missingMetrics).isEmpty();
   }

   @Test
   public void testJGroupsDetailedMetrics() {
      RestMetricsClient metricsClient = SERVERS.rest().get().metrics();
      List<Metric> metrics = getMetrics(metricsClient);

      // async requests counter
      assertDetailedMetrics(metrics, "vendor_jgroups_stats_async_requests_total", IS_POSITIVE);
      // timed out request counter (no timeouts expected during testing)
      assertDetailedMetrics(metrics, "vendor_jgroups_stats_timed_out_requests_total", IS_ZERO);
      // sync requests histogram
      assertDetailedMetrics(metrics, "vendor_jgroups_stats_sync_requests_seconds_count", IS_POSITIVE);
      assertDetailedMetrics(metrics, "vendor_jgroups_stats_sync_requests_seconds_sum", LESS_THAN_ONE);
      assertDetailedMetrics(metrics, "vendor_jgroups_stats_sync_requests_seconds_max", LESS_THAN_ONE);
      // bytes sent distribution summary
      assertDetailedMetrics(metrics, "vendor_jgroups_stats_bytes_sent_count", IS_POSITIVE);
      assertDetailedMetrics(metrics, "vendor_jgroups_stats_bytes_sent_sum", IS_POSITIVE);
      assertDetailedMetrics(metrics, "vendor_jgroups_stats_bytes_sent_max", IS_POSITIVE);
   }

   @Test
   public void testDetailedKeyMetrics() {
      var client = SERVERS.rest().create();
      // put some entries then check that the stats were updated
      var cache = client.cache(SERVERS.getMethodName());

      // store + read hit
      assertStatus(NO_CONTENT, cache.post("hit", "value"));
      assertStatus(OK, cache.get("hit"));

      var metrics = getMetrics(client.metrics());

      var reads = new int[OWNERSHIP.length];
      var writes = new int[OWNERSHIP.length];

      log.debugf("Test hit:%n%s", metrics.stream().map(Metric::toString).collect(Collectors.joining("\n")));

      // unable to test remove hit since the return value is always ignored.
      for (var i = 0; i < OWNERSHIP.length; ++i) {
         reads[i] = (int) findCacheMetric(metrics, String.format("vendor_statistics_hit_%s_total", OWNERSHIP[i]), cache.name()).value;
         writes[i] = (int) findCacheMetric(metrics, String.format("vendor_statistics_store_%s_total", OWNERSHIP[i]), cache.name()).value;
      }

      // only 1 operation was performed
      assertEquals(1, Arrays.stream(reads).sum());

      // all arrays must have the same position set
      assertArrayEquals(reads, writes);
   }

   @Test
   public void testDetailedKeyMetrics2() {
      var client = SERVERS.rest().create();
      // put some entries then check that the stats were updated
      var cache = client.cache(SERVERS.getMethodName());

      assertStatus(NO_CONTENT, cache.post("hit", "value"));
      assertStatus(NOT_FOUND, cache.get("miss"));
      assertStatus(NO_CONTENT, cache.remove("hit"));

      var metrics = getMetrics(client.metrics());

      var reads = new int[OWNERSHIP.length];
      var writes = new int[OWNERSHIP.length];
      var rm_misses = new int[OWNERSHIP.length];
      var rm_hits = new int[OWNERSHIP.length];

      log.debugf("Test miss:%n%s", metrics.stream().map(Metric::toString).collect(Collectors.joining("\n")));

      for (var i = 0; i < OWNERSHIP.length; ++i) {
         reads[i] = (int) findCacheMetric(metrics, String.format("vendor_statistics_miss_%s_total", OWNERSHIP[i]), cache.name()).value;
         writes[i] = (int) findCacheMetric(metrics, String.format("vendor_statistics_store_%s_total", OWNERSHIP[i]), cache.name()).value;
         rm_misses[i] = (int) findCacheMetric(metrics, String.format("vendor_statistics_remove_miss_%s_total", OWNERSHIP[i]), cache.name()).value;
         rm_hits[i] = (int) findCacheMetric(metrics, String.format("vendor_statistics_remove_hit_%s_total", OWNERSHIP[i]), cache.name()).value;
      }

      // 1 miss + 1 hit (remove performs a read before removing)
      assertEquals(2, Arrays.stream(reads).sum());

      // 1 write
      assertEquals(1, Arrays.stream(writes).sum());

      // The arrays must have the same position set
      // If the request is sent to the primary owner, it is recorded as a hit.
      // Because "IGNORE_RETURN_VALUES" flag is set, the back owner records it as a miss.
      if (writes[0] == 1) {
         assertArrayEquals(writes, rm_hits);
      } else {
         assertArrayEquals(writes, rm_misses);
      }
   }

   private static void assertDetailedMetrics(List<Metric> allMetrics, String name, Consumer<AbstractDoubleAssert<?>> consumer) {
      List<Metric> metrics = allMetrics.stream().filter(metric -> metric.matches(name)).collect(Collectors.toList());
      log.debugf("Filtered metrics: %s", metrics);
      int expectedNumberOfMetrics = NUM_SERVERS - 1;
      assertEquals(expectedNumberOfMetrics, metrics.size(), "Wrong number of metrics: " + metrics);
      for (int i = 0; i < expectedNumberOfMetrics - 1; ++i) {
         Metric a = metrics.get(i);
         Metric b = metrics.get(i + 1);
         assertNotEquals(a, b, "Metrics must differ");
         a.assertSameName(b);
         a.assertNotSameTags(b);
      }
      metrics.stream().map(Metric::value).forEach(consumer);
   }

   public static void checkIsPrometheus(MediaType contentType) {
      String[] expectedContentType = PrometheusTextFormatWriter.CONTENT_TYPE.split(";");
      String[] actualContentType = contentType.toString().split(";");

      assertThat(actualContentType).containsExactlyInAnyOrder(expectedContentType);
   }

   public static void checkIsOpenmetrics(MediaType contentType) {
      String[] expectedContentType = OpenMetricsTextFormatWriter.CONTENT_TYPE.split(";");
      String[] actualContentType = contentType.toString().split(";");

      assertThat(actualContentType).containsExactlyInAnyOrder(expectedContentType);
   }

   public static List<Metric> getMetrics(RestMetricsClient client) {
      try (RestResponse response = sync(client.metrics())) {
         assertEquals(200, response.status());
         checkIsPrometheus(response.contentType());
         return parseMetrics(response.body());
      }
   }

   public static List<Metric> parseMetrics(String prometheusScrape) {
      assertNotNull(prometheusScrape);
      return prometheusScrape.lines()
            .map(PROMETHEUS_PATTERN::matcher)
            .filter(Matcher::matches)
            .map(LegacyRestMetricsResourceIT::matcherToMetric)
            .peek(log::debug)
            .toList();
   }

   public static Metric findMetric(List<Metric> metrics, String metricName) {
      var metricOpt = metrics.stream().filter(m -> m.matches(metricName)).findFirst();
      assertTrue(metricOpt.isPresent());
      return metricOpt.get();
   }

   public static Metric findCacheMetric(List<Metric> metrics, String metricName, String cacheName) {
      var metricOpt = metrics.stream()
            .filter(m -> m.matches(metricName))
            .filter(m -> m.containsTag(Constants.CACHE_TAG_NAME, cacheName))
            .findFirst();
      assertTrue(metricOpt.isPresent());
      return metricOpt.get();
   }

   private static Metric matcherToMetric(Matcher matcher) {
      Metric m = new Metric(matcher.group("metric"), matcher.group("tags"), Double.parseDouble(matcher.group("value")));
      log.tracef("Line matched. Parsing result: %s", m);
      return m;
   }

   public static class Metric {
      private final String name;
      private final String rawTags;
      private final double value;

      Metric(String name, String rawTags, double value) {
         this.name = Objects.requireNonNull(name);
         this.rawTags = rawTags;
         this.value = value;
      }

      public boolean matches(String name) {
         return Objects.equals(this.name, name);
      }

      public void assertSameName(Metric other) {
         metricName().isEqualTo(other.name);
      }

      public void assertNotSameTags(Metric other) {
         tags().isNotEqualTo(other.rawTags);
      }

      public void assertTagPresent(String tagKey, String tagValue) {
         tags().contains(tagKey + "=\"" + tagValue + "\"");
      }

      public void assertTagMissing(String tagKey, String tagValue) {
         tags().doesNotContain(tagKey + "=\"" + tagValue + "\"");
      }

      public boolean containsTag(String tagKey, String tagValue) {
         return rawTags.contains(tagKey + "=\"" + tagValue + "\"");
      }

      public AbstractStringAssert<?> metricName() {
         return addDescription(assertThat(name));
      }

      public AbstractStringAssert<?> tags() {
         return addDescription(assertThat(rawTags));
      }

      public AbstractDoubleAssert<?> value() {
         return addDescription(assertThat(value));
      }

      private <T extends AbstractAssert<?,?>> T addDescription(T abstractAssert) {
         abstractAssert.getWritableAssertionInfo().description("metric=%s, tags=%s", name, rawTags);
         return abstractAssert;
      }

      @Override
      public String toString() {
         return "Metric{" +
               "name='" + name + '\'' +
               ", rawTags='" + rawTags + '\'' +
               ", value=" + value +
               '}';
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) {
            return true;
         }
         if (o == null || getClass() != o.getClass()) {
            return false;
         }

         Metric metric = (Metric) o;

         if (Double.compare(value, metric.value) != 0) {
            return false;
         }
         if (!name.equals(metric.name)) {
            return false;
         }
         return Objects.equals(rawTags, metric.rawTags);
      }

      @Override
      public int hashCode() {
         int result;
         result = name.hashCode();
         result = 31 * result + (rawTags != null ? rawTags.hashCode() : 0);
         result = 31 * result + Double.hashCode(value);
         return result;
      }
   }
}
