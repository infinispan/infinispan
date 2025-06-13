package org.infinispan.server.functional.hotrod;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.infinispan.client.hotrod.metrics.micrometer.MicrometerRemoteCacheManagerMetricsRegistry;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.functional.rest.RestMetricsResourceIT;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

public class HotRodClientMetrics {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final String DEFAULT_PREFIX = "client_hotrod";
   private static final String[] CONNECTION_POOL_METRICS = {
         "connection_pool_retries_total",
   };
   private static final String[] NEAR_CACHE_METRICS = {
         "cache_near_cache_invalidations_total",
         "cache_near_cache_misses_total",
         "cache_near_cache_size",
         "cache_near_cache_hits_total"
   };
   // count is present with or without histograms enabled
   private static final String[] CACHE_COMMON_METRICS = {
         "cache_writes_seconds_count",
         "cache_writes_seconds_sum",
         "cache_removes_seconds_count",
         "cache_removes_seconds_sum",
         "cache_reads_hit_seconds_count",
         "cache_reads_hit_seconds_sum",
         "cache_reads_miss_seconds_count",
         "cache_reads_miss_seconds_sum"
   };
   private static final String[] CACHE_HISTOGRAMS = {
         "cache_writes_seconds_bucket",
         "cache_writes_seconds_max",
         "cache_removes_seconds_bucket",
         "cache_removes_seconds_max",
         "cache_reads_hit_seconds_bucket",
         "cache_reads_hit_seconds_max",
         "cache_reads_miss_seconds_bucket",
         "cache_reads_miss_seconds_max"
   };

   static class ConnectionPoolArgsProvider implements ArgumentsProvider {

      @Override
      public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) throws Exception {
         return Stream.of(
               Arguments.of(null, null),
               Arguments.of("a", Map.of("b", "c"))
         );
      }
   }

   static class PerCacheArgsProvider implements ArgumentsProvider {
      @Override
      public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
         return Stream.of(
               Arguments.of(null, null, false, false),
               Arguments.of(null, null, true, false),
               Arguments.of("a", null, false, false),
               Arguments.of("b", null, true, false),
               Arguments.of("c", Map.of("tag1", "value1"), false, false),
               Arguments.of("d", Map.of("tag2", "value2"), true, false),
               Arguments.of(null, null, false, true),
               Arguments.of("e", Map.of("tag3", "value3"), false, true)
         );
      }
   }

   @RegisterExtension
   public static InfinispanServerExtension SERVERS = ClusteredIT.SERVERS;

   @ParameterizedTest(name = "testConnectionPoolMetrics[{0},{1}]")
   @ArgumentsSource(ConnectionPoolArgsProvider.class)
   public void testConnectionPoolMetrics(String prefix, Map<String, String> tags) {
      var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
      var builder = createNewConfiguration(registry, prefix, tags, false, false);
      try (var ignored = SERVERS.hotrod().withClientConfiguration(builder).createRemoteCacheManager()) {
         if (prefix == null) {
            prefix = DEFAULT_PREFIX;
         }
         assertConnectionPoolMetricsPresent(registry, prefix, tags);
      } finally {
         registry.close();
      }
   }

   @ParameterizedTest(name = "testCacheMetrics[{0},{1},{2},{3}]")
   @ArgumentsSource(PerCacheArgsProvider.class)
   public void testCacheMetrics(String prefix, Map<String, String> tags, boolean histogram, boolean nearCache) {
      var registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
      var builder = createNewConfiguration(registry, prefix, tags, histogram, nearCache);
      RemoteCache<String, String> cache = SERVERS.hotrod().withClientConfiguration(builder).withCacheMode(CacheMode.DIST_SYNC).create();

      try {
         // make sure the cache name is present in the tags
         if (tags == null) {
            tags = Map.of("cache", cache.getName());
         } else {
            Map<String, String> copy = new HashMap<>(tags);
            copy.put("cache", cache.getName());
            tags = copy;
         }

         cache.put("k1", "v1"); // write++
         cache.put("k2", "v2"); // write++
         assertEquals("v1", cache.get("k1")); // read-hit++, near-cache-miss++, near-cache-size++
         assertEquals("v1", cache.get("k1")); // near-cache-hit++
         assertEquals("v2", cache.get("k2"));  // read-hit++, near-cache-miss++, near-cache-size++
         assertNull(cache.get("k3")); // read-miss++, near-cache-miss++
         cache.remove("k1"); //remove++, near-cache-invalidation++, near-cache-size--

         var scrape = registry.scrape();
         log.debugf("---%n%s%n----", scrape);

         var metrics = RestMetricsResourceIT.parseMetrics(scrape);

         if (prefix == null) {
            prefix = DEFAULT_PREFIX;
         }

         // cache metrics
         assertAllPresent(CACHE_COMMON_METRICS, metrics, prefix, tags);
         assertMetricValue(metrics, "cache_writes_seconds_count", prefix, tags, 2.0);
         assertMetricValue(metrics, "cache_removes_seconds_count", prefix, tags, 1.0);
         assertMetricValue(metrics, "cache_reads_hit_seconds_count", prefix, tags, nearCache ? 2.0 : 3.0);
         assertMetricValue(metrics, "cache_reads_miss_seconds_count", prefix, tags, 1.0);

         // near cache
         assertAllPresent(NEAR_CACHE_METRICS, metrics, prefix, tags);
         if (nearCache) {
            assertMetricValue(metrics, "cache_near_cache_invalidations_total", prefix, tags, 1.0);
            assertMetricValue(metrics, "cache_near_cache_misses_total", prefix, tags, 3.0);
            assertMetricValue(metrics, "cache_near_cache_size", prefix, tags, 1.0);
            assertMetricValue(metrics, "cache_near_cache_hits_total", prefix, tags, 1.0);
         }

         // cache metrics (histograms)
         if (histogram) {
            assertAllPresent(CACHE_HISTOGRAMS, metrics, prefix, tags);
         }
      } finally {
         registry.close();
         cache.stop();
      }
   }

   private static void assertAllPresent(String[] metricsName, List<RestMetricsResourceIT.Metric> metrics, String prefix, Map<String, String> tags) {
      Arrays.stream(metricsName).forEach(name -> assertMetricsPresent(metrics, name, prefix, tags));
   }

   private static void assertConnectionPoolMetricsPresent(PrometheusMeterRegistry registry, String prefix, Map<String, String> tags) {
      assertNotNull(prefix);
      var metrics = RestMetricsResourceIT.parseMetrics(registry.scrape());
      assertAllPresent(CONNECTION_POOL_METRICS, metrics, prefix, tags);
   }

   private static void assertMetricValue(List<RestMetricsResourceIT.Metric> metrics, String metricName, String prefix, Map<String, String> tags, double expected) {
      var fullName = "vendor_%s_%s".formatted(prefix, metricName);
      var metricOptional = metrics.stream()
            .filter(metric -> metric.matches(fullName))
            .findFirst();
      assertTrue(metricOptional.isPresent(), "metric '%s' missing.".formatted(fullName));
      var metric = metricOptional.get();
      metric.value().isEqualTo(expected);
      if (tags != null) {
         tags.forEach(metric::assertTagPresent);
      }
   }

   private static void assertMetricsPresent(List<RestMetricsResourceIT.Metric> metrics, String metricName, String prefix, Map<String, String> tags) {
      var fullName = "vendor_%s_%s".formatted(prefix, metricName);
      var metricOptional = metrics.stream()
            .filter(metric -> metric.matches(fullName))
            .findFirst();
      assertTrue(metricOptional.isPresent(), "metric '%s' missing.".formatted(fullName));
      var metric = metricOptional.get();
      metric.value().isGreaterThanOrEqualTo(0.0);
      if (tags != null) {
         tags.forEach(metric::assertTagPresent);
      }
   }

   private static ConfigurationBuilder createNewConfiguration(MeterRegistry registry, String prefix, Map<String, String> customTags, boolean histograms, boolean nearCache) {
      var metricsBuilder = new MicrometerRemoteCacheManagerMetricsRegistry.Builder(registry)
            .withHistograms(histograms);
      if (prefix != null) {
         metricsBuilder.withPrefix(prefix);
      }
      if (customTags != null) {
         metricsBuilder.clearTags();
         customTags.forEach(metricsBuilder::withTag);
      }
      var builder = new ConfigurationBuilder();
      builder.withMetricRegistry(metricsBuilder.build());
      builder.statistics().enable();
      if (nearCache) {
         builder.remoteCache("*").nearCacheMode(NearCacheMode.INVALIDATED).nearCacheMaxEntries(1000);
      }
      return builder;
   }

}
