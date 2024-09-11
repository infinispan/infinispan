package org.infinispan.client.hotrod.metrics.micrometer;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.infinispan.client.hotrod.metrics.HotRodClientMetricsRegistry;
import org.infinispan.client.hotrod.metrics.RemoteCacheManagerMetricsRegistry;
import org.infinispan.commons.stat.CounterTracker;
import org.infinispan.commons.stat.DistributionSummaryTracker;
import org.infinispan.commons.stat.TimerTracker;

import io.micrometer.core.instrument.MeterRegistry;

/**
 * A {@link RemoteCacheManagerMetricsRegistry} implementation that uses Micrometer to register and expose metrics.
 * <p>
 * This class is instantiated using the {@link Builder} class.
 *
 * @since 15.1
 */
public class MicrometerRemoteCacheManagerMetricsRegistry implements RemoteCacheManagerMetricsRegistry {

   private static final String DEFAULT_PREFIX = "client.hotrod";
   private static final String CACHE_PREFIX = "cache";
   private static final String CACHE_TAG = "cache";

   private final MicrometerHotRodClientMetricRegistry globalRegistry;
   private final Map<String, String> globalTags;
   private final Map<String, MicrometerHotRodClientMetricRegistry> cacheRegistry;

   private MicrometerRemoteCacheManagerMetricsRegistry(MeterRegistry registry, Map<String, String> globalTags, String prefix, boolean histograms) {
      this.globalTags = globalTags;
      globalRegistry = new MicrometerHotRodClientMetricRegistry(registry, this.globalTags, prefix, histograms);
      cacheRegistry = new ConcurrentHashMap<>();
   }

   @Override
   public HotRodClientMetricsRegistry withCache(String cacheName) {
      return cacheRegistry.computeIfAbsent(cacheName, this::createForCache);
   }

   @Override
   public void removeCache(String cacheName) {
      var existing = cacheRegistry.remove(cacheName);
      if (existing != null) {
         existing.close();
      }
   }

   @Override
   public CounterTracker createCounter(String metricName, String description, Map<String, String> tags, Consumer<Object> generatedId) {
      return globalRegistry.createCounter(metricName, description, tags, generatedId);
   }

   @Override
   public DistributionSummaryTracker createDistributionSummery(String metricName, String description, Map<String, String> tags, Consumer<Object> generatedId) {
      return globalRegistry.createDistributionSummery(metricName, description, tags, generatedId);
   }

   @Override
   public void createGauge(String metricName, String description, Supplier<Number> gauge, Map<String, String> tags, Consumer<Object> generatedId) {
      globalRegistry.createGauge(metricName, description, gauge, tags, generatedId);
   }

   @Override
   public void createTimeGauge(String metricName, String description, Supplier<Number> gauge, TimeUnit timeUnit, Map<String, String> tags, Consumer<Object> generatedId) {
      globalRegistry.createTimeGauge(metricName, description, gauge, timeUnit, tags, generatedId);
   }

   @Override
   public TimerTracker createTimer(String metricName, String description, Map<String, String> tags, Consumer<Object> generatedId) {
      return globalRegistry.createTimer(metricName, description, tags, generatedId);
   }

   @Override
   public void close() {
      globalRegistry.close();
      cacheRegistry.values().forEach(MicrometerHotRodClientMetricRegistry::close);
      cacheRegistry.clear();
   }

   @Override
   public void removeMetric(Object id) {
      globalRegistry.removeMetric(id);
   }

   private MicrometerHotRodClientMetricRegistry createForCache(String cacheName) {
      var cachePrefix = globalRegistry.getPrefix() == null && globalRegistry.getPrefix().isEmpty() ?
            CACHE_PREFIX :
            globalRegistry.getPrefix() + "." + CACHE_PREFIX;
      Map<String, String> cacheTags = new HashMap<>(globalTags);
      cacheTags.put(CACHE_TAG, cacheName);
      return new MicrometerHotRodClientMetricRegistry(globalRegistry.getRegistry(), cacheTags, cachePrefix, globalRegistry.isHistogramEnabled());
   }

   @Override
   public String toString() {
      return "MicrometerHotRodMetricRegistry{" +
            ", globalRegistry=" + globalRegistry +
            ", globalTags=" + globalTags +
            '}';
   }

   /**
    * Creates a new {@link MicrometerRemoteCacheManagerMetricsRegistry} instance.
    */
   public static class Builder {

      private final MeterRegistry registry;
      private final Map<String, String> tags;
      private String prefix = DEFAULT_PREFIX;
      private boolean histograms = true;

      public Builder(MeterRegistry registry) {
         this.registry = Objects.requireNonNull(registry);
         tags = new HashMap<>();
      }

      /**
       * Prefixes all metrics name with {@code prefix}.
       * <p>
       * If {@code null}, all metrics have the prefix {@code "client.hotrod"}.
       *
       * @param prefix The prefix to use in all metrics name. Can be null.
       * @return This instance.
       */
      public Builder withPrefix(String prefix) {
         this.prefix = prefix;
         return this;
      }

      /**
       * Adds a Micrometer Tag to all metrics.
       * <p>
       * This method can be invoked more than once.
       * <p>
       * Use the {@link #clearTags()} to remove all custom tags.
       *
       * @param tagName  The tag's name.
       * @param tagValue The tag's value.
       * @return This instance.
       */
      public Builder withTag(String tagName, String tagValue) {
         this.tags.put(tagName, tagValue);
         return this;
      }

      /**
       * Removes all Micrometer tags registered by {@link #withTag(String, String)}
       *
       * @return This instance.
       */
      public Builder clearTags() {
         this.tags.clear();
         return this;
      }

      /**
       * Enables or disables histograms.
       * <p>
       * When enabled, Micrometer exposes the histogram percentiles.
       * <p>
       * Collecting and exposing buckets may be expensive and affect the Hot Rod client performance.
       *
       * @param enabled Set to {@code true} to enabled buckets to be exposed.
       * @return This instance.
       */
      public Builder withHistograms(boolean enabled) {
         this.histograms = enabled;
         return this;
      }

      /**
       * Create a new {@link MicrometerRemoteCacheManagerMetricsRegistry} with this configuration.
       *
       * @return The new {@link MicrometerRemoteCacheManagerMetricsRegistry} instance.
       */
      public MicrometerRemoteCacheManagerMetricsRegistry build() {
         return new MicrometerRemoteCacheManagerMetricsRegistry(registry, Map.copyOf(tags), prefix, histograms);
      }
   }
}
