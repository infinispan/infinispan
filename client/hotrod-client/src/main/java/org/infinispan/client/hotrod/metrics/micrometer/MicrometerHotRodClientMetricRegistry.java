package org.infinispan.client.hotrod.metrics.micrometer;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.infinispan.client.hotrod.metrics.HotRodClientMetricsRegistry;
import org.infinispan.commons.stat.CounterTracker;
import org.infinispan.commons.stat.DistributionSummaryTracker;
import org.infinispan.commons.stat.SimpleTimerTracker;
import org.infinispan.commons.stat.TimerTracker;
import org.infinispan.commons.stat.micrometer.MicrometerCounterTracker;
import org.infinispan.commons.stat.micrometer.MicrometerDistributionSummary;
import org.infinispan.commons.stat.micrometer.MicrometerTimerTracker;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.TimeGauge;
import io.micrometer.core.instrument.Timer;

class MicrometerHotRodClientMetricRegistry implements HotRodClientMetricsRegistry {

   private final MeterRegistry registry;
   private final List<Tag> globalTags;
   private final String prefix;
   private final boolean histogramEnabled;
   private final Set<Meter.Id> ids;

   MicrometerHotRodClientMetricRegistry(MeterRegistry registry, Map<String, String> globalTags, String prefix, boolean histogramEnabled) {
      this.registry = Objects.requireNonNull(registry);
      this.globalTags = globalTags == null ? List.of() : mapToTag(globalTags).toList();
      this.prefix = prefix;
      this.histogramEnabled = histogramEnabled;
      this.ids = ConcurrentHashMap.newKeySet();
   }

   @Override
   public void createGauge(String metricName, String description, Supplier<Number> gauge, Map<String, String> tags, Consumer<Object> generatedId) {
      var id = Gauge.builder(computeMetricName(metricName), gauge)
            .strongReference(true)
            .tags(createTags(tags))
            .description(description)
            .register(registry)
            .getId();
      trackMeterId(id, generatedId);
   }

   @Override
   public void createTimeGauge(String metricName, String description, Supplier<Number> gauge, TimeUnit timeUnit, Map<String, String> tags, Consumer<Object> generatedId) {
      var id = TimeGauge.builder(computeMetricName(metricName), gauge, timeUnit)
            .strongReference(true)
            .tags(createTags(tags))
            .description(description)
            .register(registry)
            .getId();
      trackMeterId(id, generatedId);
   }

   @Override
   public TimerTracker createTimer(String metricName, String description, Map<String, String> tags, Consumer<Object> generatedId) {
      if (histogramEnabled) {
         var timer = Timer.builder(computeMetricName(metricName))
               .description(description)
               .publishPercentileHistogram(true)
               .tags(createTags(tags))
               .register(registry);
         trackMeterId(timer.getId(), generatedId);
         return new MicrometerTimerTracker(timer);
      }
      var simpleTimer = new SimpleTimerTracker();
      var id = FunctionTimer.builder(computeMetricName(metricName), simpleTimer, SimpleTimerTracker::count, SimpleTimerTracker::totalTime, TimeUnit.NANOSECONDS)
            .description(description)
            .tags(createTags(tags))
            .register(registry)
            .getId();
      trackMeterId(id, generatedId);
      return simpleTimer;
   }

   @Override
   public CounterTracker createCounter(String metricName, String description, Map<String, String> tags, Consumer<Object> generatedId) {
      var counter = Counter.builder(computeMetricName(metricName))
            .description(description)
            .tags(createTags(tags))
            .register(registry);
      trackMeterId(counter.getId(), generatedId);
      return new MicrometerCounterTracker(counter);
   }

   @Override
   public DistributionSummaryTracker createDistributionSummery(String metricName, String description, Map<String, String> tags, Consumer<Object> generatedId) {
      if (histogramEnabled) {
         var summary = DistributionSummary.builder(computeMetricName(metricName))
               .description(description)
               .publishPercentileHistogram(true)
               .tags(createTags(tags))
               .register(registry);
         trackMeterId(summary.getId(), generatedId);
         return new MicrometerDistributionSummary(summary);
      }
      return DistributionSummaryTracker.NO_OP;
   }

   @Override
   public void close() {
      ids.forEach(registry::remove);
      ids.clear();
   }

   @Override
   public void removeMetric(Object id) {
      if (id instanceof Meter.Id mid) {
         registry.remove(mid);
         ids.remove(mid);
      }
   }

   MeterRegistry getRegistry() {
      return registry;
   }

   boolean isHistogramEnabled() {
      return histogramEnabled;
   }

   public String getPrefix() {
      return prefix;
   }

   private String computeMetricName(String metricName) {
      var safeMetricName = decamelize(metricName);
      if (prefix == null || prefix.isEmpty()) {
         return "vendor.%s".formatted(safeMetricName);
      }
      return "vendor.%s.%s".formatted(prefix, safeMetricName);
   }

   private Iterable<Tag> createTags(Map<String, String> metricTags) {
      if (globalTags.isEmpty() && metricTags.isEmpty()) {
         return List.of();
      }
      if (metricTags.isEmpty()) {
         return globalTags;
      }
      if (globalTags.isEmpty()) {
         return () -> mapToTag(metricTags).iterator();
      }

      return () -> Stream.concat(globalTags.stream(), mapToTag(metricTags))
            .distinct()
            .iterator();
   }

   private void trackMeterId(Meter.Id id, Consumer<Object> generatedIdConsumer) {
      ids.add(id);
      if (generatedIdConsumer != null) {
         generatedIdConsumer.accept(id);
      }
   }

   /**
    * Replace illegal metric name chars with underscores.
    */
   public static String filterIllegalChars(String name) {
      return name.replaceAll("\\W+", "_");
   }

   /**
    * Transform a camel-cased name to snake-case, because Micrometer metrics loves underscores. Eventual sequences of
    * multiple underscores are replaced with a single underscore. Illegal characters are also replaced with underscore.
    */
   public static String decamelize(String name) {
      StringBuilder sb = new StringBuilder(name);
      for (int i = 1; i < sb.length(); i++) {
         if (Character.isUpperCase(sb.charAt(i))) {
            sb.insert(i++, '_');
            while (i < sb.length() && Character.isUpperCase(sb.charAt(i))) {
               i++;
            }
         }
      }
      return filterIllegalChars(sb.toString().toLowerCase()).replaceAll("_+", "_");
   }

   private static Stream<Tag> mapToTag(Map<String, String> tags) {
      return tags.entrySet().stream().map(e -> Tag.of(e.getKey(), e.getValue()));
   }

}
