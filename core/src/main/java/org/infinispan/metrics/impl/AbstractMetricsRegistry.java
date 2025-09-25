package org.infinispan.metrics.impl;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.infinispan.commons.stat.CounterMetricInfo;
import org.infinispan.commons.stat.CounterTracker;
import org.infinispan.commons.stat.DistributionSummaryMetricInfo;
import org.infinispan.commons.stat.DistributionSummaryTracker;
import org.infinispan.commons.stat.FunctionTimerMetricInfo;
import org.infinispan.commons.stat.GaugeMetricInfo;
import org.infinispan.commons.stat.MetricInfo;
import org.infinispan.commons.stat.SimpleTimerTracker;
import org.infinispan.commons.stat.TimeGaugeMetricInfo;
import org.infinispan.commons.stat.TimerMetricInfo;
import org.infinispan.commons.stat.TimerTracker;
import org.infinispan.commons.stat.micrometer.MicrometerCounterTracker;
import org.infinispan.commons.stat.micrometer.MicrometerDistributionSummary;
import org.infinispan.commons.stat.micrometer.MicrometerTimerTracker;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metrics.config.MicrometerMeterRegistryConfiguration;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.TimeGauge;
import io.micrometer.core.instrument.Timer;

/**
 * Abstract implementation of {@link MetricsRegistry}.
 */
@Scope(Scopes.GLOBAL)
abstract class AbstractMetricsRegistry implements MetricsRegistry {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   @Inject GlobalConfiguration globalConfiguration;
   private ScrapeRegistry registry;
   private final JvmMetrics jvmMetrics = new JvmMetrics();

   @Start
   protected void start() {
      stop();

      boolean registerListeners = registry == null || !registry.externalManaged();
      MicrometerMeterRegistryConfiguration configuration = globalConfiguration.module(MicrometerMeterRegistryConfiguration.class);
      MeterRegistry configuredRegistry = configuration == null ? null : configuration.meterRegistry();
      registry = createScrapeRegistry(configuredRegistry);

      if (registerListeners) {
         if (globalConfiguration.metrics().jvm()) {
            if (globalConfiguration.metrics().legacy()) {
               new BaseAdditionalMetrics().bindTo(registry.registry());
            } else {
               jvmMetrics.bindTo(registry.registry());
            }
         }
         registry.registry().config().onMeterAdded(AbstractMetricsRegistry::onMeterAdded);
         registry.registry().config().onMeterRemoved(AbstractMetricsRegistry::onMeterRemoved);
         registry.registry().config().onMeterRegistrationFailed(AbstractMetricsRegistry::onMeterRegistrationFailed);
      }
   }

   @Stop
   protected void stop() {
      if (registry != null && !registry.externalManaged()) {
         registry.registry().close();
      }
      jvmMetrics.close();
   }

   abstract ScrapeRegistry createScrapeRegistry(MeterRegistry registry);

   @Override
   public Set<Object> registerMetrics(Object instance, Collection<MetricInfo> metricInfos, String namePrefix, Map<String, String> tags) {
      Set<Object> metricIds = new HashSet<>(metricInfos.size());

      for (MetricInfo info : metricInfos) {
         String name = metricName(namePrefix, info);
         if (globalConfiguration.metrics().gauges()) {
            var id = onGaugeEnabled(instance, name, info, tags);
            if (id != null) {
               metricIds.add(id);
               continue;
            }
         } else {
            onGaugeDisabled(instance, info);
         }

         if (globalConfiguration.metrics().histograms()) {
            var id = onHistogramEnabled(instance, name, info, tags);
            if (id != null) {
               metricIds.add(id);
               continue;
            }
         } else {
            onHistogramDisabled(instance, info);
         }

         if (log.isTraceEnabled()) {
            log.tracef("Metric %s not registered. Gauges enabled? %s, Histograms enabled? %s", info, globalConfiguration.metrics().gauges(), globalConfiguration.metrics().histograms());
         }
      }

      if (log.isTraceEnabled()) {
         log.tracef("Registered %d metrics. Metric registry @%x contains %d metrics.",
               metricIds.size(), System.identityHashCode(registry), registry.registry().getMeters().size());
      }

      return metricIds;
   }

   @Override
   public void unregisterMetric(Object metricId) {
      if (!(metricId instanceof Meter.Id)) {
         if (log.isTraceEnabled()) {
            log.tracef("Metric %s is not a valid Meter.Id", metricId);
         }
         return;
      }
      Meter removed = registry.registry().remove((Meter.Id) metricId);
      if (log.isTraceEnabled()) {
         if (removed != null) {
            log.tracef("Unregistered metric \"%s\". Metric registry @%x contains %d metrics.",
                  metricId, System.identityHashCode(registry), registry.registry().getMeters().size());
         } else {
            log.tracef("Could not remove unexisting metric \"%s\". Metric registry @%x contains %d metrics.",
                  metricId, System.identityHashCode(registry), registry.registry().getMeters().size());
         }
      }
   }

   @Override
   @Deprecated(forRemoval = true, since = "16.0")
   public boolean legacy() {
      return globalConfiguration.metrics().legacy();
   }

   @Override
   @Deprecated(forRemoval = true, since = "16.0")
   public boolean namesAsTags() {
      return globalConfiguration.metrics().namesAsTags();
   }

   @Override
   public boolean supportScrape() {
      return registry.supportsScrape();
   }

   @Override
   public String scrape(String contentType) {
      return registry.scrape(contentType);
   }

   // mainly for testing
   protected MeterRegistry registry() {
      return registry.registry();
   }

   private static void onMeterAdded(Meter meter) {
      if (log.isDebugEnabled()) {
         log.debugf("Registered metric %s", meter.getId());
      }
   }

   private static void onMeterRemoved(Meter meter) {
      if (log.isDebugEnabled()) {
         log.debugf("Unregistered metric %s", meter.getId());
      }
   }

   private static void onMeterRegistrationFailed(Meter.Id id, String reason) {
      log.metricRegistrationFailed(String.valueOf(id), reason);
   }

   @SuppressWarnings("unchecked")
   private Meter.Id onGaugeEnabled(Object targetInstance, String name, MetricInfo metricInfo, Map<String, String> commonTags) {
      if (metricInfo instanceof GaugeMetricInfo) {
         return createGauge(targetInstance, name, (GaugeMetricInfo<Object>) metricInfo, commonTags);
      } else if (metricInfo instanceof CounterMetricInfo) {
         return createCounter(targetInstance, name, (CounterMetricInfo<Object>) metricInfo, commonTags);
      } else if (metricInfo instanceof FunctionTimerMetricInfo) {
         return createFunctionTimer(targetInstance, name, (FunctionTimerMetricInfo<Object>) metricInfo, commonTags);
      } else if (metricInfo instanceof TimeGaugeMetricInfo) {
         return createTimeGauge(targetInstance, name, (TimeGaugeMetricInfo<Object>) metricInfo, commonTags);
      }
      return null;
   }

   @SuppressWarnings("unchecked")
   private void onGaugeDisabled(Object targetInstance, MetricInfo metricInfo) {
      if (metricInfo instanceof CounterMetricInfo) {
         ((CounterMetricInfo<Object>) metricInfo).accept(targetInstance, CounterTracker.NO_OP);
      } else if (metricInfo instanceof FunctionTimerMetricInfo) {
         ((FunctionTimerMetricInfo<Object>) metricInfo).accept(targetInstance, TimerTracker.NO_OP);
      }
   }

   @SuppressWarnings("unchecked")
   private Meter.Id onHistogramEnabled(Object targetInstance, String name, MetricInfo metricInfo, Map<String, String> commonTags) {
      if (metricInfo instanceof TimerMetricInfo) {
         return createTimer(targetInstance, name, (TimerMetricInfo<Object>) metricInfo, commonTags);
      } else if (metricInfo instanceof DistributionSummaryMetricInfo) {
         return createSummary(targetInstance, name, (DistributionSummaryMetricInfo<Object>) metricInfo, commonTags);
      }
      return null;
   }

   @SuppressWarnings("unchecked")
   private void onHistogramDisabled(Object targetInstance, MetricInfo metricInfo) {
      if (metricInfo instanceof TimerMetricInfo) {
         ((TimerMetricInfo<Object>) metricInfo).accept(targetInstance, TimerTracker.NO_OP);
      } else if (metricInfo instanceof DistributionSummaryMetricInfo) {
         ((DistributionSummaryMetricInfo<Object>) metricInfo).accept(targetInstance, DistributionSummaryTracker.NO_OP);
      }
   }

   private Collection<Tag> createTags(Map<String, String> attrTags, Map<String, String> tags) {
      Map<String, String> allTags = new TreeMap<>();
      allTags.put(CACHE_MANAGER_TAG_NAME, globalConfiguration.cacheManagerName());
      if (tags != null) {
         allTags.putAll(tags);
      }
      if (attrTags != null) {
         allTags.putAll(attrTags);
      }
      return allTags.entrySet().stream()
            .map(AbstractMetricsRegistry::tagFromMapEntry)
            .collect(Collectors.toList());
   }

   private static Tag tagFromMapEntry(Map.Entry<String, String> entry) {
      return Tag.of(entry.getKey(), entry.getValue());
   }

   private static String metricName(String prefix, MetricInfo info) {
      return prefix + NameUtils.decamelize(info.getName());
   }

   private Meter.Id createTimeGauge(Object instance, String name, TimeGaugeMetricInfo<Object> info, Map<String, String> tags) {
      return TimeGauge.builder(name, info.getGauge(instance), info.getTimeUnit())
            .strongReference(true)
            .tags(createTags(info.getTags(), tags))
            .description(info.getDescription())
            .register(registry.registry())
            .getId();
   }

   private Meter.Id createGauge(Object instance, String name, GaugeMetricInfo<Object> info, Map<String, String> tags) {
      return Gauge.builder(name, info.getGauge(instance))
            .strongReference(true)
            .tags(createTags(info.getTags(), tags))
            .description(info.getDescription())
            .register(registry.registry())
            .getId();
   }

   private Meter.Id createCounter(Object instance, String name, CounterMetricInfo<Object> info, Map<String, String> tags) {
      Counter counter = Counter.builder(name)
            .description(info.getDescription())
            .tags(createTags(info.getTags(), tags))
            .register(registry.registry());
      info.accept(instance, new MicrometerCounterTracker(counter));
      return counter.getId();
   }

   private Meter.Id createFunctionTimer(Object instance, String name, FunctionTimerMetricInfo<Object> info, Map<String, String> tags) {
      var timerTracker = new SimpleTimerTracker();
      info.accept(instance, timerTracker);
      return FunctionTimer.builder(name, timerTracker, SimpleTimerTracker::count, SimpleTimerTracker::totalTime, TimeUnit.NANOSECONDS)
            .description(info.getDescription())
            .tags(createTags(info.getTags(), tags))
            .register(registry.registry())
            .getId();
   }

   private Meter.Id createTimer(Object instance, String name, TimerMetricInfo<Object> info, Map<String, String> tags) {
      Timer timer = Timer.builder(name)
            .description(info.getDescription())
            .publishPercentileHistogram(true)
            .tags(createTags(info.getTags(), tags))
            .register(registry.registry());
      info.accept(instance, new MicrometerTimerTracker(timer));
      return timer.getId();
   }

   private Meter.Id createSummary(Object instance, String name, DistributionSummaryMetricInfo<Object> info, Map<String, String> tags) {
      DistributionSummary summary = DistributionSummary.builder(name)
            .description(info.getDescription())
            .publishPercentileHistogram(true)
            .tags(createTags(info.getTags(), tags))
            .register(registry.registry());
      info.accept(instance, new MicrometerDistributionSummary(summary));
      return summary.getId();
   }

   protected interface ScrapeRegistry {
      boolean supportsScrape();

      String scrape(String contentType);

      MeterRegistry registry();

      boolean externalManaged();
   }

   protected record NoScrapeRegistry(MeterRegistry registry, boolean externalManaged) implements ScrapeRegistry {

      @Override
      public boolean supportsScrape() {
         return false;
      }

      @Override
      public String scrape(String contentType) {
         return null;
      }
   }
}
