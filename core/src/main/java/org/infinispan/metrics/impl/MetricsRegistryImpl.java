package org.infinispan.metrics.impl;

import java.lang.invoke.MethodHandles;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import org.infinispan.commons.stat.CounterMetricInfo;
import org.infinispan.commons.stat.DistributionSummaryMetricInfo;
import org.infinispan.commons.stat.FunctionTimerMetricInfo;
import org.infinispan.commons.stat.GaugeMetricInfo;
import org.infinispan.commons.stat.MetricInfo;
import org.infinispan.commons.stat.TimeGaugeMetricInfo;
import org.infinispan.commons.stat.TimerMetricInfo;
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
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.TimeGauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

/**
 * Concrete implementation of {@link MetricsRegistry}.
 * <p>
 * It uses {@link MeterRegistry} from micrometer. It can use the instance configured from
 * {@link MicrometerMeterRegistryConfiguration#meterRegistry()} or, if not configured, it instantiates
 * {@link PrometheusMeterRegistry}.
 */
@Scope(Scopes.GLOBAL)
public class MetricsRegistryImpl implements MetricsRegistry {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   @Inject GlobalConfiguration globalConfiguration;
   private ScrapeRegistry registry;

   @Start
   public void start() {
      boolean registerListeners = registry == null || !registry.isExternalManaged();
      if (registry != null && !registry.isExternalManaged()) {
         registry.registry().close();
      }
      registry = createMeterRegistry(globalConfiguration);

      if (registerListeners) {
         new BaseAdditionalMetrics().bindTo(registry.registry());
         new VendorAdditionalMetrics().bindTo(registry.registry());
         registry.registry().config().onMeterAdded(MetricsRegistryImpl::onMeterAdded);
         registry.registry().config().onMeterRemoved(MetricsRegistryImpl::onMeterRemoved);
         registry.registry().config().onMeterRegistrationFailed(MetricsRegistryImpl::onMeterRegistrationFailed);
      }
   }

   @Stop
   public void stop() {
      if (registry == null || registry.isExternalManaged()) {
         return;
      }
      registry.registry().close();
   }

   private static ScrapeRegistry createMeterRegistry(GlobalConfiguration globalConfiguration) {
      MicrometerMeterRegistryConfiguration configuration = globalConfiguration.module(MicrometerMeterRegistryConfiguration.class);
      MeterRegistry registry = configuration == null ? null : configuration.meterRegistry();
      boolean externalManaged = true;
      if (registry == null) {
         registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
         externalManaged = false;
      }
      return registry instanceof PrometheusMeterRegistry ?
            new PrometheusRegistry((PrometheusMeterRegistry) registry, externalManaged) :
            new NoScrapeRegistry(registry, externalManaged);
   }

   @Override
   public Set<Object> registerMetrics(Object instance, Collection<MetricInfo> metricInfos, String prefix, Map<String, String> tags) {
      Set<Object> metricIds = new HashSet<>(metricInfos.size());

      for (MetricInfo info : metricInfos) {
         if (globalConfiguration.metrics().gauges()) {
            var id = onGaugeEnabled(instance, prefix, info, tags);
            if (id != null) {
               metricIds.add(id);
               continue;
            }
         }

         if (globalConfiguration.metrics().histograms()) {
            var id = onHistogramEnabled(instance, prefix, info, tags);
            if (id != null) {
               metricIds.add(id);
               continue;
            }
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
   public MeterRegistry registry() {
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
   private Meter.Id onGaugeEnabled(Object targetInstance, String prefix, MetricInfo metricInfo, Map<String, String> commonTags) {
      if (metricInfo instanceof GaugeMetricInfo) {
         return createGauge(targetInstance, prefix, (GaugeMetricInfo<Object>) metricInfo, commonTags);
      } else if (metricInfo instanceof CounterMetricInfo) {
         return createCounter(targetInstance, prefix, (CounterMetricInfo<Object>) metricInfo, commonTags);
      } else if (metricInfo instanceof FunctionTimerMetricInfo) {
         return createFunctionTimer(targetInstance, prefix, (FunctionTimerMetricInfo<Object>) metricInfo, commonTags);
      } else if (metricInfo instanceof TimeGaugeMetricInfo) {
         return createTimeGauge(targetInstance, prefix, (TimeGaugeMetricInfo<Object>) metricInfo, commonTags);
      }
      return null;
   }

   @SuppressWarnings("unchecked")
   private Meter.Id onHistogramEnabled(Object targetInstance, String prefix, MetricInfo metricInfo, Map<String, String> commonTags) {
      if (metricInfo instanceof TimerMetricInfo) {
         return createTimer(targetInstance, prefix, (TimerMetricInfo<Object>) metricInfo, commonTags);
      } else if (metricInfo instanceof DistributionSummaryMetricInfo) {
         return createSummary(targetInstance, prefix, (DistributionSummaryMetricInfo<Object>) metricInfo, commonTags);
      }
      return null;
   }

   private Collection<Tag> createTags(Map<String, String> attrTags, Map<String, String> tags) {
      Map<String, String> allTags = new TreeMap<>();
      if (namesAsTags()) {
         allTags.put(CACHE_MANAGER_TAG_NAME, globalConfiguration.cacheManagerName());
      }
      if (tags != null) {
         allTags.putAll(tags);
      }
      if (attrTags != null) {
         allTags.putAll(attrTags);
      }
      return allTags.entrySet().stream()
            .map(MetricsRegistryImpl::tagFromMapEntry)
            .collect(Collectors.toList());
   }

   private static Tag tagFromMapEntry(Map.Entry<String, String> entry) {
      return Tag.of(entry.getKey(), entry.getValue());
   }

   private static String metricName(String prefix, MetricInfo info) {
      return VendorAdditionalMetrics.PREFIX + prefix + NameUtils.decamelize(info.getName());
   }

   private Meter.Id createTimeGauge(Object instance, String prefix, TimeGaugeMetricInfo<Object> info, Map<String, String> tags) {
      return TimeGauge.builder(metricName(prefix, info), info.getGauge(instance), info.getTimeUnit())
            .strongReference(true)
            .tags(createTags(info.getTags(), tags))
            .description(info.getDescription())
            .register(registry.registry())
            .getId();
   }

   private Meter.Id createGauge(Object instance, String prefix, GaugeMetricInfo<Object> info, Map<String, String> tags) {
      return Gauge.builder(metricName(prefix, info), info.getGauge(instance))
            .strongReference(true)
            .tags(createTags(info.getTags(), tags))
            .description(info.getDescription())
            .register(registry.registry())
            .getId();
   }

   private Meter.Id createCounter(Object instance, String prefix, CounterMetricInfo<Object> info, Map<String, String> tags) {
      Counter counter = Counter.builder(metricName(prefix, info))
            .description(info.getDescription())
            .tags(createTags(info.getTags(), tags))
            .register(registry.registry());
      info.accept(instance, new CounterTrackerImpl(counter));
      return counter.getId();
   }

   private Meter.Id createFunctionTimer(Object instance, String prefix, FunctionTimerMetricInfo<Object> info, Map<String, String> tags) {
      FunctionTimerTrackerImpl timerTracker = new FunctionTimerTrackerImpl(info.getTimeUnit());
      info.accept(instance, timerTracker);
      return timerTracker.create(metricName(prefix, info))
            .description(info.getDescription())
            .tags(createTags(info.getTags(), tags))
            .register(registry.registry())
            .getId();
   }

   private Meter.Id createTimer(Object instance, String prefix, TimerMetricInfo<Object> info, Map<String, String> tags) {
      Timer timer = Timer.builder(metricName(prefix, info))
            .description(info.getDescription())
            .publishPercentileHistogram(true)
            .tags(createTags(info.getTags(), tags))
            .register(registry.registry());
      info.accept(instance, new TimerTrackerImpl(timer));
      return timer.getId();
   }

   private Meter.Id createSummary(Object instance, String prefix, DistributionSummaryMetricInfo<Object> info, Map<String, String> tags) {
      DistributionSummary summary = DistributionSummary.builder(metricName(prefix, info))
            .description(info.getDescription())
            .publishPercentileHistogram(true)
            .tags(createTags(info.getTags(), tags))
            .register(registry.registry());
      info.accept(instance, new DistributionSummaryTrackerImpl(summary));
      return summary.getId();
   }

   private interface ScrapeRegistry {
      boolean supportsScrape();

      String scrape(String contentType);

      MeterRegistry registry();

      boolean isExternalManaged();
   }

   private static class PrometheusRegistry implements ScrapeRegistry {
      final PrometheusMeterRegistry registry;
      final boolean externalManaged;

      PrometheusRegistry(PrometheusMeterRegistry registry, boolean externalManaged) {
         this.registry = registry;
         this.externalManaged = externalManaged;
      }

      @Override
      public boolean supportsScrape() {
         return true;
      }

      @Override
      public String scrape(String contentType) {
         return registry.scrape(contentType);
      }

      @Override
      public MeterRegistry registry() {
         return registry;
      }

      @Override
      public boolean isExternalManaged() {
         return externalManaged;
      }
   }

   private static class NoScrapeRegistry implements ScrapeRegistry {
      final MeterRegistry registry;
      final boolean externalManaged;

      NoScrapeRegistry(MeterRegistry registry, boolean externalManaged) {
         this.registry = registry;
         this.externalManaged = externalManaged;
      }

      @Override
      public boolean supportsScrape() {
         return false;
      }

      @Override
      public String scrape(String contentType) {
         return null;
      }

      @Override
      public MeterRegistry registry() {
         return registry;
      }

      @Override
      public boolean isExternalManaged() {
         return externalManaged;
      }
   }
}
