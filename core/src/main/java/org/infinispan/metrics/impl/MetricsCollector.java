package org.infinispan.metrics.impl;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.infinispan.commons.stat.TimerTracker;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalMetricsConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.impl.MBeanMetadata;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metrics.Constants;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

/**
 * Keeps a reference to the Micrometer MeterRegistry. Optional component in component registry. Availability based on
 * available jars in classpath! See {@link MetricsCollectorFactory}.
 *
 * @author anistor@redhat.com
 * @since 10.1
 */
@Scope(Scopes.GLOBAL)
@SurvivesRestarts
public class MetricsCollector implements Constants {

   private static final Log log = LogFactory.getLog(MetricsCollector.class);

   private PrometheusMeterRegistry baseRegistry;
   private PrometheusMeterRegistry vendorRegistry;

   private Tag nodeTag;

   private Tag cacheManagerTag;

   @Inject
   GlobalConfiguration globalConfig;

   @Inject
   ComponentRef<Transport> transportRef;

   protected MetricsCollector() {
   }

   public PrometheusMeterRegistry getBaseRegistry() {
      return baseRegistry;
   }

   public PrometheusMeterRegistry getVendorRegistry() {
      return vendorRegistry;
   }

   @Start
   protected void start() {
      baseRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
      baseRegistry.config().meterFilter(new BaseFilter());
      new BaseAdditionalMetrics().bindTo(baseRegistry);

      vendorRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
      vendorRegistry.config().meterFilter(new VendorFilter());
      new VendorAdditionalMetrics().bindTo(vendorRegistry);

      Transport transport = transportRef.running();
      String nodeName = transport != null ? transport.getAddress().toString() : globalConfig.transport().nodeName();
      if (nodeName == null) {
         //TODO [anistor] Maybe we should just ensure a unique node name was set in all tests and also in real life usage, even for local cache managers
         nodeName = generateRandomName();
         //throw new CacheConfigurationException("Node name must always be specified in configuration if metrics are enabled.");
      }
      nodeTag = Tag.of(NODE_TAG_NAME, nodeName);

      if (globalConfig.metrics().namesAsTags()) {
         cacheManagerTag = Tag.of(CACHE_MANAGER_TAG_NAME, globalConfig.cacheManagerName());
      }
   }

   @Stop
   protected void stop() {
      try {
         if (baseRegistry != null) {
            baseRegistry.close();
            baseRegistry = null;
         }
      } finally {
         if (vendorRegistry != null) {
            vendorRegistry.close();
            vendorRegistry = null;
         }
      }
   }

   /**
    * Generate a not so random name based on host name.
    */
   private static String generateRandomName() {
      String hostName = null;
      try {
         hostName = InetAddress.getLocalHost().getHostName();
      } catch (Throwable ignored) {
      }
      if (hostName == null) {
         try {
            hostName = InetAddress.getByName(null).getHostName();
         } catch (Throwable ignored) {
         }
      }
      if (hostName == null) {
         hostName = "localhost";
      } else {
         int dotPos = hostName.indexOf('.');
         if (dotPos > 0 && !Character.isDigit(hostName.charAt(0))) {
            hostName = hostName.substring(0, dotPos);
         }
      }
      int rand = 1 + ThreadLocalRandom.current().nextInt(Short.MAX_VALUE * 2);
      return hostName + '-' + rand;
   }

   @SuppressWarnings("unchecked")
   public Set<Object> registerMetrics(Object instance, Collection<MBeanMetadata.AttributeMetadata> attributes, String namePrefix, String cacheName) {
      return registerMetrics(instance, attributes, namePrefix, cacheName, nodeTag);
   }

   public Set<Object> registerMetrics(Object instance, Collection<MBeanMetadata.AttributeMetadata> attributes, String namePrefix, String cacheName, String nodeName) {
      return registerMetrics(instance, attributes, namePrefix, cacheName, nodeName == null ? null : Tag.of(NODE_TAG_NAME, nodeName));
   }

   private Set<Object> registerMetrics(Object instance, Collection<MBeanMetadata.AttributeMetadata> attributes, String namePrefix, String cacheName, Tag nodeTag) {
      Set<Object> metricIds = new HashSet<>(attributes.size());

      GlobalMetricsConfiguration metricsCfg = globalConfig.metrics();
      int numTags = 1;
      if (cacheManagerTag != null) {
         numTags++;
         if (cacheName != null) {
            numTags++;
         }
      }

      ArrayList<Tag> tags = new ArrayList<>(numTags);
      if (nodeTag != null) {
         // in some case this can be null,
         // e.g. if it is called:
         // from org.infinispan.remoting.transport.jgroups.JGroupsTransport.lambda$channelConnected
         tags.add(nodeTag);
      }

      if (cacheManagerTag != null) {
         tags.add(cacheManagerTag);
         if (cacheName != null) {
            tags.add(Tag.of(CACHE_TAG_NAME, cacheName));
         }
      }

      for (MBeanMetadata.AttributeMetadata attr : attributes) {
         Supplier<Number> getter = (Supplier<Number>) attr.getter(instance);
         Consumer<TimerTracker> setter = (Consumer<TimerTracker>) attr.setter(instance);

         if (getter != null || setter != null) {
            String metricName = namePrefix + NameUtils.decamelize(attr.getName());

            if (getter != null) {
               if (metricsCfg.gauges()) {
                  Gauge gauge = Gauge.builder(metricName, getter)
                        .tags(tags)
                        .strongReference(true)
                        .description(attr.getDescription())
                        .register(vendorRegistry);

                  Meter.Id id = gauge.getId();

                  if (log.isTraceEnabled()) {
                     log.tracef("Registering gauge metric %s", id);
                  }
                  metricIds.add(id);
               }
            } else {
               if (metricsCfg.histograms()) {
                  Timer timer = Timer.builder(metricName)
                        .tags(tags)
                        .description(attr.getDescription())
                        .register(vendorRegistry);

                  Meter.Id id = timer.getId();

                  if (log.isTraceEnabled()) {
                     log.tracef("Registering histogram metric %s", id);
                  }
                  setter.accept(new TimerTrackerImpl(timer));
                  metricIds.add(id);
               }
            }
         }
      }

      if (log.isTraceEnabled()) {
         log.tracef("Registered %d metrics. Metric registry @%x contains %d metrics.",
               metricIds.size(), System.identityHashCode(vendorRegistry), vendorRegistry.getMeters().size());
      }

      return metricIds;
   }

   public void unregisterMetric(Object metricId) {
      if (vendorRegistry == null) {
         return;
      }

      Meter removed = vendorRegistry.remove((Meter.Id) metricId);
      if (log.isTraceEnabled()) {
         if (removed != null) {
            log.tracef("Unregistered metric \"%s\". Metric registry @%x contains %d metrics.",
                  metricId, System.identityHashCode(vendorRegistry), vendorRegistry.getMeters().size());
         } else {
            log.tracef("Could not remove unexisting metric \"%s\". Metric registry @%x contains %d metrics.",
                  metricId, System.identityHashCode(vendorRegistry), vendorRegistry.getMeters().size());
         }
      }
   }

   private static class BaseFilter implements MeterFilter {
      private static final String PREFIX = "base.";

      @Override
      public Meter.Id map(Meter.Id id) {
         return id.withName(PREFIX + id.getName());
      }
   }

   private static class VendorFilter implements MeterFilter {
      private static final String PREFIX = "vendor.";

      @Override
      public Meter.Id map(Meter.Id id) {
         return id.withName(PREFIX + id.getName());
      }
   }
}
