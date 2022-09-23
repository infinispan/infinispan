package org.infinispan.metrics.impl;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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

   private PrometheusMeterRegistry registry;

   private Tag nodeTag;

   private Tag cacheManagerTag;

   @Inject
   GlobalConfiguration globalConfig;

   @Inject
   ComponentRef<Transport> transportRef;

   protected MetricsCollector() {
   }

   public PrometheusMeterRegistry registry() {
      return registry;
   }

   @Start
   protected void start() {
      registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
      new BaseAdditionalMetrics().bindTo(registry);
      new VendorAdditionalMetrics().bindTo(registry);

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
      if (registry != null) {
         registry.close();
         registry = null;
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
      return registerMetrics(instance, attributes, namePrefix, asTag(CACHE_TAG_NAME, cacheName), nodeTag);
   }

   public Set<Object> registerMetrics(Object instance, Collection<MBeanMetadata.AttributeMetadata> attributes, String namePrefix, String cacheName, String nodeName) {
      return registerMetrics(instance, attributes, namePrefix, asTag(CACHE_TAG_NAME, cacheName), asTag(NODE_TAG_NAME, nodeName));
   }

   private Set<Object> registerMetrics(Object instance, Collection<MBeanMetadata.AttributeMetadata> attributes, String namePrefix, Tag ...initialTags) {
      Set<Object> metricIds = new HashSet<>(attributes.size());

      GlobalMetricsConfiguration metricsCfg = globalConfig.metrics();
      List<Tag> tags = prepareTags(initialTags);

      for (MBeanMetadata.AttributeMetadata attr : attributes) {
         Supplier<Number> getter = (Supplier<Number>) attr.getter(instance);
         Consumer<TimerTracker> setter = (Consumer<TimerTracker>) attr.setter(instance);

         if (getter != null || setter != null) {
            String metricName = VendorAdditionalMetrics.PREFIX + namePrefix + NameUtils.decamelize(attr.getName());

            if (getter != null) {
               if (metricsCfg.gauges()) {
                  Gauge gauge = Gauge.builder(metricName, getter)
                        .tags(tags)
                        .strongReference(true)
                        .description(attr.getDescription())
                        .register(registry);

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
                        .register(registry);

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
               metricIds.size(), System.identityHashCode(registry), registry.getMeters().size());
      }

      return metricIds;
   }

   private List<Tag> prepareTags(Tag ...tags) {
      List<Tag> allTags = Arrays.stream(tags).filter(Objects::nonNull).collect(Collectors.toList());
      if (cacheManagerTag != null) allTags.add(cacheManagerTag);

      return allTags;
   }

   private Tag asTag(String key, String value) {
      return value != null
            ? Tag.of(key, value)
            : null;
   }

   public void unregisterMetric(Object metricId) {
      if (registry == null) {
         return;
      }

      Meter removed = registry.remove((Meter.Id) metricId);
      if (log.isTraceEnabled()) {
         if (removed != null) {
            log.tracef("Unregistered metric \"%s\". Metric registry @%x contains %d metrics.",
                  metricId, System.identityHashCode(registry), registry.getMeters().size());
         } else {
            log.tracef("Could not remove unexisting metric \"%s\". Metric registry @%x contains %d metrics.",
                  metricId, System.identityHashCode(registry), registry.getMeters().size());
         }
      }
   }
}
