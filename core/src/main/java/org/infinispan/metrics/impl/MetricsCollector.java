package org.infinispan.metrics.impl;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.eclipse.microprofile.metrics.Gauge;
import org.eclipse.microprofile.metrics.Metadata;
import org.eclipse.microprofile.metrics.MetadataBuilder;
import org.eclipse.microprofile.metrics.Metric;
import org.eclipse.microprofile.metrics.MetricID;
import org.eclipse.microprofile.metrics.MetricRegistry;
import org.eclipse.microprofile.metrics.MetricType;
import org.eclipse.microprofile.metrics.MetricUnits;
import org.eclipse.microprofile.metrics.Tag;
import org.eclipse.microprofile.metrics.Timer;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalMetricsConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.impl.MBeanMetadata;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Keeps a reference to the microprofile MetricRegistry. Optional component in component registry. Availability based on
 * available jars in classpath! See {@link MetricsCollectorFactory}.
 *
 * @author anistor@redhat.com
 * @since 10.1.3
 */
@Scope(Scopes.GLOBAL)
@SurvivesRestarts
public class MetricsCollector {

   private static final Log log = LogFactory.getLog(MetricsCollector.class);

   public static final String NODE_TAG_NAME = "node";

   private final MetricRegistry registry;

   private Tag nodeTag;

   @Inject
   GlobalConfiguration globalConfig;

   @Inject
   ComponentRef<Transport> transportRef;

   protected MetricsCollector(MetricRegistry registry) {
      this.registry = registry;
   }

   @Start
   protected void start() {
      Transport transport = transportRef.running();
      String nodeName = transport != null ? transport.getAddress().toString() : globalConfig.transport().nodeName();
      if (nodeName == null) {
         //TODO [anistor] Maybe we should just ensure a unique node name was set in all tests and also in real life usage, even for local cache managers
         nodeName = generateRandomName();
         //throw new CacheConfigurationException("Node name must always be specified in configuration if metrics are enabled.");
      }
      nodeName = NameUtils.filterIllegalChars(nodeName);
      nodeTag = new Tag(NODE_TAG_NAME, nodeName);
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

   public Set<MetricID> registerMetrics(Object instance, MBeanMetadata beanMetadata, String namePrefix) {
      Set<MetricID> metricIds = new HashSet<>();

      GlobalMetricsConfiguration metricsCfg = globalConfig.metrics();
      for (MBeanMetadata.AttributeMetadata attr : beanMetadata.getAttributes()) {
         Supplier<?> getter = attr.getter(instance);
         Consumer<Metric> setter = (Consumer<Metric>) attr.setter(instance);

         if (getter != null || setter != null) {
            String metricName = namePrefix + '_' + NameUtils.decamelize(attr.getName());
            MetricID metricId = new MetricID(metricName, nodeTag);

            if (getter != null) {
               if (metricsCfg.gauges()) {
                  Gauge<Number> gaugeMetric = () -> (Number) getter.get();
                  Metadata metadata = new MetadataBuilder()
                        .withType(MetricType.GAUGE)
                        .withUnit(MetricUnits.NONE)
                        .withName(metricName)
                        .withDisplayName(attr.getName())
                        .withDescription(attr.getDescription())
                        .build();

                  if (log.isTraceEnabled()) {
                     log.tracef("Registering gauge metric %s", metricId);
                  }
                  registry.register(metadata, gaugeMetric, nodeTag);
                  metricIds.add(metricId);
               }
            } else {
               if (metricsCfg.histograms()) {
                  Metadata metadata = new MetadataBuilder()
                        .withType(MetricType.TIMER)
                        .withUnit(MetricUnits.NANOSECONDS)
                        .withName(metricName)
                        .withDisplayName(attr.getName())
                        .withDescription(attr.getDescription())
                        .build();

                  if (log.isTraceEnabled()) {
                     log.tracef("Registering histogram metric %s", metricId);
                  }
                  Timer timerMetric = registry.timer(metadata, nodeTag);
                  setter.accept(timerMetric);
                  metricIds.add(metricId);
               }
            }
         }
      }

      if (log.isTraceEnabled()) {
         log.tracef("Registered %d metrics. Metric registry @%x contains %d metrics.",
                    metricIds.size(), System.identityHashCode(registry), registry.getMetrics().size());
      }

      return metricIds;
   }

   public void unregisterMetric(MetricID metricId) {
      boolean removed = registry.remove(metricId);
      if (log.isTraceEnabled()) {
         if (removed) {
            log.tracef("Unregistered metric \"%s\". Metric registry @%x contains %d metrics.",
                       metricId, System.identityHashCode(registry), registry.getMetrics().size());
         } else {
            log.tracef("Could not remove unexisting metric \"%s\". Metric registry @%x contains %d metrics.",
                       metricId, System.identityHashCode(registry), registry.getMetrics().size());
         }
      }
   }
}
