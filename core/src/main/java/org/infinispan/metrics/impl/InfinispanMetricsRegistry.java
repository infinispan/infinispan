package org.infinispan.metrics.impl;

import java.util.HashSet;
import java.util.Set;
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
import org.infinispan.configuration.global.GlobalMetricsConfiguration;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.impl.MBeanMetadata;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.smallrye.metrics.MetricRegistries;

/**
 * Keeps a reference to the microprofile MetricRegistry. Optional component in component registry. Availability based on
 * available jars in classpath!
 *
 * @author anistor@redhat.com
 * @since 10.1.3
 */
@Scope(Scopes.GLOBAL)
@SurvivesRestarts
public class InfinispanMetricsRegistry {

   private static final Log log = LogFactory.getLog(InfinispanMetricsRegistry.class);

   public static final String NODE_TAG_NAME = "node";

   private final MetricRegistry registry;

   public InfinispanMetricsRegistry() {
      registry = makeRegistry();
   }

   protected MetricRegistry makeRegistry() {
      return MetricRegistries.get(MetricRegistry.Type.VENDOR);
   }

   public final MetricRegistry getRegistry() {
      return registry;
   }

   public Set<MetricID> register(GlobalMetricsConfiguration metricsCfg, String nodeName, Object instance, MBeanMetadata mBeanMetadata, String namePrefix) {
      Tag nodeTag = new Tag(NODE_TAG_NAME, nodeName);
      Set<MetricID> metricIds = new HashSet<>();

      for (MBeanMetadata.AttributeMetadata attr : mBeanMetadata.getAttributes()) {
         Supplier<?> getter = attr.getter(instance);
         Consumer<Metric> setter = (Consumer<Metric>) attr.setter(instance);

         if (getter != null || setter != null) {
            String metricName = namePrefix + "_" + NameUtils.decamelize(attr.getName());
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
                     log.tracef("Registering metric %s", metricId);
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
                     log.tracef("Registering metric %s", metricId);
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

   public void unregister(MetricID metricId) {
      registry.remove(metricId);

      if (log.isTraceEnabled()) {
         log.tracef("Unregistered metric \"%s\". Metric registry @%x contains %d metrics.",
                    metricId, System.identityHashCode(registry), registry.getMetrics().size());
      }
   }
}
