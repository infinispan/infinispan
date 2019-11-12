package org.infinispan.metrics.impl;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import javax.management.AttributeNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.ObjectName;

import org.eclipse.microprofile.metrics.Gauge;
import org.eclipse.microprofile.metrics.Metadata;
import org.eclipse.microprofile.metrics.MetadataBuilder;
import org.eclipse.microprofile.metrics.MetricRegistry;
import org.eclipse.microprofile.metrics.MetricType;
import org.eclipse.microprofile.metrics.Tag;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.ResourceDMBean;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.smallrye.metrics.MetricRegistries;

// TODO this is temporarily coupled to our jmx infrastructure. It is not supposed to use any essential JMX stuff except
//  ObjectName and MBeanInfo. No access to the MBeanServer allowed. Only the methods in ResourceDMBeans that are
//  statically resolvable (no reflection) are allowed. Should work even with Graalvm's limitations.

/**
 * A bridge between ResourceDMBeans and microprofile metrics. Exposes all numeric JMX attributes as Gauge metrics
 * automatically. The metric id is generated automatically from the ObjectName.
 *
 * @author anistor@redhat.com
 * @since 10.0
 */
@Scope(Scopes.GLOBAL)
@SurvivesRestarts
public class ApplicationMetricsRegistry {

   private static final Log log = LogFactory.getLog(ApplicationMetricsRegistry.class);

   private final MetricRegistry applicationMetricRegistry;

   public ApplicationMetricsRegistry() {
      applicationMetricRegistry = makeRegistry();
   }

   protected MetricRegistry makeRegistry() {
      return MetricRegistries.get(MetricRegistry.Type.APPLICATION);
   }

   public final MetricRegistry getRegistry() {
      return applicationMetricRegistry;
   }

   public void register(ResourceDMBean resourceDMBean) {
      MetricRegistry registry = getRegistry();

      ObjectName objectName = resourceDMBean.getObjectName();
      int metricCounter = 0;
      if (log.isTraceEnabled()) {
         log.tracef("Metric registry @%x contains %d metrics. Registering metrics for ObjectName \"%s\" ...",
                    System.identityHashCode(registry), registry.getMetrics().size(), objectName);
      }
      MBeanInfo mBeanInfo = resourceDMBean.getMBeanInfo();
      MBeanAttributeInfo[] mBeanAttributes = mBeanInfo.getAttributes();

      Tag[] tags = ObjectNameMapper.makeTags(objectName);

      for (MBeanAttributeInfo attr : mBeanAttributes) {
         if (!attr.isReadable()) {
            // skip unreadable attributes (if we ever come across such an odd case)
            continue;
         }

         String attrName = attr.getName();
         Supplier valueSupplier;
         try {
            valueSupplier = resourceDMBean.getAttributeValueSupplier(attrName);
            if (valueSupplier == null) {
               continue;
            }
         } catch (AttributeNotFoundException e) {
            throw new IllegalStateException(e);
         }
         Gauge<Number> gaugeMetric = () -> (Number) valueSupplier.get();
         String metricName = ObjectNameMapper.makeMetricName(objectName, attrName);

         Metadata metadata = new MetadataBuilder()
               .withType(MetricType.GAUGE)
               .withName(metricName)
               .withDisplayName(attr.getName())
               .withDescription(attr.getDescription())
               .build();

         if (log.isTraceEnabled()) {
            log.tracef("Registering metric %s with tags %s", metricName, Arrays.toString(tags));
            metricCounter++;
         }
         registry.register(metadata, gaugeMetric, tags);
      }

      if (log.isTraceEnabled()) {
         log.tracef("Metric registry @%x contains %d metrics. Registered %d metrics for ObjectName \"%s\"",
                    System.identityHashCode(registry), registry.getMetrics().size(), metricCounter, objectName);
      }
   }

   public void unregister(ObjectName objectName) {
      MetricRegistry registry = getRegistry();

      if (log.isTraceEnabled()) {
         log.tracef("Metric registry @%x contains %d metrics. Unregistering metrics for ObjectName \"%s\" ...",
                    System.identityHashCode(registry), registry.getMetrics().size(), objectName);
      }
      String prefix = ObjectNameMapper.makeMetricNamePrefix(objectName);
      Map<String, String> tags = ObjectNameMapper.makeTagMap(objectName);
      AtomicInteger metricCounter = new AtomicInteger();
      registry.removeMatching((metricID, metric) -> {
         boolean isMatching = metricID.getName().startsWith(prefix) && tags.equals(metricID.getTags());
         if (log.isTraceEnabled() && isMatching) {
            log.tracef("Unregistering metric %s with tags %s", metricID.getName(), tags);
            metricCounter.getAndIncrement();
         }
         return isMatching;
      });
      if (log.isTraceEnabled()) {
         log.tracef("Metric registry @%x contains %d metrics. Unregistered %d metrics for ObjectName \"%s\"",
                    System.identityHashCode(registry), registry.getMetrics().size(), metricCounter.get(), objectName);
      }
   }
}
