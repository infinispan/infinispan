package org.infinispan.metrics.impl;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;
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
public final class ApplicationMetricsRegistry {

   private final MetricRegistry applicationMetricRegistry = MetricRegistries.get(MetricRegistry.Type.APPLICATION);

   public ApplicationMetricsRegistry() {
   }

   public MetricRegistry getRegistry() {
      return applicationMetricRegistry;
   }

   public void register(ResourceDMBean resourceDMBean) {
      ObjectName objectName = resourceDMBean.getObjectName();
      MBeanInfo mBeanInfo = resourceDMBean.getMBeanInfo();
      MBeanAttributeInfo[] mBeanAttributes = mBeanInfo.getAttributes();

      Tag[] tags = ObjectNameMapper.makeTags(objectName);

      for (MBeanAttributeInfo attr : mBeanAttributes) {
         if (!attr.isReadable()) {
            // skip unreadable attributes (if we ever come across such an odd case)
            continue;
         }

         // metrics are numbers, ignore other attributes
         String type = attr.getType();
         if (!type.equals("java.lang.Integer") && !type.equals("int") &&
               !type.equals("java.lang.Long") && !type.equals("long") &&
               !type.equals("java.lang.Short") && !type.equals("short") &&
               !type.equals("java.lang.Byte") && !type.equals("byte") &&
               !type.equals("java.lang.Double") && !type.equals("double") &&
               !type.equals("java.lang.Float") && !type.equals("float") &&
               !type.equals("java.math.BigDecimal") && !type.equals("java.math.BigInteger")) {
            continue;
         }

         String attrName = attr.getName();
         String metricName = ObjectNameMapper.makeMetricName(objectName, attrName);

         Supplier valueSupplier;
         try {
            valueSupplier = resourceDMBean.getAttributeValueSupplier(attrName);
         } catch (AttributeNotFoundException e) {
            throw new IllegalStateException(e);
         }

         Gauge<Number> gaugeMetric = () -> {
            try {
               Object val = valueSupplier.get();

               // some attribute values produced by our MBeans are actually strings (for historical reasons) and need to be parsed into a Number
               if (val != null && !(val instanceof Number)) {
                  String strVal = val.toString();
                  switch (type) {
                     case "java.lang.Integer":
                     case "int":
                        return Integer.valueOf(strVal);
                     case "java.lang.Long":
                     case "long":
                        return Long.valueOf(strVal);
                     case "java.lang.Short":
                     case "short":
                        return Short.valueOf(strVal);
                     case "java.lang.Byte":
                     case "byte":
                        return Byte.valueOf(strVal);
                     case "java.lang.Double":
                     case "double":
                        return Double.valueOf(strVal);
                     case "java.lang.Float":
                     case "float":
                        return Float.valueOf(strVal);
                     case "java.math.BigDecimal":
                        return new BigDecimal(strVal);
                     case "java.math.BigInteger":
                        return new BigInteger(strVal);
                  }
               }

               return (Number) val;
            } catch (Throwable e) {
               throw new IllegalStateException("Error while retrieving attribute '" + metricName + "'", e);
            }
         };

         Metadata metadata = new MetadataBuilder()
               .withType(MetricType.GAUGE)
               .withName(metricName)
               .withDisplayName(attr.getName())
               .withDescription(attr.getDescription())
               .build();

         getRegistry().register(metadata, gaugeMetric, tags);
      }
   }

   public void unregister(ResourceDMBean resourceDMBean) {
      unregister(resourceDMBean.getObjectName());
   }

   public void unregister(ObjectName objectName) {
      String prefix = ObjectNameMapper.makeMetricNamePrefix(objectName);
      Map<String, String> tags = ObjectNameMapper.makeTagMap(objectName);
      getRegistry().removeMatching((metricID, metric) -> metricID.getName().startsWith(prefix) && tags.equals(metricID.getTags()));
   }
}
