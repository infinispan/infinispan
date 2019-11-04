package org.infinispan.metrics.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.ObjectName;

import org.eclipse.microprofile.metrics.MetricID;
import org.eclipse.microprofile.metrics.Tag;
import org.infinispan.jmx.ObjectNameKeys;

/**
 * @author anistor@redhat.com
 * @since 10.0
 */
public final class ObjectNameMapper {

   /**
    * A pseudo tag added to represent the JXM domain of the MBean.
    */
   public static final String JMX_DOMAIN_TAG = "_domain";

   public static MetricID makeMetricId(ObjectName objectName, String attributeName) {
      return new MetricID(makeMetricName(objectName, attributeName), makeTags(objectName));
   }

   public static String makeMetricName(ObjectName objectName, String attributeName) {
      return makeMetricNamePrefix(objectName) + attributeName;
   }

   public static String makeMetricNamePrefix(ObjectName objectName) {
      StringBuilder sb = new StringBuilder();

      String typeKey = objectName.getKeyProperty(ObjectNameKeys.TYPE);
      if (typeKey != null) {
         sb.append(typeKey).append('_');
      }

      String componentKey = objectName.getKeyProperty(ObjectNameKeys.COMPONENT);
      // avoid situations where TYPE equals COMPONENT like CacheManager_CacheManager_numberOfCreatedCaches
      if (componentKey != null && !componentKey.equals(typeKey)) {
         sb.append(componentKey.replace('.', '_')).append('_');
      }

      return sb.toString();
   }

   public static Tag[] makeTags(ObjectName objectName) {
      Map<String, String> props = objectName.getKeyPropertyList();
      List<Tag> tags = new ArrayList<>(props.size());
      for (String name : props.keySet()) {
         if (!name.equals(ObjectNameKeys.TYPE) && !name.equals(ObjectNameKeys.COMPONENT)) {
            tags.add(new Tag(name, props.get(name)));
         }
      }
      // add a pseudo tag to represent the JMX domain
      tags.add(new Tag(JMX_DOMAIN_TAG, objectName.getDomain()));
      return tags.toArray(new Tag[0]);
   }

   public static Map<String, String> makeTagMap(ObjectName objectName) {
      Map<String, String> tags = new HashMap<>(objectName.getKeyPropertyList());
      tags.remove(ObjectNameKeys.TYPE);
      tags.remove(ObjectNameKeys.COMPONENT);
      tags.put(JMX_DOMAIN_TAG, objectName.getDomain());
      return tags;
   }
}
