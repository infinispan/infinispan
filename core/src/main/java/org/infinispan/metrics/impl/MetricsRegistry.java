package org.infinispan.metrics.impl;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.stat.MetricInfo;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalMetricsConfiguration;
import org.infinispan.metrics.Constants;

/**
 * A registry for metrics.
 * <p>
 * This component should not have any dependency on any other component. Also, the tag {@link #CACHE_MANAGER_TAG_NAME}
 * should be set and match the one defined in {@link GlobalConfiguration#cacheManagerName()}.
 * <p>
 * This registry should only be used if it needs to register metrics before the
 * {@link org.infinispan.remoting.transport.Transport} is started. Otherwise, {@link MetricsCollector} should be used as
 * it adds tags related to the node.
 */
public interface MetricsRegistry extends Constants {

   /**
    * Register the {@link org.infinispan.factories.impl.MBeanMetadata.AttributeMetadata} as metrics.
    *
    * @param instance   The instance from where metrics are collected.
    * @param attributes The attribute (metrics) to be registered, if valid.
    * @param namePrefix The prefix or the component name.
    * @param tags       Extra tags to be attached to the metrics.
    * @return A set of ids that can be used by {@link #unregisterMetric(Object)} or
    * {@link #unregisterMetrics(Collection)}.
    */
   Set<Object> registerMetrics(Object instance, Collection<MetricInfo> attributes, String namePrefix, Map<String, String> tags);

   /**
    * Unregisters a single metric.
    * <p>
    * If the {@code metricId} is not a valid id, this method does nothing.
    *
    * @param metricId The metric id as returned by {@link #registerMetrics(Object, Collection, String, Map)}.
    */
   void unregisterMetric(Object metricId);

   /**
    * Unregisters multiple metrics.
    * <p>
    * Check {@link #unregisterMetric(Object)} for more details.
    *
    * @param metricsId The metric id as returned by {@link #registerMetrics(Object, Collection, String, Map)}.
    */
   default void unregisterMetrics(Collection<Object> metricsId) {
      if (metricsId == null) {
         return;
      }
      metricsId.forEach(this::unregisterMetric);
   }

   /**
    * Same as {@link GlobalMetricsConfiguration#namesAsTags()}.
    *
    * @return {@code true} if it should use tags to identify the cluster, site or cache manager name.
    */
   boolean namesAsTags();

   /**
    * If this instance supports scrapping.
    *
    * @return {@code true} if it supports Prometheus scrapping.
    * @see #scrape(String)
    */
   boolean supportScrape();

   /**
    * Prometheus like scrapping of the registered metrics.
    * <p>
    * It returns {@code null} if {@link #supportScrape()} returns {@code false}.
    *
    * @param contentType A valid content-type.
    * @return A {@link String} with the metrics values.
    */
   String scrape(String contentType);
}
