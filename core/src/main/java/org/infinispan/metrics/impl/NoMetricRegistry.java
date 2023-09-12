package org.infinispan.metrics.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.stat.MetricInfo;

/**
 * An empty implementation of {@link MetricsRegistry}.
 * <p>
 * There is no registry in this instance and all the operations are no-ops. This instance is used when metrics are
 * disabled globally.
 */
public final class NoMetricRegistry implements MetricsRegistry {

   static final MetricsRegistry NO_OP_INSTANCE = new NoMetricRegistry();

   private NoMetricRegistry() {
   }

   @Override
   public Set<Object> registerMetrics(Object instance, Collection<MetricInfo> attributes, String namePrefix, Map<String, String> tags) {
      return Collections.emptySet();
   }

   @Override
   public void unregisterMetric(Object metricId) {
      //no-op
   }

   @Override
   public boolean namesAsTags() {
      return false;
   }

   @Override
   public boolean supportScrape() {
      return false;
   }

   @Override
   public String scrape(String contentType) {
      return null;
   }
}
