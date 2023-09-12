package org.infinispan.metrics.impl;

import java.util.Collection;
import java.util.Set;

import org.infinispan.commons.stat.MetricInfo;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Creates and registers metrics for all components from a cache manager's global component registry.
 *
 * @author anistor@redhat.com
 * @since 10.1.3
 */
@Scope(Scopes.GLOBAL)
@SurvivesRestarts
public final class CacheManagerMetricsRegistration extends AbstractMetricsRegistration {

   @Override
   public boolean metricsEnabled() {
      return metricsCollector != null && globalConfig.statistics();
   }

   @Override
   protected Set<Object> internalRegisterMetrics(Object instance, Collection<MetricInfo> metrics, String metricPrefix) {
      return metricsCollector.registerMetrics(instance, metrics, metricPrefix, null);
   }
}
