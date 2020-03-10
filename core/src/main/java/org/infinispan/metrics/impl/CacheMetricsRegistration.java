package org.infinispan.metrics.impl;

import java.util.Set;

import org.eclipse.microprofile.metrics.MetricID;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.impl.MBeanMetadata;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Creates and registers metrics for all components from a cache's component registry.
 *
 * @author anistor@redhat.com
 * @since 10.1.3
 */
@Scope(Scopes.NAMED_CACHE)
@SurvivesRestarts
public final class CacheMetricsRegistration extends AbstractMetricsRegistration {

   @Inject
   Configuration cacheConfiguration;

   @ComponentName(KnownComponentNames.CACHE_NAME)
   @Inject
   String cacheName;

   @Inject
   CacheManagerMetricsRegistration globalMetricsRegistration;

   @Override
   public boolean metricsEnabled() {
      return globalMetricsRegistration.metricsEnabled() && cacheConfiguration.statistics().enabled();
   }

   @Override
   protected String initNamePrefix() {
      String prefix = globalMetricsRegistration.namePrefix;
      if (!globalConfig.metrics().namesAsTags()) {
         prefix += "cache_" + NameUtils.filterIllegalChars(cacheName) + '_';
      }
      return prefix;
   }

   @Override
   protected Set<MetricID> internalRegisterMetrics(Object instance, MBeanMetadata beanMetadata, String metricPrefix) {
      return metricsCollector.registerMetrics(instance, beanMetadata, metricPrefix, cacheName);
   }
}
