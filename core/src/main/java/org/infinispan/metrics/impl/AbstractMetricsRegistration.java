package org.infinispan.metrics.impl;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.eclipse.microprofile.metrics.MetricID;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalMetricsConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.impl.MBeanMetadata;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Parent class for microprofile metrics registration. Gathers all components in component registry and registers
 * metrics for them.
 *
 * @author anistor@redhat.com
 * @since 10.1.3
 */
@Scope(Scopes.NONE)
abstract class AbstractMetricsRegistration {

   @Inject
   GlobalConfiguration globalConfig;

   @Inject
   BasicComponentRegistry basicComponentRegistry;

   @Inject
   InfinispanMetricRegistry infinispanMetricRegistry;

   protected String namePrefix;

   private Set<MetricID> metricIds;

   @Start
   protected void start() {
      if (metricsEnabled()) {
         namePrefix = initNamePrefix();
         metricIds = Collections.synchronizedSet(new HashSet<>());
         try {
            processComponents();
         } catch (Exception e) {
            throw new CacheException("Failure while registering metrics", e);
         }
      }
   }

   @Stop
   protected void stop() {
      if (metricIds != null) {
         try {
            for (MetricID metricId : metricIds) {
               infinispanMetricRegistry.unregisterMetric(metricId);
            }
            metricIds = null;
         } catch (Exception e) {
            throw new CacheException("Failure while unregistering metrics", e);
         }
      }
   }

   protected boolean metricsEnabled() {
      return infinispanMetricRegistry != null && globalConfig.metrics().enabled();
   }

   protected abstract String initNamePrefix();

   private void processComponents() {
      for (ComponentRef<?> component : basicComponentRegistry.getRegisteredComponents()) {
         if (!component.isAlias()) {
            Object instance = component.wired();
            if (instance != null) {
               MBeanMetadata beanMetadata = basicComponentRegistry.getMBeanMetadata(instance.getClass().getName());
               if (beanMetadata != null) {
                  registerMetrics(instance, beanMetadata, component.getName());
               }
            }
         }
      }
   }

   //todo [anistor] the 'type' attribute from the ObjectName should be probably used too
   private void registerMetrics(Object instance, MBeanMetadata beanMetadata, String componentName) {
      String jmxObjectName = beanMetadata.getJmxObjectName();
      if (jmxObjectName == null) {
         jmxObjectName = componentName;
      }
      String metricPrefix = namePrefix;
      if (!jmxObjectName.equals("Cache") && !jmxObjectName.equals("CacheManager")) {
         metricPrefix = namePrefix + "_" + NameUtils.decamelize(jmxObjectName);
      }
      GlobalMetricsConfiguration metricsCfg = globalConfig.metrics();
      if (metricsCfg.prefix() != null && !metricsCfg.prefix().isEmpty()) {
         metricPrefix = metricsCfg.prefix() + "_" + metricPrefix;
      }
      Set<MetricID> ids = infinispanMetricRegistry.registerMetrics(instance, beanMetadata, metricPrefix);
      metricIds.addAll(ids);
   }
}
