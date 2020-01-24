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
import org.infinispan.util.function.TriConsumer;

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
   InfinispanMetricsRegistry infinispanMetricsRegistry;

   protected String nodeName;

   protected String namePrefix;

   private Set<MetricID> metricIds;

   @Start
   protected void start() {
      if (metricsEnabled()) {
         namePrefix = initNamePrefix();
         nodeName = initNodeName();
         metricIds = Collections.synchronizedSet(new HashSet<>());
         try {
            processComponents(this::registerMetrics);
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
               infinispanMetricsRegistry.unregister(metricId);
            }
            metricIds = null;
         } catch (Exception e) {
            throw new CacheException("Failure while unregistering metrics", e);
         }
      }
   }

   protected boolean metricsEnabled() {
      return infinispanMetricsRegistry != null
            && globalConfig.statistics()
            && globalConfig.metrics().enabled();
   }

   protected abstract String initNamePrefix();

   protected abstract String initNodeName();

   private void processComponents(TriConsumer<Object, MBeanMetadata, String> consumer) {
      for (ComponentRef<?> component : basicComponentRegistry.getRegisteredComponents()) {
         if (!component.isAlias()) {
            Object instance = component.wired();
            if (instance != null) {
               MBeanMetadata beanMetadata = basicComponentRegistry.getMBeanMetadata(instance.getClass().getName());
               if (beanMetadata != null) {
                  consumer.accept(instance, beanMetadata, component.getName());
               }
            }
         }
      }
   }

   private void registerMetrics(Object instance, MBeanMetadata mBeanMetadata, String componentName) {
      String jmxObjectName = mBeanMetadata.getJmxObjectName();
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
      metricIds.addAll(infinispanMetricsRegistry.register(metricsCfg, nodeName, instance, mBeanMetadata, metricPrefix));
   }
}
