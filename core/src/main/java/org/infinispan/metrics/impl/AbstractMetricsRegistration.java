package org.infinispan.metrics.impl;

import static org.infinispan.factories.impl.MBeanMetadata.AttributeMetadata;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.CacheException;
import org.infinispan.configuration.global.GlobalConfiguration;
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
   MetricsCollector metricsCollector;

   private String namePrefix;

   private Set<Object> metricIds;

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
         unregisterMetrics(metricIds);
         metricIds = null;
      }
   }

   public abstract boolean metricsEnabled();

   /**
    * Subclasses should override this and return the metric prefix to be used for registration. This is invoked only if
    * metrics are enabled.
    */
   protected String initNamePrefix() {
      String prefix = globalConfig.metrics().namesAsTags() ?
            "" : "cache_manager_" + NameUtils.filterIllegalChars(globalConfig.cacheManagerName()) + '_';
      String globalPrefix = globalConfig.metrics().prefix();
      return globalPrefix != null && !globalPrefix.isEmpty() ? globalPrefix + '_' + prefix : prefix;
   }

   private void processComponents() {
      for (ComponentRef<?> component : basicComponentRegistry.getRegisteredComponents()) {
         if (!component.isAlias()) {
            Object instance = component.wired();
            if (instance != null) {
               MBeanMetadata beanMetadata = basicComponentRegistry.getMBeanMetadata(instance.getClass().getName());
               if (beanMetadata != null) {
                  Set<Object> ids = registerMetrics(instance, beanMetadata.getJmxObjectName(), beanMetadata.getAttributes(), null, component.getName(), null);
                  metricIds.addAll(ids);
                  if (instance instanceof CustomMetricsSupplier) {
                     metricIds.addAll(registerMetrics(instance, beanMetadata.getJmxObjectName(), ((CustomMetricsSupplier) instance).getCustomMetrics(), null, component.getName(), null));
                  }
               }
            }
         }
      }
   }

   private Set<Object> registerMetrics(Object instance, String jmxObjectName, Collection<AttributeMetadata> attributes,  String type, String componentName, String prefix) {
      if (jmxObjectName == null) {
         jmxObjectName = componentName;
      }
      if (jmxObjectName == null) {
         throw new IllegalArgumentException("No MBean name and no component name was specified");
      }
      String metricPrefix = namePrefix;
      if (!jmxObjectName.equals("Cache") && !jmxObjectName.equals("CacheManager")) {
         if (prefix != null) {
            metricPrefix += NameUtils.decamelize(prefix) + '_';
         }
         if (type != null && !type.equals(jmxObjectName)) {
            metricPrefix += NameUtils.decamelize(type) + '_';
         }
         metricPrefix += NameUtils.decamelize(jmxObjectName) + '_';
      }
      return internalRegisterMetrics(instance, attributes, metricPrefix);
   }

   protected abstract Set<Object> internalRegisterMetrics(Object instance, Collection<AttributeMetadata> attributes, String metricPrefix);

   /**
    * Register metrics for a component that was manually registered later, after component registry startup. The metric
    * ids will be tracked and unregistration will be performed automatically on stop.
    */
   public void registerMetrics(Object instance, String type, String componentName) {
      if (metricsCollector == null) {
         throw new IllegalStateException("Microprofile metrics are not initialized");
      }
      MBeanMetadata beanMetadata = basicComponentRegistry.getMBeanMetadata(instance.getClass().getName());
      if (beanMetadata == null) {
         throw new IllegalArgumentException("No MBean metadata available for " + instance.getClass().getName());
      }
      Set<Object> ids = registerMetrics(instance, beanMetadata.getJmxObjectName(), beanMetadata.getAttributes(), type, componentName, null);
      metricIds.addAll(ids);
   }

   /**
    * Register metrics for a component that was manually registered later, after component registry startup. The metric
    * ids will <b>NOT</b> be tracked and unregistration will <b>NOT</b> be performed automatically on stop.
    */
   public Set<Object> registerExternalMetrics(Object instance, String prefix) {
      if (metricsCollector == null) {
         throw new IllegalStateException("Microprofile metrics are not initialized");
      }
      MBeanMetadata beanMetadata = basicComponentRegistry.getMBeanMetadata(instance.getClass().getName());
      if (beanMetadata == null) {
         throw new IllegalArgumentException("No MBean metadata available for " + instance.getClass().getName());
      }
      return registerMetrics(instance, beanMetadata.getJmxObjectName(), beanMetadata.getAttributes(), null, null, prefix);
   }

   public void unregisterMetrics(Set<Object> metricIds) {
      if (metricsCollector == null) {
         throw new IllegalStateException("Microprofile metrics are not initialized");
      }
      try {
         for (Object metricId : metricIds) {
            metricsCollector.unregisterMetric(metricId);
         }
      } catch (Exception e) {
         throw new CacheException("Failure while unregistering metrics", e);
      }
   }
}
