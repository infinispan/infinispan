package org.infinispan.metrics.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.stat.MetricInfo;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.impl.MBeanMetadata;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metrics.Constants;

/**
 * Parent class for metrics registration. Gathers all components in component registry and registers
 * metrics for them.
 *
 * @author anistor@redhat.com
 * @since 10.1.3
 */
@Scope(Scopes.NONE)
abstract class AbstractMetricsRegistration implements Constants {

   @Inject
   GlobalConfiguration globalConfig;

   @Inject
   BasicComponentRegistry basicComponentRegistry;

   @Inject
   MetricsCollector metricsCollector;

   @Deprecated(forRemoval = true, since = "16.0")
   private String legacyNamePrefix;

   private Set<Object> metricIds;

   @Start
   protected void start() {
      if (metricsEnabled()) {
         legacyNamePrefix = initLegacyNamePrefix();
         metricIds = Collections.synchronizedSet(new HashSet<>());
         try {
            processComponents();
         } catch (Exception e) {
            throw new CacheException("A failure occurred while registering metrics.", e);
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
   @Deprecated(forRemoval = true, since = "16.0")
   protected String initLegacyNamePrefix() {
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
                  Set<Object> ids = registerAttributes(instance, beanMetadata.getJmxObjectName(), beanMetadata.getAttributes(), null, component.getName(), null);
                  metricIds.addAll(ids);
                  if (instance instanceof CustomMetricsSupplier) {
                     metricIds.addAll(registerMetrics(instance, beanMetadata.getJmxObjectName(), ((CustomMetricsSupplier) instance).getCustomMetrics(globalConfig.metrics()), null, component.getName(), null));
                  }
               }
            }
         }
      }
   }

   private Set<Object> registerAttributes(Object instance, String jmxObjectName, Collection<MBeanMetadata.AttributeMetadata> attributes,  String type, String componentName, String prefix) {
      var metrics = attributes.stream()
            .map(MBeanMetadata.AttributeMetadata::toMetricInfo)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());
      return registerMetrics(instance, jmxObjectName, metrics, type, componentName, prefix);
   }

   private Set<Object> registerMetrics(Object instance, String jmxObjectName, Collection<MetricInfo> metrics, String type, String componentName, String prefix) {
      if (jmxObjectName == null) {
         jmxObjectName = componentName;
      }
      if (jmxObjectName == null) {
         throw new IllegalArgumentException("No MBean name and no component name was specified.");
      }
      String metricPrefix = "";
      if (!"Cache".equals(jmxObjectName) && !"CacheManager".equals(jmxObjectName)) {
         if (prefix != null) {
            metricPrefix += NameUtils.decamelize(prefix) + '_';
         }
         if (type != null && !type.equals(jmxObjectName)) {
            metricPrefix += NameUtils.decamelize(type) + '_';
         }
         metricPrefix += NameUtils.decamelize(jmxObjectName) + '_';
      }
      if (globalConfig.metrics().legacy()) {
         return internalRegisterMetrics(instance, metrics, VENDOR_PREFIX + legacyNamePrefix + metricPrefix);
      } else {
         return internalRegisterMetrics(instance, metrics, INFINISPAN_PREFIX + metricPrefix);
      }
   }

   protected abstract Set<Object> internalRegisterMetrics(Object instance, Collection<MetricInfo> metrics, String metricPrefix);

   /**
    * Register metrics for a component that was manually registered later, after component registry startup. The metric
    * ids will be tracked and unregistration will be performed automatically on stop.
    */
   public void registerMetrics(Object instance, String type, String componentName) {
      if (metricsCollector == null) {
         throw new IllegalStateException("Metrics are not initialized.");
      }
      MBeanMetadata beanMetadata = basicComponentRegistry.getMBeanMetadata(instance.getClass().getName());
      if (beanMetadata == null) {
         throw new IllegalArgumentException("No MBean metadata available for " + instance.getClass().getName());
      }
      Set<Object> ids = registerAttributes(instance, beanMetadata.getJmxObjectName(), beanMetadata.getAttributes(), type, componentName, null);
      metricIds.addAll(ids);
   }

   /**
    * Register metrics for a component that was manually registered later, after component registry startup. The metric
    * ids will <b>NOT</b> be tracked and unregistration will <b>NOT</b> be performed automatically on stop.
    */
   public Set<Object> registerExternalMetrics(Object instance, String prefix) {
      if (metricsCollector == null) {
         throw new IllegalStateException("Metrics are not initialized.");
      }
      MBeanMetadata beanMetadata = basicComponentRegistry.getMBeanMetadata(instance.getClass().getName());
      if (beanMetadata == null) {
         throw new IllegalArgumentException("No MBean metadata available for " + instance.getClass().getName());
      }
      return registerAttributes(instance, beanMetadata.getJmxObjectName(), beanMetadata.getAttributes(), null, null, prefix);
   }

   public void unregisterMetrics(Set<Object> metricIds) {
      if (metricsCollector == null) {
         throw new IllegalStateException("Metrics are not initialized.");
      }
      try {
         for (Object metricId : metricIds) {
            metricsCollector.unregisterMetric(metricId);
         }
      } catch (Exception e) {
         throw new CacheException("A failure occurred while unregistering metrics.", e);
      }
   }
}
