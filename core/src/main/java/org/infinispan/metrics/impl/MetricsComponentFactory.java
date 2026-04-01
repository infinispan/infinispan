package org.infinispan.metrics.impl;

import static org.infinispan.commons.util.Util.loadClassStrict;
import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.Objects;
import java.util.Optional;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.ComponentFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metrics.config.MicrometerMeterRegistryConfiguration;
import org.infinispan.remoting.transport.jgroups.JGroupsMetricsManager;
import org.infinispan.remoting.transport.jgroups.JGroupsMetricsManagerImpl;
import org.infinispan.remoting.transport.jgroups.NoOpJGroupsMetricManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import com.google.errorprone.annotations.concurrent.GuardedBy;

/**
 * Produces instances of {@link MetricsCollector}. MetricsCollector is optional, based on the presence of Micrometer in
 * classpath and the enabling of metrics in config.
 *
 * @author anistor@redhat.com
 * @author Fabio Massimo Ercoli
 * @since 10.1
 */
@DefaultFactoryFor(classes = {
      MetricsCollector.class,
      MetricsRegistry.class,
      JGroupsMetricsManager.class
})
@Scope(Scopes.GLOBAL)
public final class MetricsComponentFactory implements ComponentFactory, AutoInstantiableFactory {

   private static final Log log = LogFactory.getLog(MetricsComponentFactory.class);

   @Inject
   GlobalConfiguration globalConfig;
   @GuardedBy("this")
   private MetricsRegistry registry;

   @Override
   public Object construct(String componentName) {
      if (componentName.equals(MetricsRegistry.class.getName())) {
         return createMetricRegistry(globalConfig.classLoader());
      } else if (componentName.equals(MetricsCollector.class.getName())) {
         if (isMetricsDisabled()) {
            return null;
         }
         // try cautiously
         try {
            return new MetricsCollector();
         } catch (Throwable t) {
            logMissingDependencies(t);
            return null;
         }
      } else if (componentName.equals(JGroupsMetricsManager.class.getName())) {
         if (createMetricRegistry(globalConfig.classLoader()) == NoMetricRegistry.NO_OP_INSTANCE) {
            // if no registry available in the classpath, do not try to register/collect metrics in there.
            return NoOpJGroupsMetricManager.INSTANCE;
         } else {
            return new JGroupsMetricsManagerImpl(globalConfig.metrics().histograms(), globalConfig.metrics().prefix());
         }
      }
      throw CONTAINER.factoryCannotConstructComponent(componentName);
   }

   private boolean isMetricsDisabled() {
      return !globalConfig.metrics().enabled();
   }

   private static void logMissingDependencies(Throwable t) {
      log.debug("Micrometer metrics are not available because classpath dependencies are missing.", t);
   }

   private synchronized MetricsRegistry createMetricRegistry(ClassLoader classLoader) {
      if (registry != null) {
         return registry;
      }
      if (isMetricsDisabled()) {
         registry = NoMetricRegistry.NO_OP_INSTANCE;
         return registry;
      }

      registry = tryLoadPrometheusRegistry(classLoader)
            .or(() -> tryLoadDeprecatedPrometheusRegistry(classLoader))
            .or(() -> tryLoadGenericMetricRegistry(globalConfig))
            .orElseGet(MetricsComponentFactory::fallbackMetricRegistry);
      return Objects.requireNonNull(registry);
   }

   private static Optional<MetricsRegistry> tryLoadPrometheusRegistry(ClassLoader classLoader) {
      var registryClass = "io.micrometer.prometheusmetrics.PrometheusMeterRegistry";
      try {
         loadClassStrict(registryClass, classLoader);
         return Optional.of(new PrometheusRegistry());
      } catch (ClassNotFoundException ignore) {
         logMissingMicrometerImpl(registryClass);
      }
      return Optional.empty();
   }

   private static Optional<MetricsRegistry> tryLoadDeprecatedPrometheusRegistry(ClassLoader classLoader) {
      var registryClass = "io.micrometer.prometheus.PrometheusMeterRegistry";
      try {
         loadClassStrict(registryClass, classLoader);
         return Optional.of(new PrometheusSimpleClientRegistry());
      } catch (ClassNotFoundException ignore) {
         logMissingMicrometerImpl(registryClass);
      }
      return Optional.empty();
   }

   private static Optional<MetricsRegistry> tryLoadGenericMetricRegistry(GlobalConfiguration configuration) {
      var registryClass = "io.micrometer.core.instrument.MeterRegistry";
      try {
         loadClassStrict(registryClass, configuration.classLoader());
         return Optional.ofNullable(configuration.module(MicrometerMeterRegistryConfiguration.class))
               .map(MicrometerMeterRegistryConfiguration::meterRegistry)
               .map(ignored -> new GenericMetricRegistry());
      } catch (ClassNotFoundException ignore) {
         logMissingMicrometerImpl(registryClass);
      }
      return Optional.empty();
   }

   private static MetricsRegistry fallbackMetricRegistry() {
      log.warnFallbackToNoOpMetrics(NoMetricRegistry.class.getSimpleName());
      return NoMetricRegistry.NO_OP_INSTANCE;
   }

   private static void logMissingMicrometerImpl(String clazz) {
      log.debugf("Micrometer implementation '%s' not available on classpath", clazz);
   }
}
