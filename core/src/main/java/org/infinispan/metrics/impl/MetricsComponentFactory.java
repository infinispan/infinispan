package org.infinispan.metrics.impl;

import static org.infinispan.util.logging.Log.CONTAINER;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.ComponentFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.jgroups.JGroupsMetricsManager;
import org.infinispan.remoting.transport.jgroups.JGroupsMetricsManagerImpl;
import org.infinispan.remoting.transport.jgroups.NoOpJGroupsMetricManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import net.jcip.annotations.GuardedBy;

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
         return createMetricRegistry();
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
         if (createMetricRegistry() == NoMetricRegistry.NO_OP_INSTANCE) {
            // if no registry available in the classpath, do not try to register/collect metrics in there.
            return NoOpJGroupsMetricManager.INSTANCE;
         } else {
            return new JGroupsMetricsManagerImpl(globalConfig.metrics().histograms());
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

   private synchronized MetricsRegistry createMetricRegistry() {
      if (registry != null) {
         return registry;
      }
      if (isMetricsDisabled()) {
         registry = NoMetricRegistry.NO_OP_INSTANCE;
         return registry;
      }
      try {
         registry = new MetricsRegistryImpl();
      } catch (Throwable t) {
         logMissingDependencies(t);
         registry = NoMetricRegistry.NO_OP_INSTANCE;
      }
      return registry;
   }
}
