package org.infinispan.metrics.impl;

import org.eclipse.microprofile.config.ConfigProvider;
import org.eclipse.microprofile.metrics.MetricRegistry;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.ComponentFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.smallrye.metrics.MetricRegistries;

/**
 * Produces instances of InfinispanMetricsRegistry. InfinispanMetricsRegistry is optional, based on the presence of
 * microprofile metrics API and the Smallrye implementation in classpath.
 *
 * @author anistor@redhat.com
 * @since 10.1.3
 */
@DefaultFactoryFor(classes = InfinispanMetricRegistry.class)
@Scope(Scopes.GLOBAL)
public final class InfinispanMetricRegistryFactory implements ComponentFactory, AutoInstantiableFactory {

   private static final Log log = LogFactory.getLog(InfinispanMetricRegistryFactory.class);

   @Inject
   GlobalConfiguration globalConfig;

   @Override
   public Object construct(String componentName) {
      if (globalConfig.metrics().enabled()) {
         // try cautiously
         try {
            // ensure microprofile config dependencies exist
            ConfigProvider.getConfig();

            // ensure microprofile metrics dependencies exist
            MetricRegistry registry = MetricRegistries.get(MetricRegistry.Type.VENDOR);

            return new InfinispanMetricRegistry(registry);
         } catch (Throwable e) {
            // missing dependency
            log.debug("Microprofile metrics are not available due to missing classpath dependencies.", e);
         }
      }
      return null;
   }
}
