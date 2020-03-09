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
 * Produces instances of {@link MetricsCollector}. MetricsCollector is optional, based on the presence of the optional
 * microprofile metrics API and the Smallrye implementation in classpath and the enabling of metrics in config.
 *
 * @author anistor@redhat.com
 * @since 10.1
 */
@DefaultFactoryFor(classes = MetricsCollector.class)
@Scope(Scopes.GLOBAL)
public final class MetricsCollectorFactory implements ComponentFactory, AutoInstantiableFactory {

   private static final Log log = LogFactory.getLog(MetricsCollectorFactory.class);

   @Inject
   GlobalConfiguration globalConfig;

   @Override
   public Object construct(String componentName) {
      if (globalConfig.metrics().enabled()) {
         // try cautiously
         try {
            // ensure microprofile config dependencies exist and static initialization either succeeds or fails early
            ConfigProvider.getConfig();

            // ensure microprofile metrics dependencies exist
            MetricRegistry registry = MetricRegistries.get(MetricRegistry.Type.VENDOR);

            return new MetricsCollector(registry);
         } catch (Throwable e) {
            // missing dependency
            log.debug("Microprofile metrics are not available due to missing classpath dependencies.", e);
         }
      }
      return null;
   }
}
