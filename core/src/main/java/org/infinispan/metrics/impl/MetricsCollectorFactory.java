package org.infinispan.metrics.impl;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.ComponentFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

/**
 * Produces instances of {@link MetricsCollector}. MetricsCollector is optional,
 * based on the presence of Micrometer in classpath and the enabling of metrics in config.
 *
 * @author anistor@redhat.com
 * @author fabiomassimo.ercoli@gmail.com
 * @since 10.1
 */
@DefaultFactoryFor(classes = MetricsCollector.class)
@Scope(Scopes.GLOBAL)
public final class MetricsCollectorFactory implements ComponentFactory, AutoInstantiableFactory {

   @Inject
   GlobalConfiguration globalConfig;

   @Override
   public Object construct(String componentName) {
      if (!globalConfig.metrics().enabled()) {
         return null;
      }

      return new MetricsCollector(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT));
   }
}
