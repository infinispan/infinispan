package org.infinispan.metrics.impl;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.ComponentFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Produces instances of {@link MetricsCollector}. MetricsCollector is optional,
 * based on the presence of Micrometer in classpath and the enabling of metrics in config.
 *
 * @author anistor@redhat.com
 * @author Fabio Massimo Ercoli
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
      if (!globalConfig.metrics().enabled()) {
         return null;
      }

      // try cautiously
      try {
         return new MetricsCollector();
      } catch (Throwable e) {
         // missing dependency
         log.debug("Micrometer metrics are not available because classpath dependencies are missing.", e);
         return null;
      }
   }
}
