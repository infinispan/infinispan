package org.infinispan.metrics.impl;

import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.ComponentFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Produces instances of InfinispanMetricsRegistry. InfinispanMetricsRegistry is optional.
 *
 * @author anistor@redhat.com
 * @since 10.1.3
 */
@DefaultFactoryFor(classes = InfinispanMetricsRegistry.class)
@Scope(Scopes.GLOBAL)
public final class InfinispanMetricsRegistryFactory implements ComponentFactory, AutoInstantiableFactory {

   private static final Log log = LogFactory.getLog(InfinispanMetricsRegistryFactory.class);

   @Override
   public Object construct(String componentName) {
      try {
         return new InfinispanMetricsRegistry();
      } catch (NoClassDefFoundError e) {
         // missing dependency
         log.debug("Microprofile metrics are not available due to missing classpath dependencies.");
         return null;
      }
   }
}
