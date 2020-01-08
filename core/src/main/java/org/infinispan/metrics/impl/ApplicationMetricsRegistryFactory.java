package org.infinispan.metrics.impl;

import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.ComponentFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Produces instances of ApplicationMetricsRegistry.
 *
 * @author anistor@redhat.com
 * @since 10.0
 */
@DefaultFactoryFor(classes = ApplicationMetricsRegistry.class)
@Scope(Scopes.GLOBAL)
public final class ApplicationMetricsRegistryFactory implements ComponentFactory, AutoInstantiableFactory {

   @Override
   public Object construct(String componentName) {
      try {
         return new ApplicationMetricsRegistry();
      } catch (NoClassDefFoundError e) {
         // missing dependency ?
         return null;
      }
   }
}
