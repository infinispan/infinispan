package org.infinispan.metrics.impl;

import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.ComponentFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * @author anistor@redhat.com
 * @since 10.0
 */
@DefaultFactoryFor(classes = ApplicationMetricsRegistry.class)
@Scope(Scopes.GLOBAL)
public final class ApplicationMetricsRegistryFactory implements ComponentFactory, AutoInstantiableFactory {

   @Override
   public Object construct(String componentName) {
      return new ApplicationMetricsRegistry();
   }
}
