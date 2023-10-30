package org.infinispan.telemetry.impl;

import static org.infinispan.util.logging.Log.CONTAINER;

import org.infinispan.factories.AbstractComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.telemetry.InfinispanTelemetry;

@Scope(Scopes.GLOBAL)
@DefaultFactoryFor(classes = InfinispanTelemetry.class)
public class InfinispanTelemetryFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   @Override
   public Object construct(String componentName) {
      // by default, it is disabled. Server module will create a new factory.
      if (componentName.equals(InfinispanTelemetry.class.getName())) {
         return new DisabledInfinispanTelemetry();
      }

      throw CONTAINER.factoryCannotConstructComponent(componentName);
   }
}
