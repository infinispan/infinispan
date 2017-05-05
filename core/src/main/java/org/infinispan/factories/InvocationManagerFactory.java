package org.infinispan.factories;

import org.infinispan.commands.InvocationManager;
import org.infinispan.commands.impl.InvocationManagerImpl;
import org.infinispan.factories.annotations.DefaultFactoryFor;

@DefaultFactoryFor(classes = InvocationManager.class)
public class InvocationManagerFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
   @Override
   public <T> T construct(Class<T> componentType) {
      if (configuration.clustering().cacheMode().isClustered()) {
         return componentType.cast(new InvocationManagerImpl());
      } else {
         return null;
      }
   }
}
