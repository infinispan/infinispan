package org.infinispan.factories;

import org.infinispan.distribution.L1Manager;
import org.infinispan.distribution.L1ManagerImpl;
import org.infinispan.factories.annotations.DefaultFactoryFor;

@DefaultFactoryFor(classes = L1Manager.class)
public class L1ManagerFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType) {
      if (configuration.isL1CacheEnabled())
         return (T) new L1ManagerImpl();
      else
         return null;
   }
}
