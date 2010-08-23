package org.infinispan.factories;

import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.DistributionManagerImpl;
import org.infinispan.factories.annotations.DefaultFactoryFor;

@DefaultFactoryFor(classes = DistributionManager.class)
public class DistributionManagerFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType) {
      if (configuration.getCacheMode().isDistributed())
         return (T) new DistributionManagerImpl();
      else
         return null;
   }
}
