package org.infinispan.factories;

import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.impl.DistributionManagerImpl;
import org.infinispan.factories.annotations.DefaultFactoryFor;

@DefaultFactoryFor(classes = DistributionManager.class)
public class DistributionManagerFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
   @Override
   @SuppressWarnings("unchecked")
   public Object construct(String componentName) {
      if (configuration.clustering().cacheMode().isClustered())
         return new DistributionManagerImpl();
      else
         return null;
   }
}
