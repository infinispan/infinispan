package org.infinispan.factories;

import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.impl.DistributionManagerImpl;
import org.infinispan.factories.annotations.DefaultFactoryFor;

@DefaultFactoryFor(classes = DistributionManager.class)
public class DistributionManagerFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
   @Override
   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType) {
      if (configuration.clustering().cacheMode().isDistributed()
         || configuration.clustering().cacheMode().isReplicated()
         || configuration.clustering().cacheMode().isScattered())
         return (T) new DistributionManagerImpl();
      else
         return null;
   }
}
