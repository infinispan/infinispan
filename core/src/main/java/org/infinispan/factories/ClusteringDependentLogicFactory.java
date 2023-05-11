package org.infinispan.factories;

import static org.infinispan.util.logging.Log.CONTAINER;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;

@DefaultFactoryFor(classes = ClusteringDependentLogic.class)
public class ClusteringDependentLogicFactory extends AbstractNamedCacheComponentFactory implements
      AutoInstantiableFactory {
   @Override
   public Object construct(String componentName) {
      CacheMode cacheMode = configuration.clustering().cacheMode();
      ClusteringDependentLogic cdl;
      if (!cacheMode.isClustered()) {
         cdl = new ClusteringDependentLogic.LocalLogic();
      } else if (cacheMode.isInvalidation()) {
         cdl = new ClusteringDependentLogic.InvalidationLogic();
      } else if (cacheMode.isReplicated()) {
         cdl = new ClusteringDependentLogic.ReplicationLogic();
      } else if (cacheMode.isDistributed()){
         cdl = new ClusteringDependentLogic.DistributionLogic();
      } else {
         throw CONTAINER.factoryCannotConstructComponent(componentName);
      }
      return cdl;
   }
}
