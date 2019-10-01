package org.infinispan.factories;

import static org.infinispan.util.logging.Log.CONTAINER;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.MemoryConfiguration;
import org.infinispan.configuration.cache.PersistenceConfiguration;
import org.infinispan.interceptors.locking.OrderedClusteringDependentLogic;
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
      } else if (cacheMode.isScattered()) {
         cdl = new ClusteringDependentLogic.ScatteredLogic();
      } else {
         throw CONTAINER.factoryCannotConstructComponent(componentName);
      }
      MemoryConfiguration memoryConfiguration = configuration.memory();
      PersistenceConfiguration persistenceConfiguration = configuration.persistence();
      boolean usingStores = persistenceConfiguration.usingStores();
      // If eviction is enabled or stores we have to make sure to order writes with other concurrent operations
      // Eviction requires it to perform eviction notifications in a non blocking fashion
      // Stores require writing entries to data container after loading - in an atomic fashion
      if (memoryConfiguration.isEvictionEnabled() || usingStores) {
         // Passivation also has some additional things required when doing writes
         boolean passivation = usingStores && persistenceConfiguration.passivation();
         return new OrderedClusteringDependentLogic(cdl, passivation);
      }
      return cdl;
   }
}
