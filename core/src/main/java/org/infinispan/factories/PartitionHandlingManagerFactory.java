package org.infinispan.factories;


import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.partitionhandling.impl.AvailablePartitionHandlingManager;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.partitionhandling.impl.PartitionHandlingManagerImpl;
import org.infinispan.scattered.impl.ScatteredPartitionHandlingManagerImpl;

/**
 * @author Dan Berindei
 * @since 7.0
 */
@DefaultFactoryFor(classes = PartitionHandlingManager.class)
public class PartitionHandlingManagerFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

   @Override
   @SuppressWarnings("unchecked")
   public Object construct(String componentName) {
      if (configuration.clustering().partitionHandling().whenSplit() != PartitionHandling.ALLOW_READ_WRITES) {
         if (configuration.clustering().cacheMode().isDistributed() ||
               configuration.clustering().cacheMode().isReplicated()) {
            return new PartitionHandlingManagerImpl();
         } else if (configuration.clustering().cacheMode().isScattered()) {
            return new ScatteredPartitionHandlingManagerImpl();
         }
      }
      return AvailablePartitionHandlingManager.getInstance();
   }
}
