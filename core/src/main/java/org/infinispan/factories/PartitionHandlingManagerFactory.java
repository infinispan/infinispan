package org.infinispan.factories;


import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.partionhandling.impl.PartitionHandlingManager;
import org.infinispan.partionhandling.impl.PartitionHandlingManagerImpl;

/**
 * @author Dan Berindei
 * @since 7.0
 */
@DefaultFactoryFor(classes = PartitionHandlingManager.class)
public class PartitionHandlingManagerFactory extends AbstractNamedCacheComponentFactory implements
      AutoInstantiableFactory {
   @Override
   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType) {
      if (configuration.clustering().partitionHandling().enabled()) {
         if (configuration.clustering().cacheMode().isDistributed() ||
               configuration.clustering().cacheMode().isReplicated()) {
            return (T) new PartitionHandlingManagerImpl();
         }
      }
      return null;
   }
}
