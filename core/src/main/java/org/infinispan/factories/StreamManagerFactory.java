package org.infinispan.factories;

import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.stream.impl.ClusterStreamManager;
import org.infinispan.stream.impl.LocalStreamManager;
import org.infinispan.stream.impl.ClusterStreamManagerImpl;
import org.infinispan.stream.impl.LocalStreamManagerImpl;
import org.infinispan.stream.impl.PartitionAwareClusterStreamManager;

/**
 * Factory that allows creation of a {@link LocalStreamManager} or {@link ClusterStreamManager} based on the provided
 * configuration.
 *
 * @author wburns
 * @since 8.0
 */
@DefaultFactoryFor(classes = {LocalStreamManager.class, ClusterStreamManager.class})
public class StreamManagerFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
   @Override
   public <T> T construct(Class<T> componentType) {
      if (configuration.clustering().cacheMode().isDistributed()) {
         if (componentType.equals(LocalStreamManager.class)) {
            return componentType.cast(new LocalStreamManagerImpl<>());
         }
         if (componentType.equals(ClusterStreamManager.class)) {
            if (configuration.clustering().partitionHandling().enabled()) {
               return componentType.cast(new PartitionAwareClusterStreamManager<>());
            } else {
               return componentType.cast(new ClusterStreamManagerImpl<>());
            }
         }
      }
      return null;
   }
}
