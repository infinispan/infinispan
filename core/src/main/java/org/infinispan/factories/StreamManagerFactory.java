package org.infinispan.factories;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.stream.impl.ClusterStreamManager;
import org.infinispan.stream.impl.ClusterStreamManagerImpl;
import org.infinispan.stream.impl.LocalStreamManager;
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
      CacheMode cacheMode = configuration.clustering().cacheMode();
      if (cacheMode.isDistributed() || cacheMode.isReplicated()) {
         if (componentType.equals(LocalStreamManager.class)) {
            return componentType.cast(new LocalStreamManagerImpl<>());
         }
         if (componentType.equals(ClusterStreamManager.class)) {
            if (configuration.clustering().partitionHandling().whenSplit() != PartitionHandling.ALLOW_READ_WRITES) {
               return componentType.cast(new PartitionAwareClusterStreamManager<>());
            } else {
               return componentType.cast(new ClusterStreamManagerImpl<>());
            }
         }
      }
      return null;
   }
}
