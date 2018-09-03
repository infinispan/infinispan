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
   public Object construct(String componentName) {
      CacheMode cacheMode = configuration.clustering().cacheMode();
      if (cacheMode.needsStateTransfer()) {
         if (componentName.equals(LocalStreamManager.class.getName())) {
            return new LocalStreamManagerImpl<>();
         }
         if (componentName.equals(ClusterStreamManager.class.getName())) {
            if (configuration.clustering().partitionHandling().whenSplit() != PartitionHandling.ALLOW_READ_WRITES) {
               return new PartitionAwareClusterStreamManager<>();
            } else {
               return new ClusterStreamManagerImpl<>();
            }
         }
      }
      return null;
   }
}
