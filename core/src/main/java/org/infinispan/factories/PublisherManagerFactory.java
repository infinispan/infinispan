package org.infinispan.factories;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.impl.ComponentAlias;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.reactive.publisher.impl.ClusterPublisherManager;
import org.infinispan.reactive.publisher.impl.ClusterPublisherManagerImpl;
import org.infinispan.reactive.publisher.impl.LocalClusterPublisherManagerImpl;
import org.infinispan.reactive.publisher.impl.LocalPublisherManager;
import org.infinispan.reactive.publisher.impl.LocalPublisherManagerImpl;
import org.infinispan.reactive.publisher.impl.NonSegmentedLocalPublisherManagerImpl;
import org.infinispan.reactive.publisher.impl.PartitionAwareClusterPublisherManager;

/**
 * Factory that allows creation of a {@link LocalPublisherManager} or {@link ClusterPublisherManager} based on the provided
 * configuration.
 *
 * @author wburns
 * @since 10.0
 */
@DefaultFactoryFor(classes = {LocalPublisherManager.class, ClusterPublisherManager.class}, names = PublisherManagerFactory.LOCAL_CLUSTER_PUBLISHER)
public class PublisherManagerFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
   public static final String LOCAL_CLUSTER_PUBLISHER = "NoClusterPublisherManager";
   @Override
   public Object construct(String componentName) {
      if (componentName.equals(LOCAL_CLUSTER_PUBLISHER)) {
         return new LocalClusterPublisherManagerImpl<>();
      }
      if (componentName.equals(LocalPublisherManager.class.getName())) {
         if (configuration.persistence().usingStores() && !configuration.persistence().usingSegmentedStore()) {
            return new NonSegmentedLocalPublisherManagerImpl<>();
         }
         return new LocalPublisherManagerImpl<>();
      }
      CacheMode cacheMode = configuration.clustering().cacheMode();
      if (cacheMode.needsStateTransfer() && componentName.equals(ClusterPublisherManager.class.getName())) {
         if (configuration.clustering().partitionHandling().whenSplit() == PartitionHandling.DENY_READ_WRITES) {
            return new PartitionAwareClusterPublisherManager<>();
         }
         return new ClusterPublisherManagerImpl<>();
      }
      return ComponentAlias.of(LOCAL_CLUSTER_PUBLISHER);
   }
}
