package org.infinispan.factories;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.reactive.publisher.impl.ClusterPublisherManager;
import org.infinispan.reactive.publisher.impl.ClusterPublisherManagerImpl;
import org.infinispan.reactive.publisher.impl.LocalPublisherManager;
import org.infinispan.reactive.publisher.impl.LocalPublisherManagerImpl;

/**
 * Factory that allows creation of a {@link LocalPublisherManager} or {@link ClusterPublisherManager} based on the provided
 * configuration.
 *
 * @author wburns
 * @since 10.0
 */
@DefaultFactoryFor(classes = {LocalPublisherManager.class, ClusterPublisherManager.class})
public class PublisherManagerFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
   @Override
   public Object construct(String componentName) {
      if (componentName.equals(LocalPublisherManager.class.getName())) {
         return new LocalPublisherManagerImpl<>();
      }
      CacheMode cacheMode = configuration.clustering().cacheMode();
      if (cacheMode.needsStateTransfer() && componentName.equals(ClusterPublisherManager.class.getName())) {
         return new ClusterPublisherManagerImpl<>();
      }
      return null;
   }
}
