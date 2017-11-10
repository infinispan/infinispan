package org.infinispan.notifications.cachelistener.cluster;

import org.infinispan.Cache;
import org.infinispan.factories.AbstractNamedCacheComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.notifications.cachelistener.cluster.impl.BatchingClusterEventManagerImpl;

/**
 * Constructs the data container
 *
 * @author William Burns
 * @since 7.1
 */
@DefaultFactoryFor(classes = ClusterEventManager.class)
public class ClusterEventManagerFactory extends AbstractNamedCacheComponentFactory implements
         AutoInstantiableFactory {
   @Inject public Cache<?, ?> cache;

   @Override
   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType) {
      return (T) new BatchingClusterEventManagerImpl(cache);
   }
}
