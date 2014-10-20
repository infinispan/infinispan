package org.infinispan.factories;

import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.remoting.ReplicationQueue;
import org.infinispan.remoting.ReplicationQueueImpl;

/**
 * Factory for ReplicationQueue.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
@DefaultFactoryFor(classes = ReplicationQueue.class)
public class ReplicationQueueFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
   @Override
   @SuppressWarnings("unchecked")
   public <T> T construct(Class<T> componentType) {
      ClusteringConfiguration clustering = configuration.clustering();
      if ((!clustering.cacheMode().isSynchronous()) && clustering.async().useReplQueue()) {
         ReplicationQueue replQueue = clustering.async().replQueue();
         return replQueue != null ? componentType.cast(replQueue) : (T) new ReplicationQueueImpl();
      } else {
         return null;
      }
   }
}