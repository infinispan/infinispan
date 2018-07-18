package org.infinispan.notifications.cachelistener.cluster;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.marshall.core.Ids;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This DistributedCallable is used to invoke a raised notification on the cluster listener that registered to listen
 * for this event.
 *
 * @author wburns
 * @since 7.0
 */
public class ClusterEventCallable<K, V> implements DistributedCallable<K, V, Void> {

   private static final Log log = LogFactory.getLog(ClusterEventCallable.class);
   private static final boolean trace = log.isTraceEnabled();

   private transient ClusterCacheNotifier clusterCacheNotifier;

   private final UUID identifier;
   private final Collection<? extends ClusterEvent<K, V>> events;

   public ClusterEventCallable(UUID identifier, ClusterEvent<K, V> event) {
      this(identifier, Collections.singleton(event));
   }

   public ClusterEventCallable(UUID identifier, Collection<? extends ClusterEvent<K, V>> events) {
      this.identifier = identifier;
      this.events = events;
   }

   @Override
   public Void call() throws Exception {
      if (trace) {
         log.tracef("Received cluster event(s) %s, notifying cluster listener with id %s", events, identifier);
      }
      clusterCacheNotifier.notifyClusterListeners(events, identifier);
      return null;
   }

   @Override
   public void setEnvironment(Cache<K, V> cache, Set<K> inputKeys) {
      this.clusterCacheNotifier = cache.getAdvancedCache().getComponentRegistry().getComponent(ClusterCacheNotifier.class);
      for (ClusterEvent event : events) {
         event.cache = cache;
      }
   }

   @Override
   public String toString() {
      final StringBuilder sb = new StringBuilder("ClusterEventCallable{");
      sb.append("identifier=").append(identifier);
      sb.append(", events=").append(events);
      sb.append('}');
      return sb.toString();
   }

   public static class Externalizer extends AbstractExternalizer<ClusterEventCallable> {
      @Override
      public Set<Class<? extends ClusterEventCallable>> getTypeClasses() {
         return Collections.singleton(ClusterEventCallable.class);
      }

      @Override
      public void writeObject(UserObjectOutput output, ClusterEventCallable object) throws IOException {
         output.writeObject(object.identifier);
         output.writeObject(object.events);
      }

      @Override
      public ClusterEventCallable readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
         return new ClusterEventCallable((UUID)input.readObject(), (Collection<? extends ClusterEvent>)input.readObject());
      }

      @Override
      public Integer getId() {
         return Ids.CLUSTER_EVENT_CALLABLE;
      }
   }
}
