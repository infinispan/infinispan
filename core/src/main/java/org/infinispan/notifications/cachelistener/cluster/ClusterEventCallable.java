package org.infinispan.notifications.cachelistener.cluster;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.manager.EmbeddedCacheManager;
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
public class ClusterEventCallable<K, V> implements Function<EmbeddedCacheManager, Void> {

   private static final Log log = LogFactory.getLog(ClusterEventCallable.class);
   private static final boolean trace = log.isTraceEnabled();

   private transient ClusterCacheNotifier clusterCacheNotifier;

   private final String cacheName;
   private final UUID identifier;
   private final Collection<? extends ClusterEvent<K, V>> events;

   public ClusterEventCallable(String cacheName, UUID identifier, ClusterEvent<K, V> event) {
      this(cacheName, identifier, Collections.singleton(event));
   }

   public ClusterEventCallable(String cacheName, UUID identifier, Collection<? extends ClusterEvent<K, V>> events) {
      this.cacheName = cacheName;
      this.identifier = identifier;
      this.events = events;
   }

   @Override
   public Void apply(EmbeddedCacheManager embeddedCacheManager) {
      Cache<K, V> cache = embeddedCacheManager.getCache(cacheName);
      ClusterCacheNotifier<K, V> clusterCacheNotifier = cache.getAdvancedCache().getComponentRegistry().getComponent(ClusterCacheNotifier.class);
      for (ClusterEvent event : events) {
         event.cache = cache;
      }
      if (trace) {
         log.tracef("Received cluster event(s) %s, notifying cluster listener with id %s", events, identifier);
      }
      clusterCacheNotifier.notifyClusterListeners(events, identifier);
      return null;
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
      public void writeObject(ObjectOutput output, ClusterEventCallable object) throws IOException {
         output.writeObject(object.cacheName);
         output.writeObject(object.identifier);
         output.writeObject(object.events);
      }

      @Override
      public ClusterEventCallable readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new ClusterEventCallable((String) input.readObject(), (UUID)input.readObject(),
               (Collection<? extends ClusterEvent>)input.readObject());
      }

      @Override
      public Integer getId() {
         return Ids.CLUSTER_EVENT_CALLABLE;
      }
   }
}
