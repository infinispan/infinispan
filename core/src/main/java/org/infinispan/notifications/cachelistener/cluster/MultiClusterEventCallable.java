package org.infinispan.notifications.cachelistener.cluster;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
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
 * This Function is used to invoke a raised notification on the cluster listener that registered to listen
 * for this event.
 *
 * @author wburns
 * @since 7.0
 */
public class MultiClusterEventCallable<K, V> implements Function<EmbeddedCacheManager, Void> {

   private static final Log log = LogFactory.getLog(MultiClusterEventCallable.class);
   private static final boolean trace = log.isTraceEnabled();

   private final String cacheName;
   private final Map<UUID, Collection<ClusterEvent<K, V>>> multiEvents;

   public MultiClusterEventCallable(String cacheName, Map<UUID, Collection<ClusterEvent<K, V>>> events) {
      this.cacheName = cacheName;
      this.multiEvents = events;
   }

   @Override
   public Void apply(EmbeddedCacheManager embeddedCacheManager) {
      Cache<K, V> cache = embeddedCacheManager.getCache(cacheName);
      ClusterCacheNotifier<K, V> clusterCacheNotifier = cache.getAdvancedCache().getComponentRegistry().getComponent(ClusterCacheNotifier.class);
      for (Collection<? extends ClusterEvent<K, V>> events : multiEvents.values()) {
         for (ClusterEvent<K, V> event : events) {
            event.cache = cache;
         }
      }

      if (trace) {
         log.tracef("Received multiple cluster event(s) %s", multiEvents);
      }
      for (Entry<UUID, Collection<ClusterEvent<K, V>>> entry : multiEvents.entrySet()) {
         UUID identifier = entry.getKey();
         Collection<ClusterEvent<K, V>> events = entry.getValue();
         clusterCacheNotifier.notifyClusterListeners(events, identifier);
      }

      return null;
   }

   public static class Externalizer extends AbstractExternalizer<MultiClusterEventCallable> {
      @Override
      public Set<Class<? extends MultiClusterEventCallable>> getTypeClasses() {
         return Collections.singleton(MultiClusterEventCallable.class);
      }

      @Override
      public void writeObject(ObjectOutput output, MultiClusterEventCallable object) throws IOException {
         output.writeObject(object.cacheName);
         output.writeObject(object.multiEvents);
      }

      @Override
      public MultiClusterEventCallable readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new MultiClusterEventCallable((String) input.readObject(),
               (Map<UUID, Collection<? extends ClusterEvent>>)input.readObject());
      }

      @Override
      public Integer getId() {
         return Ids.MULTI_CLUSTER_EVENT_CALLABLE;
      }
   }
}
