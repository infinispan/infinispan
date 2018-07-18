package org.infinispan.notifications.cachelistener.cluster;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.AbstractExternalizer;
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
public class MultiClusterEventCallable<K, V> implements DistributedCallable<K, V, Void> {

   private static final Log log = LogFactory.getLog(MultiClusterEventCallable.class);
   private static final boolean trace = log.isTraceEnabled();

   private transient ClusterCacheNotifier<K, V> clusterCacheNotifier;

   private final Map<UUID, Collection<ClusterEvent<K, V>>> multiEvents;

   public MultiClusterEventCallable(Map<UUID, Collection<ClusterEvent<K, V>>> events) {
      this.multiEvents = events;
   }

   @Override
   public Void call() throws Exception {
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

   @Override
   public void setEnvironment(Cache<K, V> cache, Set<K> inputKeys) {
      this.clusterCacheNotifier = cache.getAdvancedCache().getComponentRegistry().getComponent(ClusterCacheNotifier.class);
      for (Collection<? extends ClusterEvent<K, V>> events : multiEvents.values()) {
         for (ClusterEvent<K, V> event : events) {
            event.cache = cache;
         }
      }
   }

   public static class Externalizer extends AbstractExternalizer<MultiClusterEventCallable> {
      @Override
      public Set<Class<? extends MultiClusterEventCallable>> getTypeClasses() {
         return Collections.singleton(MultiClusterEventCallable.class);
      }

      @Override
      public void writeObject(UserObjectOutput output, MultiClusterEventCallable object) throws IOException {
         output.writeObject(object.multiEvents);
      }

      @Override
      public MultiClusterEventCallable readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new MultiClusterEventCallable((Map<UUID, Collection<? extends ClusterEvent>>)input.readObject());
      }

      @Override
      public Integer getId() {
         return Ids.MULTI_CLUSTER_EVENT_CALLABLE;
      }
   }
}
