package org.infinispan.notifications.cachelistener.cluster;

import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.CacheNotifierImpl;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This DistributedCallable is used to remove registered {@link RemoteClusterListener} on each of the various nodes
 * when a cluster listener is unregistered from the cache.
 *
 * @author wburns
 * @since 7.0
 */
@ProtoTypeId(ProtoStreamTypeIds.CLUSTER_LISTENER_REMOVE_CALLABLE)
public class ClusterListenerRemoveCallable implements Function<EmbeddedCacheManager, Void> {
   private static final Log log = LogFactory.getLog(ClusterListenerRemoveCallable.class);

   @ProtoField(1)
   final String cacheName;

   @ProtoField(2)
   final UUID identifier;

   @ProtoFactory
   public ClusterListenerRemoveCallable(String cacheName, UUID identifier) {
      this.cacheName = cacheName;
      this.identifier = identifier;
   }

   @Override
   public Void apply(EmbeddedCacheManager embeddedCacheManager) {
      Cache<Object, Object> cache = embeddedCacheManager.getCache(cacheName);
      CacheNotifierImpl<?, ?> notifier = (CacheNotifierImpl<?, ?>) ComponentRegistry.componentOf(cache, CacheNotifier.class);
      // Remove the listener from the cache now
      Set<Object> listeners = notifier.getListeners();
      for (Object listener : listeners) {
         if (listener instanceof RemoteClusterListener) {
            RemoteClusterListener clusterListener = (RemoteClusterListener)listener;
            if (identifier.equals(clusterListener.getId())) {
               if (log.isTraceEnabled()) {
                  log.tracef("Removing local cluster listener due to parent cluster listener was removed : %s", identifier);
               }
               clusterListener.removeListener();
            }
         }
      }
      return null;
   }
}
