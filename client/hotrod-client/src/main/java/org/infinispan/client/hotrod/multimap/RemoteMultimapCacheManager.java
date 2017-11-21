package org.infinispan.client.hotrod.multimap;

import java.util.Collection;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.multimap.RemoteMultimapCacheImpl;

/**
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public class RemoteMultimapCacheManager<K, V> implements MultimapCacheManager {

   private final RemoteCacheManager remoteCacheManager;

   public RemoteMultimapCacheManager(RemoteCacheManager remoteCacheManager) {
      this.remoteCacheManager = remoteCacheManager;
   }

   @Override
   public RemoteMultimapCache<K, V> get(String cacheName) {
      RemoteCache<K, Collection<V>> cache = remoteCacheManager.getCache(cacheName);
      RemoteMultimapCacheImpl multimapCache = new RemoteMultimapCacheImpl<>(remoteCacheManager, cache);
      multimapCache.init();
      return multimapCache;
   }
}
