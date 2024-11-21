package org.infinispan.client.hotrod.multimap;

import java.util.Collection;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.multimap.RemoteMultimapCacheImpl;

/**
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
public class RemoteMultimapCacheManager<K, V> implements MultimapCacheManager<K, V> {

   private final RemoteCacheManager remoteCacheManager;

   public RemoteMultimapCacheManager(RemoteCacheManager remoteCacheManager) {
      this.remoteCacheManager = remoteCacheManager;
   }

   @Override
   public RemoteMultimapCache<K, V> get(String cacheName, boolean supportsDuplicates) {
      RemoteCache<K, Collection<V>> cache = remoteCacheManager.getCache(cacheName);
      RemoteMultimapCacheImpl<K, V> multimapCache = new RemoteMultimapCacheImpl<>(remoteCacheManager, cache, supportsDuplicates);
      multimapCache.init();
      return multimapCache;
   }
}
