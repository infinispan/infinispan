package org.infinispan.hotrod;

import org.infinispan.api.async.AsyncStreamingCache;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.hotrod.impl.cache.RemoteCache;

/**
 * @since 14.0
 **/
public class HotRodAsyncStreamingCache<K> implements AsyncStreamingCache<K> {
   private final HotRod hotrod;
   private final RemoteCache<K, ?> remoteCache;

   HotRodAsyncStreamingCache(HotRod hotrod, RemoteCache<K, ?> remoteCache) {
      this.hotrod = hotrod;
      this.remoteCache = remoteCache;
   }

   @Override
   public CacheEntrySubscriber get(K key, CacheOptions metadata) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CacheEntryPublisher put(K key, CacheWriteOptions metadata) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CacheEntryPublisher putIfAbsent(K key, CacheWriteOptions metadata) {
      throw new UnsupportedOperationException();
   }
}
