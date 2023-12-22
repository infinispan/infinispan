package org.infinispan.embedded;

import org.infinispan.AdvancedCache;
import org.infinispan.api.async.AsyncStreamingCache;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;

public class EmbeddedAsyncStreamingCache<K, V> implements AsyncStreamingCache<K> {
   private final AdvancedCache<K, V> cache;

   EmbeddedAsyncStreamingCache(AdvancedCache<K, V> cache) {
      this.cache = cache;
   }

   @Override
   public CacheEntrySubscriber get(K key, CacheOptions metadata) {
      return null;
   }

   @Override
   public CacheEntryPublisher put(K key, CacheWriteOptions metadata) {
      return null;
   }

   @Override
   public CacheEntryPublisher putIfAbsent(K key, CacheWriteOptions metadata) {
      return null;
   }
}
