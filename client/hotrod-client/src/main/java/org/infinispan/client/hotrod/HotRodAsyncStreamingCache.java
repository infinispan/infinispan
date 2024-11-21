package org.infinispan.client.hotrod;

import org.infinispan.api.async.AsyncStreamingCache;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;

final class HotRodAsyncStreamingCache<K, V> implements AsyncStreamingCache<K> {

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
