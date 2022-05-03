package org.infinispan.hotrod;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.api.mutiny.MutinyStreamingCache;
import org.infinispan.hotrod.impl.cache.RemoteCache;

import io.smallrye.mutiny.subscription.BackPressureStrategy;

/**
 * @since 14.0
 **/
public class HotRodMutinyStreamingCache<K> implements MutinyStreamingCache<K> {
   private final HotRod hotrod;
   private final RemoteCache<K, ?> remoteCache;

   HotRodMutinyStreamingCache(HotRod hotrod, RemoteCache<K, ?> remoteCache) {
      this.hotrod = hotrod;
      this.remoteCache = remoteCache;
   }

   @Override
   public CacheEntrySubscriber get(K key, BackPressureStrategy backPressureStrategy, CacheOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CacheEntryPublisher put(K key, CacheWriteOptions options, BackPressureStrategy backPressureStrategy) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CacheEntryPublisher putIfAbsent(K key, CacheWriteOptions options, BackPressureStrategy backPressureStrategy) {
      throw new UnsupportedOperationException();
   }
}
