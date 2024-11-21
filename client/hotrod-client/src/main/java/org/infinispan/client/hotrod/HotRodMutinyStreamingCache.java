package org.infinispan.client.hotrod;

import org.infinispan.api.Experimental;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.api.mutiny.MutinyStreamingCache;

import io.smallrye.mutiny.subscription.BackPressureStrategy;

@Experimental
final class HotRodMutinyStreamingCache<K> implements MutinyStreamingCache<K> {
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
