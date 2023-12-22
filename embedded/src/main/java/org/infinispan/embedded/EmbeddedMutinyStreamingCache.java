package org.infinispan.embedded;

import org.infinispan.AdvancedCache;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.api.mutiny.MutinyStreamingCache;

import io.smallrye.mutiny.subscription.BackPressureStrategy;

/**
 * @since 15.0
 */
public class EmbeddedMutinyStreamingCache<K> implements MutinyStreamingCache<K> {
   private final AdvancedCache<K, ?> cache;

   EmbeddedMutinyStreamingCache(AdvancedCache<K, ?> cache) {
      this.cache = cache;
   }

   @Override
   public CacheEntrySubscriber get(K key, BackPressureStrategy backPressureStrategy, CacheOptions options) {
      return null;
   }

   @Override
   public CacheEntryPublisher put(K key, CacheWriteOptions options, BackPressureStrategy backPressureStrategy) {
      return null;
   }

   @Override
   public CacheEntryPublisher putIfAbsent(K key, CacheWriteOptions options, BackPressureStrategy backPressureStrategy) {
      return null;
   }
}
