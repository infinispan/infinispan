package org.infinispan.stream.impl.interceptor;

import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.stream.impl.AbstractDelegatingCacheSet;
import org.infinispan.stream.impl.local.LocalKeyCacheStream;

import java.util.stream.StreamSupport;

/**
 * Abstract cache key set that delegates to the underlying cache for stream usage
 * @param <K> key type of the cache
 * @param <V> value type of the cache
 */
public abstract class AbstractDelegatingKeyCacheSet<K, V> extends AbstractDelegatingCacheSet<K> {
   private final Cache<K, V> cache;
   private final CacheSet<K> set;

   protected AbstractDelegatingKeyCacheSet(Cache<K, V> cache, CacheSet<K> set) {
      this.cache = cache;
      this.set = set;
   }

   @Override
   protected final CacheSet<K> delegate() {
      return set;
   }

   @Override
   public final CacheStream<K> stream() {
      DistributionManager dm = cache.getAdvancedCache().getDistributionManager();
      // TODO: add custom local key cache stream that doesn't use entries - this way it doesn't need to use entry set
      return new LocalKeyCacheStream<>(cache, false, dm != null ? dm.getConsistentHash() : null,
              () -> StreamSupport.stream(cache.getAdvancedCache().cacheEntrySet().spliterator(), false),
              cache.getAdvancedCache().getComponentRegistry());
   }

   @Override
   public final CacheStream<K> parallelStream() {
      DistributionManager dm = cache.getAdvancedCache().getDistributionManager();
      // TODO: add custom local key cache stream that doesn't use entries - this way it doesn't need to use entry set
      return new LocalKeyCacheStream<>(cache, true, dm != null ? dm.getConsistentHash() : null,
              () -> StreamSupport.stream(cache.getAdvancedCache().cacheEntrySet().spliterator(), true),
              cache.getAdvancedCache().getComponentRegistry());
   }
}
