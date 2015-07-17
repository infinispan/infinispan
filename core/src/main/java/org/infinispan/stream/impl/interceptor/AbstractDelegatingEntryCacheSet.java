package org.infinispan.stream.impl.interceptor;

import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.stream.impl.AbstractDelegatingCacheSet;
import org.infinispan.stream.impl.local.LocalEntryCacheStream;

import java.util.stream.StreamSupport;

/**
 * Abstract cache entry set that delegates to the underlying cache for stream usage
 * @param <K> key type of the cache
 * @param <V> value type of the cache
 */
public abstract class AbstractDelegatingEntryCacheSet<K, V> extends AbstractDelegatingCacheSet<CacheEntry<K, V>> {
   private final Cache<K, V> cache;
   private final CacheSet<CacheEntry<K, V>> set;

   protected AbstractDelegatingEntryCacheSet(Cache<K, V> cache, CacheSet<CacheEntry<K, V>> set) {
      this.cache = cache;
      this.set = set;
   }

   @Override
   protected final CacheSet<CacheEntry<K, V>> delegate() {
      return set;
   }

   @Override
   public CacheStream<CacheEntry<K, V>> stream() {
      DistributionManager dm = cache.getAdvancedCache().getDistributionManager();
      return new LocalEntryCacheStream<>(cache, false, dm != null ? dm.getConsistentHash() : null,
              () -> StreamSupport.stream(spliterator(), false), cache.getAdvancedCache().getComponentRegistry());
   }

   @Override
   public CacheStream<CacheEntry<K, V>> parallelStream() {
      DistributionManager dm = cache.getAdvancedCache().getDistributionManager();
      return new LocalEntryCacheStream<>(cache, true, dm != null ? dm.getConsistentHash() : null,
              () -> StreamSupport.stream(spliterator(), true), cache.getAdvancedCache().getComponentRegistry());
   }
}
