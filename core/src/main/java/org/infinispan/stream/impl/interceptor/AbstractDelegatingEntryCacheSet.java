package org.infinispan.stream.impl.interceptor;

import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.commons.util.CloseableSpliterator;
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
      return getStream(false);
   }

   @Override
   public CacheStream<CacheEntry<K, V>> parallelStream() {
      return getStream(true);
   }

   private CacheStream<CacheEntry<K, V>> getStream(boolean parallel) {
      DistributionManager dm = cache.getAdvancedCache().getDistributionManager();
      CloseableSpliterator<CacheEntry<K, V>> closeableSpliterator = spliterator();
      CacheStream<CacheEntry<K, V>> stream = new LocalEntryCacheStream<>(cache, parallel, dm != null ?
              dm.getConsistentHash() : null, () -> StreamSupport.stream(closeableSpliterator, parallel),
              cache.getAdvancedCache().getComponentRegistry());
      // We rely on the fact that on close returns the same instance
      stream.onClose(() -> closeableSpliterator.close());
      return stream;
   }
}
