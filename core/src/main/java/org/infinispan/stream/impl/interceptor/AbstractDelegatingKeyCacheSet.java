package org.infinispan.stream.impl.interceptor;

import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.container.entries.CacheEntry;
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
   public CacheStream<K> stream() {
      return getStream(false);
   }

   @Override
   public CacheStream<K> parallelStream() {
      return getStream(true);
   }

   private CacheStream<K> getStream(boolean parallel) {
      DistributionManager dm = cache.getAdvancedCache().getDistributionManager();
      CloseableSpliterator<CacheEntry<K, V>> closeableSpliterator = cache.getAdvancedCache().cacheEntrySet().spliterator();
      // TODO: add custom local key cache stream that doesn't use entries - this way it doesn't need to use entry set
      CacheStream<K> stream = new LocalKeyCacheStream<>(cache, parallel, dm != null ? dm.getConsistentHash() : null,
              () -> StreamSupport.stream(closeableSpliterator, parallel), cache.getAdvancedCache().getComponentRegistry());
      // We rely on the fact that on close returns the same instance
      stream.onClose(() -> closeableSpliterator.close());
      return stream;
   }
}
