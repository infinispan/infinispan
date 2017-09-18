package org.infinispan.stream.impl.interceptor;

import java.util.stream.StreamSupport;

import org.infinispan.Cache;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.stream.impl.local.KeyStreamSupplier;
import org.infinispan.stream.impl.local.LocalCacheStream;
import org.infinispan.util.AbstractDelegatingCacheSet;

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

   protected CacheStream<K> getStream(boolean parallel) {
      DistributionManager dm = cache.getAdvancedCache().getDistributionManager();
      CloseableSpliterator<K> closeableSpliterator = spliterator();
      CacheStream<K> stream = new LocalCacheStream<>(new KeyStreamSupplier<>(cache, dm != null ? dm.getCacheTopology()::getSegment : null,
              () -> StreamSupport.stream(closeableSpliterator, false)), parallel, cache.getAdvancedCache().getComponentRegistry());
      // We rely on the fact that on close returns the same instance
      stream.onClose(closeableSpliterator::close);
      return stream;
   }
}
