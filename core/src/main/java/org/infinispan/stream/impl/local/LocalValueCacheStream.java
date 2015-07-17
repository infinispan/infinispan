package org.infinispan.stream.impl.local;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.ComponentRegistry;

import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Local cache stream that returns a stream of cache values
 * @param <K> key type of the cache entries
 * @param <V> value type of the cache entries and resulting stream
 */
public class LocalValueCacheStream<K, V> extends AbstractLocalCacheStream<V, K, V> {
   private final Cache<K, V> cache;

   /**
    * @param cache
    * @param parallel
    * @param supplier This must be a supplier that provides
    */
   public LocalValueCacheStream(Cache<K, V> cache, boolean parallel, ConsistentHash hash,
                                Supplier<Stream<CacheEntry<K, V>>> supplier, ComponentRegistry registry) {
      super(parallel, hash, supplier, registry);
      this.cache = cache;
   }

   @Override
   protected Stream<V> getStream() {
      Stream<CacheEntry<K, V>> stream;
      if (keysToFilter != null) {
         AdvancedCache<K, V> advancedCache = cache.getAdvancedCache();
         log.tracef("Applying key filtering %s", keysToFilter);
         stream = keysToFilter.stream().map(k -> advancedCache.getCacheEntry(k)).filter(e -> e != null);
      } else {
         stream = supplier.get();
      }
      if (segmentsToFilter != null && hash != null) {
         log.tracef("Applying segment filter %s", segmentsToFilter);
         stream = stream.filter(k -> segmentsToFilter.contains(hash.getSegment(k.getKey())));
      }
      return stream.map(e -> e.getValue());
   }
}
