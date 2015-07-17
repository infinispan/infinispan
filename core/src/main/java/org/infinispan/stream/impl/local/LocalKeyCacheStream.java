package org.infinispan.stream.impl.local;

import org.infinispan.Cache;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.ComponentRegistry;

import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Local cache stream that returns a stream of cache keys
 * @param <K> key type of the cache entries and resulting stream
 * @param <V> value type of the cache entries
 */
public class LocalKeyCacheStream<K, V> extends AbstractLocalCacheStream<K, K, V> {
   private final Cache<K, V> cache;

   /**
    * @param cache
    * @param parallel
    * @param supplier This must be a supplier that provides
    */
   public LocalKeyCacheStream(Cache<K, V> cache, boolean parallel, ConsistentHash hash,
                              Supplier<Stream<CacheEntry<K, V>>> supplier, ComponentRegistry registry) {
      super(parallel, hash, supplier, registry);
      this.cache = cache;
   }

   @Override
   protected Stream<K> getStream() {
      Stream<K> stream;
      if (keysToFilter != null) {
         log.tracef("Applying key filtering %s", keysToFilter);
         stream = (Stream<K>) keysToFilter.stream().filter(k -> cache.containsKey(k));
      } else {
         stream = supplier.get().map(e -> e.getKey());
      }
      if (segmentsToFilter != null && hash != null) {
         log.tracef("Applying segment filter %s", segmentsToFilter);
         stream = stream.filter(k -> segmentsToFilter.contains(hash.getSegment(k)));
      }
      return stream;
   }
}
