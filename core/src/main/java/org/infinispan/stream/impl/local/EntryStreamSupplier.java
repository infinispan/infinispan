package org.infinispan.stream.impl.local;

import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Stream supplier that is to be used when the underlying stream is composed by {@link CacheEntry} instances.  This
 * supplier will do the proper filtering by key based on the CacheEntry key.
 */
public class EntryStreamSupplier<K, V> implements AbstractLocalCacheStream.StreamSupplier<CacheEntry<K, V>, Stream<CacheEntry<K, V>>> {
   private static final Log log = LogFactory.getLog(EntryStreamSupplier.class);
   private static final boolean trace = log.isTraceEnabled();

   private final Cache<K, V> cache;
   private final ToIntFunction<Object> toIntFunction;
   private final Supplier<Stream<CacheEntry<K, V>>> supplier;

   public EntryStreamSupplier(Cache<K, V> cache, ToIntFunction<Object> toIntFunction,
         Supplier<Stream<CacheEntry<K, V>>> supplier) {
      this.cache = cache;
      this.toIntFunction = toIntFunction;
      this.supplier = supplier;
   }

   @Override
   public Stream<CacheEntry<K, V>> buildStream(IntSet segmentsToFilter, Set<?> keysToFilter, boolean parallel) {
      Stream<CacheEntry<K, V>> stream;
      if (keysToFilter != null) {
         if (trace) {
            log.tracef("Applying key filtering %s", keysToFilter);
         }
         // Make sure we aren't going remote to retrieve these
         AdvancedCache<K, V> advancedCache = AbstractDelegatingCache.unwrapCache(cache).getAdvancedCache()
               .withFlags(Flag.CACHE_MODE_LOCAL);
         Stream<?> keyStream = parallel ? keysToFilter.parallelStream() : keysToFilter.stream();
         stream = keyStream
               .map(advancedCache::getCacheEntry)
               .filter(Objects::nonNull);
      } else {
         stream = supplier.get();
         if (cache.getCacheConfiguration().clustering().cacheMode().isScattered()) {
            // Ignore tombstones
            stream = stream.filter(e -> e.getValue() != null);
         }
         if (parallel) {
            stream = stream.parallel();
         }
      }
      if (segmentsToFilter != null && toIntFunction != null) {
         if (trace) {
            log.tracef("Applying segment filter %s", segmentsToFilter);
         }
         stream = stream.filter(k -> {
            K key = k.getKey();
            int segment = toIntFunction.applyAsInt(key);
            boolean isPresent = segmentsToFilter.contains(segment);
            if (trace)
               log.tracef("Is key %s present in segment %d? %b", key, segment, isPresent);
            return isPresent;
         });
      }
      return stream;
   }
}
