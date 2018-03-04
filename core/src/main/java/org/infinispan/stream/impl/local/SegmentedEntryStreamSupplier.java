package org.infinispan.stream.impl.local;

import java.lang.invoke.MethodHandles;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.container.SegmentedDataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * StreamSupplier that allows for creating streams where they utilize a {@link SegmentedDataContainer} so that
 * the stream only looks at entries in those given segments.
 * @author wburns
 * @since 9.3
 */
public class SegmentedEntryStreamSupplier<K, V> implements AbstractLocalCacheStream.StreamSupplier<CacheEntry<K, V>, Stream<CacheEntry<K, V>>> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final boolean trace = log.isTraceEnabled();

   private final Cache<K, V> cache;
   private final ToIntFunction<Object> toIntFunction;
   private final SegmentedDataContainer<K, V> segmentedDataContainer;

   public SegmentedEntryStreamSupplier(Cache<K, V> cache, ToIntFunction<Object> toIntFunction,
         SegmentedDataContainer<K, V> segmentedDataContainer) {
      this.cache = cache;
      this.toIntFunction = toIntFunction;
      this.segmentedDataContainer = segmentedDataContainer;
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
      } else {
         if (segmentsToFilter != null) {
            stream = StreamSupport.stream(cast(segmentedDataContainer.spliterator(segmentsToFilter)), parallel);
         } else {
            stream = StreamSupport.stream(cast(segmentedDataContainer.spliterator()), parallel);
         }
         if (cache.getCacheConfiguration().clustering().cacheMode().isScattered()) {
            stream = stream.filter(ce -> ce.getValue() != null);
         }
      }
      return stream;
   }

   private Spliterator<CacheEntry<K, V>> cast(Spliterator spliterator) {
      return (Spliterator<CacheEntry<K, V>>) spliterator;
   }
}
