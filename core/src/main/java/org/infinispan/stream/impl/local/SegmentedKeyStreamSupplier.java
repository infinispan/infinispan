package org.infinispan.stream.impl.local;

import java.lang.invoke.MethodHandles;
import java.util.Set;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.container.SegmentedDataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * StreamSupplier that allows for creating streams where they utilize a {@link SegmentedDataContainer} so that
 * the stream only looks at keys in those given segments.
 * @author wburns
 * @since 9.3
 */
public class SegmentedKeyStreamSupplier<K, V> implements AbstractLocalCacheStream.StreamSupplier<K, Stream<K>> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final boolean trace = log.isTraceEnabled();

   private final Cache<K, V> cache;
   private final ToIntFunction<Object> toIntFunction;
   private final SegmentedDataContainer<K, V> segmentedDataContainer;

   public SegmentedKeyStreamSupplier(Cache<K, V> cache, ToIntFunction<Object> toIntFunction,
         SegmentedDataContainer<K, V> segmentedDataContainer) {
      this.cache = cache;
      this.toIntFunction = toIntFunction;
      this.segmentedDataContainer = segmentedDataContainer;
   }

   @Override
   public Stream<K> buildStream(IntSet segmentsToFilter, Set<?> keysToFilter, boolean parallel) {
      Stream<K> stream;
      if (keysToFilter != null) {
         if (trace) {
            log.tracef("Applying key filtering %s", keysToFilter);
         }
         // Make sure we aren't going remote to retrieve these
         AdvancedCache<K, V> advancedCache = AbstractDelegatingCache.unwrapCache(cache).getAdvancedCache()
               .withFlags(Flag.CACHE_MODE_LOCAL);
         stream = (Stream<K>) (parallel ? keysToFilter.parallelStream() : keysToFilter.stream())
               .filter(advancedCache::containsKey);
         if (segmentsToFilter != null && toIntFunction != null) {
            if (trace) {
               log.tracef("Applying segment filter %s", segmentsToFilter);
            }
            stream = stream.filter(k -> {
               int segment = toIntFunction.applyAsInt(k);
               boolean isPresent = segmentsToFilter.contains(segment);
               if (trace)
                  log.tracef("Is key %s present in segment %d? %b", k, segment, isPresent);
               return isPresent;
            });
         }
      } else {
         Stream<InternalCacheEntry<K, V>> entryStream;
         if (segmentsToFilter != null) {
            entryStream = StreamSupport.stream(segmentedDataContainer.spliterator(segmentsToFilter), parallel);
         } else {
            entryStream = StreamSupport.stream(segmentedDataContainer.spliterator(), parallel);
         }
         if (cache.getCacheConfiguration().clustering().cacheMode().isScattered()) {
            entryStream = entryStream.filter(ce -> ce.getValue() != null);
         }
         stream = entryStream.map(InternalCacheEntry::getKey);
      }
      return stream;
   }
}
