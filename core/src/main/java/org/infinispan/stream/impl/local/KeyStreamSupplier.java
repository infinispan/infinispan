package org.infinispan.stream.impl.local;

import java.util.Set;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.context.Flag;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Stream supplier that is to be used when the underlying stream is composed by key instances.  This supplier will do
 * the proper filtering by assuming each element is the key itself.
 */
public class KeyStreamSupplier<K, V> implements AbstractLocalCacheStream.StreamSupplier<K, Stream<K>> {
   private static final Log log = LogFactory.getLog(KeyStreamSupplier.class);
   private static final boolean trace = log.isTraceEnabled();

   private final Cache<K, V> cache;
   private final ToIntFunction<Object> toIntFunction;
   private final Supplier<Stream<K>> supplier;

   public KeyStreamSupplier(Cache<K, V> cache, ToIntFunction<Object> toIntFunction, Supplier<Stream<K>> supplier) {
      this.cache = cache;
      this.toIntFunction = toIntFunction;
      this.supplier = supplier;
   }

   @Override
   public Stream<K> buildStream(IntSet segmentsToFilter, Set<?> keysToFilter, boolean parallel) {
      Stream<K> stream;
      // Make sure we aren't going remote to retrieve these
      AdvancedCache<K, V> advancedCache = AbstractDelegatingCache.unwrapCache(cache).getAdvancedCache()
            .withFlags(Flag.CACHE_MODE_LOCAL);
      if (keysToFilter != null) {
         if (trace) {
            log.tracef("Applying key filtering %s", keysToFilter);
         }
         // ignore tombstones and non existent keys
         stream = (Stream<K>) (parallel ? keysToFilter.parallelStream() : keysToFilter.stream())
               .filter(k -> advancedCache.get(k) != null);
      } else {
         stream = supplier.get();
         if (cache.getCacheConfiguration().clustering().cacheMode().isScattered()) {
            // Ignore tombstones
            stream = stream.filter(k -> advancedCache.get(k) != null);
         }
         if (parallel) {
            stream = stream.parallel();
         }
      }
      if (segmentsToFilter != null && toIntFunction != null) {
         if (trace) {
            log.tracef("Applying segment filter %s", segmentsToFilter);
         }
         stream = stream.filter(k -> segmentsToFilter.contains(toIntFunction.applyAsInt(k)));
      }
      return stream;
   }
}
