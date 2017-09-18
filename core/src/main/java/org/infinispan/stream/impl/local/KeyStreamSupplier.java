package org.infinispan.stream.impl.local;

import java.util.Set;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.cache.impl.AbstractDelegatingCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.RemovableCloseableIterator;
import org.infinispan.commons.util.SmallIntSet;
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
   public Stream<K> buildStream(Set<Integer> segmentsToFilter, Set<?> keysToFilter) {
      Stream<K> stream;
      if (keysToFilter != null) {
         if (trace) {
            log.tracef("Applying key filtering %s", keysToFilter);
         }
         // Make sure we aren't going remote to retrieve these
         AdvancedCache<K, V> advancedCache = AbstractDelegatingCache.unwrapCache(cache).getAdvancedCache()
               .withFlags(Flag.CACHE_MODE_LOCAL);
         stream = (Stream<K>) keysToFilter.stream().filter(advancedCache::containsKey);
      } else {
         stream = supplier.get();
      }
      if (segmentsToFilter != null && toIntFunction != null) {
         if (trace) {
            log.tracef("Applying segment filter %s", segmentsToFilter);
         }
         IntSet intSet = SmallIntSet.from(segmentsToFilter);
         stream = stream.filter(k -> intSet.contains(toIntFunction.applyAsInt(k)));
      }
      return stream;
   }

   @Override
   public CloseableIterator<K> removableIterator(CloseableIterator<K> realIterator) {
      return new RemovableCloseableIterator<>(realIterator, cache::remove);
   }
}
