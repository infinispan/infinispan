package org.infinispan.iteration.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterable;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.filter.CacheFilters;
import org.infinispan.filter.Converter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.filter.KeyValueFilterConverter;
import org.infinispan.util.concurrent.ConcurrentHashSet;

import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

/**
 * CloseableIterable that also tracks the streams it spawns so it can properly close them when the close method is
 * invoked.
 *
 * @author wburns
 * @since 8.0
 */
public class TrackingEntryIterableFromStream<K, V, C> implements CloseableIterable<CacheEntry<K, C>> {
   protected final KeyValueFilter<? super K, ? super V> filter;
   protected final Converter<? super K, ? super V, C> converter;
   protected final AdvancedCache<K, V> cache;

   protected final AtomicBoolean closed = new AtomicBoolean(false);
   protected final Set<Stream<?>> streams = new ConcurrentHashSet<>();

   public TrackingEntryIterableFromStream(KeyValueFilter<? super K, ? super V> filter,
           Converter<? super K, ? super V, C> converter, EnumSet<Flag> flags, Cache<K, V> cache) {
      if (cache == null) {
         throw new NullPointerException("Cache cannot be null!");
      }
      if (filter == null) {
         throw new NullPointerException("Filter cannot be null!");
      }
      this.filter = filter;
      this.converter = converter;
      AdvancedCache<K, V> advancedCache = cache.getAdvancedCache();
      if (flags != null) {
         this.cache = advancedCache.withFlags(flags.toArray(new Flag[flags.size()]));
      } else {
         this.cache = advancedCache;
      }
   }

   @Override
   public void close() {
      closed.set(true);
      for (Stream<?> stream : streams) {
         stream.close();
      }
   }

   private Stream<CacheEntry<K, C>> applyFilterConverter(Stream<CacheEntry<K, V>> stream) {
      Stream<CacheEntry<K, C>> resultingStream;
         if (filter instanceof KeyValueFilterConverter && (filter == converter || converter == null)) {
            // perform filtering and conversion in a single step
            resultingStream = CacheFilters.filterAndConvert(stream, (KeyValueFilterConverter) filter);
         } else {
            if (filter != null) {
               stream = stream.filter(CacheFilters.predicate(filter));
            }
            if (converter != null) {
               resultingStream = stream.map(CacheFilters.function(converter));
            } else {
               resultingStream = (Stream) stream;
            }
         }

      return resultingStream;
   }

   @Override
   public CloseableIterator<CacheEntry<K, C>> iterator() {
      if (closed.get()) {
         throw new IllegalStateException("Iterable has been closed - cannot be reused");
      }
      Stream<CacheEntry<K, V>> stream = cache.getAdvancedCache().cacheEntrySet().stream();
      // TODO: when an iterator is closed we should remove it from the set - this isn't critical yet though
      CloseableIterator<CacheEntry<K, C>> iterator = Closeables.iterator(applyFilterConverter(stream));
      streams.add(stream);
      // Note we have to check if we were closed afterwards just in case if a concurrent close occurred.
      if (closed.get()) {
         // Rely on fact that multiple closes don't have adverse effects
         iterator.close();
         throw new IllegalStateException("Iterable has been closed - cannot be reused");
      }
      return new RemovableEntryIterator(iterator, cache, true);
   }
}
