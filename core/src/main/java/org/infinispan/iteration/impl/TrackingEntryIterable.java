package org.infinispan.iteration.impl;

import org.infinispan.Cache;
import org.infinispan.commons.util.CloseableIterable;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.filter.Converter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.util.concurrent.ConcurrentHashSet;

import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * CloseableIterable that also tracks the iterators it spawns so it can properly close them when the close method is
 * invoked.
 *
 * @author wburns
 * @since 7.0
 */
public class TrackingEntryIterable<K, V, C> implements CloseableIterable<CacheEntry<K, C>> {
   protected final EntryRetriever<K, V> entryRetriever;
   protected final KeyValueFilter<? super K, ? super V> filter;
   protected final Converter<? super K, ? super V, ? extends C> converter;
   protected final EnumSet<Flag> flags;
   protected final Cache<K, V> cache;

   protected final AtomicBoolean closed = new AtomicBoolean(false);
   protected final Set<CloseableIterator<CacheEntry<K, C>>> iterators =
         new ConcurrentHashSet<>();

   public TrackingEntryIterable(EntryRetriever<K, V> retriever, KeyValueFilter<? super K, ? super V> filter,
                                 Converter<? super K, ? super V, ? extends C> converter, EnumSet<Flag> flags,
                                 Cache<K, V> cache) {
      if (retriever == null) {
         throw new NullPointerException("Retriever cannot be null!");
      }
      if (filter == null) {
         throw new NullPointerException("Filter cannot be null!");
      }
      this.entryRetriever = retriever;
      this.filter = filter;
      this.converter = converter;
      this.flags = flags;
      this.cache = cache;
   }

   @Override
   public void close() {
      closed.set(true);
      for (CloseableIterator<CacheEntry<K, C>> iterator : iterators) {
         iterator.close();
      }
   }

   @Override
   public CloseableIterator<CacheEntry<K, C>> iterator() {
      if (closed.get()) {
         throw new IllegalStateException("Iterable has been closed - cannot be reused");
      }
      // TODO: when an iterator is closed we should remove it from the set - this isn't critical yet though
      CloseableIterator<CacheEntry<K, C>> iterator = entryRetriever.retrieveEntries(filter, converter, flags, null);
      iterators.add(iterator);
      // Note we have to check if we were closed afterwards just in case if a concurrent close occurred.
      if (closed.get()) {
         // Rely on fact that multiple closes don't have adverse effects
         iterator.close();
         throw new IllegalStateException("Iterable has been closed - cannot be reused");
      }
      return new RemovableEntryIterator(iterator, cache, true);
   }
}
