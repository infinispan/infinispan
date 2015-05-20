package org.infinispan.stream.impl.local;

import org.infinispan.Cache;
import org.infinispan.CacheCollection;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.commands.read.AbstractCloseableIteratorCollection;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorMapper;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.stream.StreamMarshalling;
import org.infinispan.stream.impl.local.LocalEntryCacheStream;
import org.infinispan.stream.impl.local.LocalValueCacheStream;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * CacheCollection that can be used for the values method of a cache.  Backs all the calls to the cacheSet version
 * allowing for key filtering still to be applied.
 * @param <K> key type of the cache
 * @param <V> value type of the cache
 */
public class ValueCacheCollection<K, V> extends AbstractCloseableIteratorCollection<V, K, V>
        implements CacheCollection<V> {
   private final CacheSet<CacheEntry<K, V>> cacheSet;

   public ValueCacheCollection(Cache<K, V> cache, CacheSet<CacheEntry<K, V>> cacheSet) {
      super(cache);
      this.cacheSet = cacheSet;
   }

   @Override
   public CloseableIterator<V> iterator() {
      return new CloseableIteratorMapper<>(cacheSet.iterator(), CacheEntry::getValue);
   }

   @Override
   public CloseableSpliterator<V> spliterator() {
      return Closeables.spliterator(iterator(), cache.getAdvancedCache().getDataContainer().size(),
              Spliterator.CONCURRENT | Spliterator.NONNULL);
   }

   @Override
   public boolean contains(Object o) {
      // We don't support null values
      if (o == null) {
         throw new NullPointerException();
      }
      try (CloseableIterator<V> it = iterator()) {
         while (it.hasNext())
            if (o.equals(it.next()))
               return true;
         return false;
      }
   }

   @Override
   public boolean containsAll(Collection<?> c) {
      // The AbstractCollection implementation calls contains for each element.  Instead we want to call the iterator
      // only once so we have a special implementation.
      if (c.size() > 0) {
         Set<?> set = new HashSet<>(c);
         try (CloseableIterator<V> it = iterator()) {
            while (!set.isEmpty() && it.hasNext()) {
               set.remove(it.next());
            }
         }
         return set.isEmpty();
      }
      return true;
   }

   @Override
   public boolean remove(Object o) {
      try (CloseableIterator<V> it = iterator()) {
         while (it.hasNext()) {
            if (o.equals(it.next())) {
               it.remove();
               return true;
            }
         }
         return false;
      }
   }

   @Override
   public CacheStream<V> stream() {
      Stream<CacheEntry<K, V>> stream = cacheSet.stream();
      if (stream instanceof LocalEntryCacheStream) {
         return ((LocalEntryCacheStream) stream).toLocalValueCacheStream();
      }
      return (CacheStream<V>) stream.map(StreamMarshalling.entryToValueFunction());
   }

   @Override
   public CacheStream<V> parallelStream() {
      Stream<CacheEntry<K, V>> stream = cacheSet.parallelStream();
      if (stream instanceof LocalEntryCacheStream) {
         return ((LocalEntryCacheStream) stream).toLocalValueCacheStream();
      }
      return (CacheStream<V>) cacheSet.parallelStream().map(StreamMarshalling.entryToValueFunction());
   }
}
