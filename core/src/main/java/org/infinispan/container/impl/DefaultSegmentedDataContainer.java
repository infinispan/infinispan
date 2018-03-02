package org.infinispan.container.impl;

import java.lang.invoke.MethodHandles;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.ObjIntConsumer;
import java.util.function.Predicate;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.ConcatIterator;
import org.infinispan.commons.util.FlattenSpliterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;

/**
 * DataContainer implementation that internally stores entries in an array of maps. This array is indexed by
 * the segment that the entries belong to. This provides for much better iteration of entries when a subset of
 * segments are required.
 * <p>
 * This implemenation doesn't support bounding or temporary entries (L1).
 * @author wburns
 * @since 9.3
 */
public class DefaultSegmentedDataContainer<K, V> extends AbstractInternalDataContainer<K, V> {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final boolean trace = log.isTraceEnabled();

   protected final AtomicReferenceArray<ConcurrentMap<K, InternalCacheEntry<K, V>>> maps;
   protected boolean shouldStopSegments;


   public DefaultSegmentedDataContainer(int numSegments) {
      maps = new AtomicReferenceArray<>(numSegments);
   }

   @Start
   public void start() {
      // Local (invalidation), replicated and scattered cache we just instantiate all the maps immediately
      // Scattered needs this for backups as they can be for any segment
      // Distributed needs them all only at beginning for preload of data - rehash event will remove others
      for (int i = 0; i < maps.length(); ++i) {
         maps.set(i, new ConcurrentHashMap<>());
      }
      // Distributed is the only mode that allows for dynamic addition/removal of maps as others own all segments
      // in some fashion
      shouldStopSegments = cache.getCacheConfiguration().clustering().cacheMode().isDistributed();
   }

   @Stop(priority = 999)
   public void stop() {
      for (int i = 0; i < maps.length(); ++i) {
         maps.set(0, null);
      }
   }

   @Override
   protected int getSegmentForKey(Object key) {
      return keyPartitioner.getSegment(key);
   }

   @Override
   protected ConcurrentMap<K, InternalCacheEntry<K, V>> getMapForSegment(int segment) {
      return maps.get(segment);
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iterator(IntSet segments) {
      return new EntryIterator(iteratorIncludingExpired(segments));
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iterator() {
      return new EntryIterator(iteratorIncludingExpired());
   }

   @Override
   public Spliterator<InternalCacheEntry<K, V>> spliterator(IntSet segments) {
      return filterExpiredEntries(spliteratorIncludingExpired(segments));
   }

   @Override
   public Spliterator<InternalCacheEntry<K, V>> spliterator() {
      return filterExpiredEntries(spliteratorIncludingExpired());
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iteratorIncludingExpired(IntSet segments) {
      // TODO: explore creating streaming approach to not create this list?
      List<Collection<InternalCacheEntry<K, V>>> valueIterables = new ArrayList<>(segments.size());
      segments.forEach((int s) -> {
         ConcurrentMap<K, InternalCacheEntry<K, V>> map = maps.get(s);
         if (map != null) {
            valueIterables.add(map.values());
         }
      });
      return new ConcatIterator<>(valueIterables);
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iteratorIncludingExpired() {
      List<Collection<InternalCacheEntry<K, V>>> valueIterables = new ArrayList<>(maps.length() + 1);
      for (int i = 0; i < maps.length(); ++i) {
         ConcurrentMap<K, InternalCacheEntry<K, V>> map = maps.get(i);
         if (map != null) {
            valueIterables.add(map.values());
         }
      }
      return new ConcatIterator<>(valueIterables);
   }

   @Override
   public Spliterator<InternalCacheEntry<K, V>> spliteratorIncludingExpired(IntSet segments) {
      // Copy the ints into an array to parallelize them
      int[] segmentArray = segments.toIntArray();

      return new FlattenSpliterator<>(i -> {
         ConcurrentMap<K, InternalCacheEntry<K, V>> map = maps.get(segmentArray[i]);
         if (map == null) {
            return Collections.emptyList();
         }
         return map.values();
      }, segmentArray.length, Spliterator.CONCURRENT | Spliterator.NONNULL | Spliterator.DISTINCT);
   }

   @Override
   public Spliterator<InternalCacheEntry<K, V>> spliteratorIncludingExpired() {
      return new FlattenSpliterator<>(i -> {
         ConcurrentMap<K, InternalCacheEntry<K, V>> map = maps.get(i);
         if (map == null) {
            return Collections.emptyList();
         }
         return map.values();
      }, maps.length(), Spliterator.CONCURRENT | Spliterator.NONNULL | Spliterator.DISTINCT);
   }

   @Override
   public int sizeIncludingExpired(IntSet segment) {
      int size = 0;
      for (PrimitiveIterator.OfInt iter = segment.iterator(); iter.hasNext(); ) {
         ConcurrentMap<K, InternalCacheEntry<K, V>> map = maps.get(iter.nextInt());
         size += map != null ? map.size() : 0;
         // Overflow
         if (size < 0) {
            return Integer.MAX_VALUE;
         }
      }
      return size;
   }

   @Override
   public int sizeIncludingExpired() {
      int size = 0;
      for (int i = 0; i < maps.length(); ++i) {
         ConcurrentMap<K, InternalCacheEntry<K, V>> map = maps.get(i);
         if (map != null) {
            size += map.size();
            // Overflow
            if (size < 0) {
               return Integer.MAX_VALUE;
            }
         }
      }
      return size;
   }

   @Override
   public void clear() {
      for (int i = 0; i < maps.length(); ++i) {
         ConcurrentMap<K, InternalCacheEntry<K, V>> map = maps.get(i);
         if (map != null) {
            map.clear();
         }
      }
   }

   @Override
   public void forEach(IntSet segments, Consumer<? super InternalCacheEntry<K, V>> action) {
      Predicate<InternalCacheEntry<K, V>> expiredPredicate = expiredIterationPredicate(timeService.wallClockTime());
      BiConsumer<? super K, ? super InternalCacheEntry<K, V>> biConsumer = (k, ice) -> {
         if (expiredPredicate.test(ice)) {
            action.accept(ice);
         }
      };
      segments.forEach((int s) -> {
         ConcurrentMap<K, InternalCacheEntry<K, V>> map = maps.get(s);
         if (map != null) {
            map.forEach(biConsumer);
         }
      });
   }

   @Override
   public void forEachIncludingExpired(ObjIntConsumer<? super InternalCacheEntry<K, V>> action) {
      for (int i = 0; i < maps.length(); ++i) {
         ConcurrentMap<K, InternalCacheEntry<K, V>> map = maps.get(i);
         if (map != null) {
            int segment = i;
            map.forEach((k, ice) -> action.accept(ice, segment));
         }
      }
   }

   @Override
   public void addSegments(IntSet segments) {
      if (shouldStopSegments) {
         if (trace) {
            log.tracef("Ensuring segments %s are started", segments);
         }
         // Without this we will get a boxing and unboxing from int to Integer and back to int
         segments.forEach((IntConsumer) this::startNewMap);
      }
   }

   @Override
   public void removeSegments(IntSet segments) {
      if (shouldStopSegments) {
         if (trace) {
            log.tracef("Removing segments: %s from container", segments);
         }
         segments.forEach((int s) -> {
            ConcurrentMap<K, InternalCacheEntry<K, V>> map = maps.getAndSet(s, null);
            if (map != null && !map.isEmpty()) {
               listeners.forEach(c -> c.accept(map.values()));
            }
         });
      }
   }

   @Override
   public Set<K> keySet() {
      // This automatically immutable
      return new AbstractSet<K>() {
         @Override
         public boolean contains(Object o) {
            return containsKey(o);
         }

         @Override
         public Iterator<K> iterator() {
            return new IteratorMapper<>(iteratorIncludingExpired(), Map.Entry::getKey);
         }

         @Override
         public int size() {
            return DefaultSegmentedDataContainer.this.size();
         }

         @Override
         public Spliterator<K> spliterator() {
            return Spliterators.spliterator(this, Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.CONCURRENT);
         }
      };
   }

   private void startNewMap(int segment) {
      if (maps.get(segment) == null) {
         // Just in case of concurrent starts - this shouldn't be possible
         maps.compareAndSet(segment, null, new ConcurrentHashMap<>());
      }
   }
}
