package org.infinispan.container.impl;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.ObjIntConsumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.ConcatIterator;
import org.infinispan.commons.util.FlattenSpliterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Flowable;

/**
 * DataContainer implementation that internally stores entries in an array of maps. This array is indexed by
 * the segment that the entries belong to. This provides for much better iteration of entries when a subset of
 * segments are required.
 * <p>
 * This implementation doesn't support bounding or temporary entries (L1).
 * @author wburns
 * @since 9.3
 */
public class DefaultSegmentedDataContainer<K, V> extends AbstractInternalDataContainer<K, V> {

   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   protected final AtomicReferenceArray<PeekableTouchableMap<K, V>> maps;
   protected final Supplier<PeekableTouchableMap<K, V>> mapSupplier;
   protected boolean shouldStopSegments;

   public DefaultSegmentedDataContainer(Supplier<PeekableTouchableMap<K, V>> mapSupplier, int numSegments) {
      maps = new AtomicReferenceArray<>(numSegments);
      this.mapSupplier = Objects.requireNonNull(mapSupplier);
   }

   @Start
   public void start() {
      // Local (invalidation) and replicated we just instantiate all the maps immediately
      // Distributed needs them all only at beginning for preload of data - rehash event will remove others
      for (int i = 0; i < maps.length(); ++i) {
         startNewMap(i);
      }
      // Distributed is the only mode that allows for dynamic addition/removal of maps as others own all segments
      // in some fashion
      shouldStopSegments = configuration.clustering().cacheMode().isDistributed();
   }

   @Stop
   public void stop() {
      clear();
      for (int i = 0; i < maps.length(); ++i) {
         stopMap(i, false);
      }
   }

   @Override
   public int getSegmentForKey(Object key) {
      return keyPartitioner.getSegment(key);
   }

   @Override
   public PeekableTouchableMap<K, V> getMapForSegment(int segment) {
      return maps.get(segment);
   }

   @Override
   public Publisher<InternalCacheEntry<K, V>> publisher(int segment) {
      return Flowable.defer(() -> {
         long accessTime = timeService.wallClockTime();
         return innerPublisher(segment, accessTime);
      });
   }

   private Publisher<InternalCacheEntry<K, V>> innerPublisher(int segment, long accessTime) {
      ConcurrentMap<K, InternalCacheEntry<K, V>> mapForSegment = maps.get(segment);
      if (mapForSegment == null) {
         return Flowable.empty();
      }
      return Flowable.fromIterable(mapForSegment.values()).filter(e -> !e.isExpired(accessTime));
   }

   @Override
   public Publisher<InternalCacheEntry<K, V>> publisher(IntSet segments) {
      return Flowable.defer(() -> {
         long accessTime = timeService.wallClockTime();
         return Flowable.fromIterable(segments)
               .flatMap(segment -> innerPublisher(segment, accessTime));
      });
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
   public void addSegments(IntSet segments) {
      if (shouldStopSegments) {
         if (log.isTraceEnabled()) {
            log.tracef("Ensuring segments %s are started", segments);
         }
         // Without this we will get a boxing and unboxing from int to Integer and back to int
         segments.forEach((IntConsumer) this::startNewMap);
      }
   }

   @Override
   public void removeSegments(IntSet segments) {
      if (shouldStopSegments) {
         if (log.isTraceEnabled()) {
            log.tracef("Removing segments: %s from container", segments);
         }
         for (PrimitiveIterator.OfInt segmentIterator = segments.iterator(); segmentIterator.hasNext(); ) {
            int segment = segmentIterator.nextInt();
            stopMap(segment, true);
         }
      }
   }

   @Override
   public void forEachSegment(ObjIntConsumer<PeekableTouchableMap<K, V>> segmentMapConsumer) {
      for (int i = 0; i < maps.length(); ++i) {
         PeekableTouchableMap<K, V> map = maps.get(i);
         if (map != null) {
            segmentMapConsumer.accept(map, i);
         }
      }
   }

   private void startNewMap(int segment) {
      if (maps.get(segment) == null) {
         PeekableTouchableMap<K, V> newMap = mapSupplier.get();
         // Just in case of concurrent starts - this shouldn't be possible
         if (!maps.compareAndSet(segment, null, newMap) && newMap instanceof AutoCloseable) {
            try {
               ((AutoCloseable) newMap).close();
            } catch (Exception e) {
               throw new CacheException(e);
            }
         }
      }
   }

   private void stopMap(int segment, boolean notifyListener) {
      ConcurrentMap<K, InternalCacheEntry<K, V>> map = maps.getAndSet(segment, null);
      if (map != null) {
         if (notifyListener && !map.isEmpty()) {
            listeners.forEach(c -> c.accept(map.values()));
         }
         segmentRemoved(map);
         if (map instanceof AutoCloseable) {
            try {
               ((AutoCloseable) map).close();
            } catch (Exception e) {
               throw new CacheException(e);
            }
         }
      }
   }
}
