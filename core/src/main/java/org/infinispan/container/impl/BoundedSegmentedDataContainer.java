package org.infinispan.container.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;

import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.ConcatIterator;
import org.infinispan.commons.util.EntrySizeCalculator;
import org.infinispan.commons.util.FlattenSpliterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.CacheEntrySizeCalculator;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.PrimitiveEntrySizeCalculator;
import org.infinispan.marshall.core.WrappedByteArraySizeCalculator;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Policy;

/**
 * Bounded implementation of segmented data container. Bulk operations (iterator|spliterator) that are given segments
 * use the segments maps directly to only read the given segments (non segment based just read from bounded container).
 * <p>
 * Note this implementation supports both temporary non owned segments and not (L1). This map only utilizes heap based
 * (ie. ConcurrentHashMap) maps internally
 *
 * @author wburns
 * @since 9.3
 */
public class BoundedSegmentedDataContainer<K, V> extends DefaultSegmentedDataContainer<K, V> {
   protected final Cache<K, InternalCacheEntry<K, V>> evictionCache;
   protected final PeekableTouchableMap<K, V> entries;

   public BoundedSegmentedDataContainer(int numSegments, long thresholdSize, boolean memoryBased) {
      super(PeekableTouchableContainerMap::new, numSegments);

      Caffeine<K, InternalCacheEntry<K, V>> caffeine = caffeineBuilder();

      if (memoryBased) {
         CacheEntrySizeCalculator<K, V> calc = new CacheEntrySizeCalculator<>(new WrappedByteArraySizeCalculator<>(
               new PrimitiveEntrySizeCalculator()));
         caffeine.weigher((k, v) -> (int) calc.calculateSize(k, v)).maximumWeight(thresholdSize);
      } else {
         caffeine.maximumSize(thresholdSize);
      }
      DefaultEvictionListener evictionListener = new DefaultEvictionListener() {
         @Override
         void onEntryChosenForEviction(K key, InternalCacheEntry<K, V> value) {
            super.onEntryChosenForEviction(key, value);
            computeEntryRemoved(key, value);
         }
      };
      evictionCache = applyListener(caffeine, evictionListener).build();
      entries = new PeekableTouchableCaffeineMap<>(evictionCache);
   }

   public BoundedSegmentedDataContainer(int numSegments, long thresholdSize,
         EntrySizeCalculator<? super K, ? super InternalCacheEntry<K, V>> sizeCalculator) {
      super(PeekableTouchableContainerMap::new, numSegments);
      DefaultEvictionListener evictionListener = new DefaultEvictionListener();

      evictionCache = applyListener(Caffeine.newBuilder()
            .weigher((K k, InternalCacheEntry<K, V> v) -> (int) sizeCalculator.calculateSize(k, v))
            .maximumWeight(thresholdSize), evictionListener)
            .build();

      entries = new PeekableTouchableCaffeineMap<>(evictionCache);
   }

   @Override
   protected void computeEntryWritten(K key, InternalCacheEntry<K, V> value) {
      computeEntryWritten(getSegmentForKey(key), key, value);
   }

   protected void computeEntryWritten(int segment, K key, InternalCacheEntry<K, V> value) {
      ConcurrentMap<K, InternalCacheEntry<K, V>> map = BoundedSegmentedDataContainer.super.getMapForSegment(segment);
      if (map != null) {
         map.put(key, value);
      }
   }

   @Override
   protected void computeEntryRemoved(K key, InternalCacheEntry<K, V> value) {
      computeEntryRemoved(getSegmentForKey(key), key, value);
   }

   protected void computeEntryRemoved(int segment, K key, InternalCacheEntry<K, V> value) {
      ConcurrentMap<K, InternalCacheEntry<K, V>> map = BoundedSegmentedDataContainer.super.getMapForSegment(segment);
      if (map != null) {
         map.remove(key, value);
      }
   }

   @Override
   protected void putEntryInMap(PeekableTouchableMap<K, V> map, int segment, K key, InternalCacheEntry<K, V> ice) {
      map.compute(key, (k, __) -> {
         computeEntryWritten(segment, k, ice);
         return ice;
      });
   }

   @Override
   protected InternalCacheEntry<K, V> removeEntryInMap(PeekableTouchableMap<K, V> map, int segment, Object key) {
      ByRef<InternalCacheEntry<K, V>> ref = new ByRef<>(null);
      map.computeIfPresent((K) key, (k, prev) -> {
         computeEntryRemoved(segment, k, prev);
         ref.set(prev);
         return null;
      });
      return ref.get();
   }

   @Override
   public PeekableTouchableMap<K, V> getMapForSegment(int segment) {
      // All writes and other ops go directly to the caffeine cache
      return entries;
   }

   @Override
   public InternalCacheEntry<K, V> peek(Object k) {
      return peek(-1, k);
   }

   @Override
   public void clear() {
      entries.clear();
      for (int i = 0; i < maps.length(); ++i) {
         clearMapIfPresent(i);
      }
   }

   @Override
   public void clear(IntSet segments) {
      clear(segments, false);
      segments.forEach((IntConsumer) this::clearMapIfPresent);
   }

   private void clearMapIfPresent(int segment) {
      ConcurrentMap<?, ?> map = maps.get(segment);
      if (map != null) {
         map.clear();
      }
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iteratorIncludingExpired() {
      return entries.values().iterator();
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iteratorIncludingExpired(IntSet segments) {
      // We could explore a streaming approach here to not have to allocate an additional ArrayList
      List<Collection<InternalCacheEntry<K, V>>> valueIterables = new ArrayList<>(segments.size() + 1);
      PrimitiveIterator.OfInt iter = segments.iterator();
      boolean includeOthers = false;
      while (iter.hasNext()) {
         int segment = iter.nextInt();
         ConcurrentMap<K, InternalCacheEntry<K, V>> map = maps.get(segment);
         if (map != null) {
            valueIterables.add(map.values());
         } else {
            includeOthers = true;
         }
      }
      if (includeOthers) {
         valueIterables.add(entries.values().stream()
               .filter(e -> segments.contains(getSegmentForKey(e.getKey())))
               .collect(Collectors.toSet()));
      }
      return new ConcatIterator<>(valueIterables);
   }

   @Override
   public Spliterator<InternalCacheEntry<K, V>> spliteratorIncludingExpired() {
      return entries.values().spliterator();
   }

   @Override
   public Spliterator<InternalCacheEntry<K, V>> spliteratorIncludingExpired(IntSet segments) {
      // Copy the ints into an array to parallelize them
      int[] segmentArray = segments.toIntArray();
      AtomicBoolean usedOthers = new AtomicBoolean(false);

      return new FlattenSpliterator<>(i -> {
         ConcurrentMap<K, InternalCacheEntry<K, V>> map = maps.get(segmentArray[i]);
         if (map == null) {
            if (!usedOthers.getAndSet(true)) {
               return entries.values().stream()
                     .filter(e -> segments.contains(getSegmentForKey(e.getKey())))
                     .collect(Collectors.toSet());
            }
            return Collections.emptyList();
         }
         return map.values();
      }, segmentArray.length, Spliterator.CONCURRENT | Spliterator.NONNULL | Spliterator.DISTINCT);
   }

   @Override
   public int sizeIncludingExpired() {
      return entries.size();
   }

   /**
    * Clears entries out of caffeine map by invoking remove on iterator. This can either keep all keys that match the
    * provided segments when keepSegments is <code>true</code> or it will remove only the provided segments when
    * keepSegments is <code>false</code>.
    * @param segments the segments to either remove or keep
    * @param keepSegments whether segments are kept or removed
    */
   private void clear(IntSet segments, boolean keepSegments) {
      for (Iterator<K> keyIterator = entries.keySet().iterator(); keyIterator.hasNext(); ) {
         K key = keyIterator.next();
         int keySegment = getSegmentForKey(key);
         if (keepSegments != segments.contains(keySegment)) {
            keyIterator.remove();
         }
      }
   }

   @Override
   public void removeSegments(IntSet segments) {
      // Call super remove segments so the maps are removed more efficiently
      super.removeSegments(segments);
      // Finally remove the entries from bounded cache
      clear(segments, false);
   }

   private Policy.Eviction<K, InternalCacheEntry<K, V>> eviction() {
      if (evictionCache != null) {
         Optional<Policy.Eviction<K, InternalCacheEntry<K, V>>> eviction = evictionCache.policy().eviction();
         if (eviction.isPresent()) {
            return eviction.get();
         }
      }
      throw new UnsupportedOperationException();
   }

   @Override
   public long capacity() {
      Policy.Eviction<K, InternalCacheEntry<K, V>> evict = eviction();
      return evict.getMaximum();
   }

   @Override
   public void resize(long newSize) {
      Policy.Eviction<K, InternalCacheEntry<K, V>> evict = eviction();
      evict.setMaximum(newSize);
   }

   @Override
   public long evictionSize() {
      Policy.Eviction<K, InternalCacheEntry<K, V>> evict = eviction();
      return evict.weightedSize().orElse(entries.size());
   }

   @Override
   public void cleanUp() {
      evictionCache.cleanUp();
   }
}
