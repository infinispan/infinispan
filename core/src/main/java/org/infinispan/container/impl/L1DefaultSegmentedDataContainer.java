package org.infinispan.container.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.infinispan.commons.util.FlattenSpliterator;
import org.infinispan.commons.util.ConcatIterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.SmallIntSet;
import org.infinispan.container.entries.InternalCacheEntry;

/**
 * Segmented data container that also allows for non owned segments to be written to a temporary map (L1). This
 * temporary map is cleared whenever a segment becomes no longer owned.
 * <p>
 * If the segment is owned, only the owner segment is used. If the segment is not owned it will query the tempoary
 * map to see if the object is stored there.
 * @author wburns
 * @since 9.3
 */
public class L1DefaultSegmentedDataContainer<K, V> extends DefaultSegmentedDataContainer<K, V> {
   private final ConcurrentMap<K, InternalCacheEntry<K, V>> nonOwnedEntries;

   public L1DefaultSegmentedDataContainer(int numSegments) {
      super(numSegments);
      this.nonOwnedEntries = new ConcurrentHashMap<>();
   }

   @Override
   protected ConcurrentMap<K, InternalCacheEntry<K, V>> getMapForSegment(int segment) {
      ConcurrentMap<K, InternalCacheEntry<K, V>> map = super.getMapForSegment(segment);
      if (map == null) {
         map = nonOwnedEntries;
      }
      return map;
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
         valueIterables.add(nonOwnedEntries.values().stream()
               .filter(e -> segments.contains(getSegmentForKey(e.getKey())))
               .collect(Collectors.toSet()));
      }
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
      valueIterables.add(nonOwnedEntries.values());
      return new ConcatIterator<>(valueIterables);
   }

   @Override
   public Spliterator<InternalCacheEntry<K, V>> spliteratorIncludingExpired(IntSet segments) {
      // Copy the ints into an array to parallelize them if needed
      int[] segmentArray = segments.toIntArray();
      // This variable is used when we query a segment we don't own. In this case we return all the tempoary
      // entries and set the variable, ensuring we don't return it on any subsequent segment "misses"
      AtomicBoolean usedOthers = new AtomicBoolean(false);

      return new FlattenSpliterator<>(i -> {
         ConcurrentMap<K, InternalCacheEntry<K, V>> map = maps.get(segmentArray[i]);
         if (map == null) {
            if (!usedOthers.getAndSet(true)) {
               return nonOwnedEntries.values().stream()
                     .filter(e -> segments.contains(getSegmentForKey(e.getKey())))
                     .collect(Collectors.toSet());
            }
            return Collections.emptyList();
         }
         return map.values();
      }, segmentArray.length, Spliterator.CONCURRENT | Spliterator.NONNULL | Spliterator.DISTINCT);
   }

   @Override
   public Spliterator<InternalCacheEntry<K, V>> spliteratorIncludingExpired() {
      // This variable is used when we query a segment we don't own. In this case we return all the tempoary
      // entries and set the variable, ensuring we don't return it on any subsequent segment "misses"
      AtomicBoolean usedOthers = new AtomicBoolean(false);
      return new FlattenSpliterator<>(i -> {
         ConcurrentMap<K, InternalCacheEntry<K, V>> map = maps.get(i);
         if (map == null) {
            if (!usedOthers.getAndSet(true)) {
               return nonOwnedEntries.values();
            }
            return Collections.emptyList();
         }
         return map.values();
      }, maps.length(), Spliterator.CONCURRENT | Spliterator.NONNULL | Spliterator.DISTINCT);
   }

   @Override
   public void clear() {
      nonOwnedEntries.clear();
      super.clear();
   }

   /**
    * Removes all entries that map to the given segments
    * @param segments the segments to clear data for
    */
   @Override
   public void clear(IntSet segments) {
      IntSet extraSegments = null;
      PrimitiveIterator.OfInt iter = segments.iterator();
      // First try to just clear the respective maps
      while (iter.hasNext()) {
         int segment = iter.nextInt();
         ConcurrentMap<K, InternalCacheEntry<K, V>> map = maps.get(segment);
         if (map != null) {
            map.clear();
         } else {
            // If we don't have a map for a segment we have to later go through the unowned segments and remove
            // those entries separately
            if (extraSegments == null) {
               extraSegments = new SmallIntSet(segments.size());
            }
            extraSegments.add(segment);
         }
      }

      if (extraSegments != null) {
         IntSet finalExtraSegments = extraSegments;
         nonOwnedEntries.keySet().removeIf(k -> finalExtraSegments.contains(getSegmentForKey(k)));
      }
   }

   @Override
   public void removeSegments(IntSet segments) {
      if (!segments.isEmpty()) {
         nonOwnedEntries.clear();
      }
      super.removeSegments(segments);
   }
}
