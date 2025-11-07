package org.infinispan.container.impl;

import java.util.Map;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.container.entries.InternalCacheEntry;

import com.github.benmanes.caffeine.cache.Policy;

/**
 * A shared container where all of the entries are stored in the caffeine map, but they are indexed in the map segment
 * container of the DefaultsegmentedDataContainer. The {@link #onEntryChosenForEviction(boolean, Object, InternalCacheEntry)}
 * must be invoked externally whenever an entry is evicted from the provided <b>caffeineMap</b>.
 * <p>
 * To guarantee consistency any write operations must be done while in the lock for the corresponding key of the
 * provided <b>caffeineMap</b>. This means any writes to any of the maps per segment must only be done while in this
 * secured region.
 * <p>
 * This class is implemented so that every single key read and write operation are constant time and iteration is O(N)
 * where N is the size of the entries in the individual container and not the shared. However, due to this the
 * {@link #clear()} and {@link #clear(IntSet)} methods are not O(1) as in many other implementations but rather are O(N).
 * Please see them respectively for more details.
 * @param <K> The key type
 * @param <V> The value type
 */
public class SharedBoundedContainer<K, V> extends DefaultSegmentedDataContainer<K, V> implements EvictionListener<K, V>,
      Consumer<Iterable<InternalCacheEntry<K, V>>> {

   private final PeekableTouchableCaffeineMap<K, V> caffeineMap;
   private final Map<Object, CompletableFuture<Void>> ensureEvictionDone = new ConcurrentHashMap<>();

   public SharedBoundedContainer(PeekableTouchableCaffeineMap<K, V> caffeineMap,
                                 Supplier<PeekableTouchableMap<K, V>> peekableTouchableMapSupplier,
                                 int numSegments) {
      super(peekableTouchableMapSupplier, numSegments);
      this.caffeineMap = caffeineMap;
   }

   @Override
   public void start() {
      super.start();
      // If we remove segments on stop, optimize for segment removal
      if (shouldStopSegments) {
         addRemovalListener(this);
      }
   }

   @Override
   public PeekableTouchableMap<K, V> getMapForSegment(int segment) {
      return caffeineMap;
   }

   @Override
   protected void computeEntryRemoved(int segment, K key, InternalCacheEntry<K, V> value) {
      ConcurrentMap<K, InternalCacheEntry<K, V>> map = super.getMapForSegment(segment);
      if (map != null) {
         map.remove(key, value);
      }
   }

   @Override
   protected void computeEntryWritten(int segment, K key, InternalCacheEntry<K, V> value) {
      ConcurrentMap<K, InternalCacheEntry<K, V>> map = super.getMapForSegment(segment);
      if (map != null) {
         map.put(key, value);
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

   /**
    * Removes all entries that map only to this container and its map entries. To not clear the underlying
    * <b>caffeineMap</b> completely we must do additional work, see {@link #clear(IntSet)} for implementation and
    * performance details.
    */
   @Override
   public void clear() {
      clear(IntSets.immutableRangeSet(maps.length()));
   }

   /**
    * Clears only the entries that map to the given segments. Note that to ensure the shared <b>caffeineMap</b> is not
    * adversely affected we have to iterate over all of the entries in the map segment storage and then individually
    * remove those values from the <b>caffeineMap</b>. This way we still have consistency as the <b>caffeineMap</b> lock
    * is acquired before removing from the map segment.
    * <p>
    * This iteration means that this clear operation performs with O(N) time complexity as we must only remove the
    * entries we control from the <b>caffeineMap</b> and our segment maps.
    * <p>
    * Due to clearing a map or its segments should be done much less frequently than other operations in this
    * implementation, these two clear methods have increased time complexity to allow all other operations to have
    * their best respective time complexity implementations.
    * @param segments segments of entries to remove
    */
   @Override
   public void clear(IntSet segments) {
      for (PrimitiveIterator.OfInt iter = segments.iterator(); iter.hasNext(); ) {
         int segment = iter.nextInt();
         PeekableTouchableMap<K, V> map = maps.get(segment);
         if (map != null) {
            // Note this must use the iterator and cannot use forEach as it would be inside the Map lock
            map.entrySet().iterator().forEachRemaining(ice -> {
               // TODO: should we check the value??
               removeEntryInMap(caffeineMap, segment, ice.getKey());
            });
         }
      }
   }

   @Override
   public void accept(Iterable<InternalCacheEntry<K, V>> internalCacheEntries) {
      // This technically does not acquire the caffeineMap lock before removal for the map segments, but should be fine
      // as the segment map was just removed completely anyway
      caffeineMap.removeAll(() -> new IteratorMapper<>(internalCacheEntries.iterator(), InternalCacheEntry::getKey));
   }

   @Override
   public void removeSegments(IntSet segments) {
      // Normally, we must first acquire the lock on the <b>caffeineMap</b> before doing any write operations, but when
      // <b>shouldStopSegments</b> is <b>true</b> we are removing
      // the segment completely it is fine to remove the entry from the <b>caffeineMap</b> after the segment removal
      // is done since the Map will no longer exist in the segment array. The removal from the <b>caffeineMap</b> is
      // handled via the segment listener and the accept method this class implements
      if (shouldStopSegments) {
         super.removeSegments(segments);
      } else {
         clear(segments);
      }
   }

   @Override
   public InternalCacheEntry<K, V> peek(Object k) {
      return peek(-1, k);
   }

   @Override
   public boolean containsKey(Object k) {
      return containsKey(-1, k);
   }

   @Override
   public void cleanUp() {
      caffeineMap.cleanUp();
   }

   private Policy.Eviction<K, InternalCacheEntry<K, V>> eviction() {
      Optional<Policy.Eviction<K, InternalCacheEntry<K, V>>> eviction = caffeineMap.policy().eviction();
      if (eviction.isPresent()) {
         return eviction.get();
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
      return evict.weightedSize().orElse(caffeineMap.size());
   }

   @Override
   public void onEntryChosenForEviction(boolean pre, K key, InternalCacheEntry<K, V> value) {
      if (pre) {
         // Schedule an eviction to happen after the key lock is released
         CompletableFuture<Void> future = new CompletableFuture<>();
         ensureEvictionDone.put(key, future);
         handleEviction(value, orderer, passivator.running(), evictionManager, this,
               nonBlockingExecutor, future);
      } else {
         computeEntryRemoved(getSegmentForKey(key), key, value);
         CompletableFuture<Void> future = ensureEvictionDone.remove(key);
         if (future != null) {
            future.complete(null);
         }
      }
   }
}
