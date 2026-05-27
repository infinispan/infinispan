package org.infinispan.container.impl;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.commons.util.ByRef;
import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.InternalCacheEntry;

import com.github.benmanes.caffeine.cache.Policy;

/**
 * A non-segmented shared container for use with simple/local caches. All entries are stored in the shared
 * caffeine map, but indexed in the single {@code entries} map inherited from {@link DefaultDataContainer}.
 * <p>
 * This class avoids the {@link DefaultSegmentedDataContainer} array-based segment indexing which causes
 * {@link ArrayIndexOutOfBoundsException} when callers pass segment sets larger than the container's
 * single-segment array.
 * <p>
 * Write operations must be done while holding the lock for the corresponding key in the shared caffeine map,
 * same as {@link SharedBoundedContainer}.
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public class SharedBoundedLocalContainer<K, V> extends DefaultDataContainer<K, V> implements EvictionListener<K, V> {

   private final PeekableTouchableCaffeineMap<K, V> caffeineMap;
   private final Map<Object, CompletableFuture<Void>> ensureEvictionDone = new ConcurrentHashMap<>();

   public SharedBoundedLocalContainer(PeekableTouchableCaffeineMap<K, V> caffeineMap) {
      super(new PeekableTouchableContainerMap<>(new ConcurrentHashMap<>()));
      this.caffeineMap = caffeineMap;
   }

   @Override
   protected PeekableTouchableMap<K, V> getMapForSegment(int segment) {
      return caffeineMap;
   }

   @Override
   protected int getSegmentForKey(Object key) {
      return -1;
   }

   @Override
   protected void computeEntryRemoved(int segment, K key, InternalCacheEntry<K, V> value) {
      entries.remove(key, value);
   }

   @Override
   protected void computeEntryWritten(int segment, K key, InternalCacheEntry<K, V> value) {
      entries.put(key, value);
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
   public void clear() {
      entries.entrySet().iterator().forEachRemaining(e ->
            removeEntryInMap(caffeineMap, -1, e.getKey()));
   }

   @Override
   public void clear(IntSet segments) {
      clear();
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
