package org.infinispan.container.offheap;

import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.commons.util.IntSets;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.eviction.EvictionType;
import org.infinispan.factories.annotations.Start;

/**
 * @author wburns
 * @since 9.4
 */
public class BoundedOffHeapDataContainer extends SegmentedBoundedOffHeapDataContainer {
   public BoundedOffHeapDataContainer(int addressCount, long maxSize, EvictionType type) {
      super(addressCount, 1, maxSize, type);
   }

   @Override
   @Start
   public void start() {
      super.start();
      // Force the start up of segment 0
      addSegments(IntSets.immutableSet(0));
   }

   @Override
   protected OffHeapConcurrentMap getMapThatContainsKey(byte[] key) {
      return (OffHeapConcurrentMap) dataContainer.getMapForSegment(0);
   }

   @Override
   public boolean containsKey(Object k) {
      return super.containsKey(0, k);
   }

   @Override
   public InternalCacheEntry<WrappedBytes, WrappedBytes> peek(Object k) {
      return super.peek(0, k);
   }

   @Override
   public InternalCacheEntry<WrappedBytes, WrappedBytes> get(Object k) {
      return super.get(0, k);
   }

   @Override
   public InternalCacheEntry<WrappedBytes, WrappedBytes> compute(WrappedBytes key, ComputeAction<WrappedBytes, WrappedBytes> action) {
      return super.compute(0, key, action);
   }

   @Override
   public InternalCacheEntry<WrappedBytes, WrappedBytes> remove(Object k) {
      return super.remove(0, k);
   }

   @Override
   public void evict(WrappedBytes key) {
      super.evict(0, key);
   }
}
