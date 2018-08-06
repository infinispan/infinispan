package org.infinispan.container.offheap;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentMap;
import java.util.function.ObjIntConsumer;

import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.commons.util.FilterIterator;
import org.infinispan.commons.util.FilterSpliterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.ProcessorInfo;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.AbstractInternalDataContainer;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;

/**
 * @author wburns
 * @since 9.4
 */
public class OffHeapDataContainer extends AbstractInternalDataContainer<WrappedBytes, WrappedBytes> {
   @Inject protected OffHeapMemoryAllocator allocator;
   @Inject protected OffHeapEntryFactory offHeapEntryFactory;

   private final int desiredSize;

   private OffHeapConcurrentMap map;

   public OffHeapDataContainer(int desiredSize) {
      this.desiredSize = desiredSize;
   }

   public static int getActualAddressCount(int desiredSize) {
      return OffHeapConcurrentMap.getActualAddressCount(desiredSize,
            Util.findNextHighestPowerOfTwo(ProcessorInfo.availableProcessors() << 1));
   }

   @Start
   public void start() {
      map = new OffHeapConcurrentMap(desiredSize, allocator, offHeapEntryFactory, null);
      map.start();
   }

   @Stop
   public void stop() {
      map.stop();
   }

   @Override
   protected ConcurrentMap<WrappedBytes, InternalCacheEntry<WrappedBytes, WrappedBytes>> getMapForSegment(int segment) {
      return map;
   }

   @Override
   protected int getSegmentForKey(Object key) {
      // We always map to same map, so no reason to waste finding out segment
      return -1;
   }

   @Override
   public Spliterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> spliterator() {
      return filterExpiredEntries(spliteratorIncludingExpired());
   }

   @Override
   public Spliterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> spliterator(IntSet segments) {
      return new FilterSpliterator<>(spliterator(), ice -> segments.contains(keyPartitioner.getSegment(ice.getKey())));
   }

   @Override
   public Spliterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> spliteratorIncludingExpired() {
      return map.values().spliterator();
   }

   @Override
   public Spliterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> spliteratorIncludingExpired(IntSet segments) {
      return new FilterSpliterator<>(spliteratorIncludingExpired(),
            ice -> segments.contains(keyPartitioner.getSegment(ice.getKey())));
   }

   @Override
   public Iterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> iterator() {
      return new EntryIterator(iteratorIncludingExpired());
   }

   @Override
   public Iterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> iterator(IntSet segments) {
      return new FilterIterator<>(iterator(), ice -> segments.contains(keyPartitioner.getSegment(ice.getKey())));
   }

   @Override
   public Iterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> iteratorIncludingExpired() {
      return map.values().iterator();
   }

   @Override
   public Iterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> iteratorIncludingExpired(IntSet segments) {
      return new FilterIterator<>(iteratorIncludingExpired(),
            ice -> segments.contains(keyPartitioner.getSegment(ice.getKey())));
   }

   @Override
   public void forEachIncludingExpired(ObjIntConsumer<? super InternalCacheEntry<WrappedBytes, WrappedBytes>> action) {
      map.values().forEach(e -> action.accept(e, 0));
   }

   @Override
   public void addSegments(IntSet segments) {
      throw new UnsupportedOperationException("Container is not segmented");
   }

   @Override
   public void removeSegments(IntSet segments) {
      throw new UnsupportedOperationException("Container is not segmented");
   }

   @Override
   public int sizeIncludingExpired() {
      return map.size();
   }

   @Override
   public void clear() {
      map.clear();
   }
}
