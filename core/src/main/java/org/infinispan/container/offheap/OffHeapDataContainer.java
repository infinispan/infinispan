package org.infinispan.container.offheap;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.ObjIntConsumer;

import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.commons.util.FilterIterator;
import org.infinispan.commons.util.FilterSpliterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.AbstractInternalDataContainer;
import org.infinispan.container.impl.PeekableTouchableMap;
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

   private OffHeapConcurrentMap map;

   @Start
   public void start() {
      map = new OffHeapConcurrentMap(allocator, offHeapEntryFactory, null);
   }

   @Stop
   public void stop() {
      clear();
      map.close();
   }

   @Override
   protected PeekableTouchableMap<WrappedBytes, WrappedBytes> getMapForSegment(int segment) {
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
   public void addSegments(IntSet segments) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void removeSegments(IntSet segments) {
      throw new UnsupportedOperationException();
   }

   @Override
   public int sizeIncludingExpired() {
      return map.size();
   }

   @Override
   public void clear() {
      map.clear();
   }

   @Override
   public void forEachSegment(ObjIntConsumer<PeekableTouchableMap<WrappedBytes, WrappedBytes>> segmentMapConsumer) {
      segmentMapConsumer.accept(map, 0);
   }
}
