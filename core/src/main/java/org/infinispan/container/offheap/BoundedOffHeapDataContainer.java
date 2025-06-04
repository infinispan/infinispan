package org.infinispan.container.offheap;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.commons.util.FilterIterator;
import org.infinispan.commons.util.FilterSpliterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;

/**
 * @author wburns
 * @since 9.4
 */
public class BoundedOffHeapDataContainer extends SegmentedBoundedOffHeapDataContainer {
   @Inject KeyPartitioner keyPartitioner;

   protected final List<Consumer<Iterable<InternalCacheEntry<WrappedBytes, WrappedBytes>>>> listeners =
      new CopyOnWriteArrayList<>();

   public BoundedOffHeapDataContainer(long maxSize, boolean memoryBounded) {
      super(1, maxSize, memoryBounded);
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
   public InternalCacheEntry<WrappedBytes, WrappedBytes> compute(WrappedBytes key, ComputeAction<WrappedBytes, WrappedBytes> action) {
      return super.compute(0, key, action);
   }

   @Override
   public InternalCacheEntry<WrappedBytes, WrappedBytes> remove(Object k) {
      return super.remove(0, k);
   }

   @Override
   public void evict(WrappedBytes key) {
      CompletionStages.join(super.evict(0, key));
   }

   @Override
   public void put(WrappedBytes key, WrappedBytes value, Metadata metadata) {
      super.put(0, key, value, metadata, null, -1, -1);
   }

   @Override
   public boolean containsKey(int segment, Object k) {
      return super.containsKey(0, k);
   }

   @Override
   public InternalCacheEntry<WrappedBytes, WrappedBytes> peek(int segment, Object k) {
      return super.peek(0, k);
   }

   @Override
   public InternalCacheEntry<WrappedBytes, WrappedBytes> compute(int segment, WrappedBytes key,
                                                                 ComputeAction<WrappedBytes, WrappedBytes> action) {
      return super.compute(0, key, action);
   }

   @Override
   public InternalCacheEntry<WrappedBytes, WrappedBytes> remove(int segment, Object k) {
      return super.remove(0, k);
   }

   @Override
   public CompletionStage<Void> evict(int segment, WrappedBytes key) {
      return super.evict(0, key);
   }

   @Override
   public void put(int segment, WrappedBytes key, WrappedBytes value, Metadata metadata,
         PrivateMetadata internalMetadata, long createdTimestamp,
         long lastUseTimestamp) {
      super.put(0, key, value, metadata, internalMetadata, createdTimestamp, lastUseTimestamp);
   }

   @Override
   public Spliterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> spliterator(IntSet segments) {
      return new FilterSpliterator<>(spliterator(), ice -> segments.contains(keyPartitioner.getSegment(ice.getKey())));
   }

   @Override
   public Spliterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> spliteratorIncludingExpired(IntSet segments) {
      return new FilterSpliterator<>(spliteratorIncludingExpired(),
                                     ice -> segments.contains(keyPartitioner.getSegment(ice.getKey())));
   }

   @Override
   public Iterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> iterator(IntSet segments) {
      return new FilterIterator<>(iterator(), ice -> segments.contains(keyPartitioner.getSegment(ice.getKey())));
   }

   @Override
   public Iterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> iteratorIncludingExpired(IntSet segments) {
      return new FilterIterator<>(iteratorIncludingExpired(),
                                  ice -> segments.contains(keyPartitioner.getSegment(ice.getKey())));
   }

   @Override
   public int sizeIncludingExpired(IntSet segments) {
      int size = 0;
      // We have to loop through and count all the entries
      for (Iterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> iter = iteratorIncludingExpired(segments); iter.hasNext(); ) {
         iter.next();
         if (++size == Integer.MAX_VALUE) return Integer.MAX_VALUE;
      }
      return size;
   }

   @Override
   public int size(IntSet segments) {
      int size = 0;
      // We have to loop through and count the non expired entries
      for (Iterator<InternalCacheEntry<WrappedBytes, WrappedBytes>> iter = iterator(segments); iter.hasNext(); ) {
         iter.next();
         if (++size == Integer.MAX_VALUE) return Integer.MAX_VALUE;
      }
      return size;
   }

   @Override
   public void addRemovalListener(Consumer<Iterable<InternalCacheEntry<WrappedBytes, WrappedBytes>>> listener) {
      listeners.add(listener);
   }

   @Override
   public void removeRemovalListener(Object listener) {
      listeners.remove(listener);
   }

   @Override
   public void addSegments(IntSet segments) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void removeSegments(IntSet segments) {
      throw new UnsupportedOperationException();
   }
}
