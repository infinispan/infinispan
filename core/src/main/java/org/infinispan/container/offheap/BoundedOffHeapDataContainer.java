package org.infinispan.container.offheap;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import org.infinispan.commons.marshall.WrappedBytes;
import org.infinispan.commons.util.FilterIterator;
import org.infinispan.commons.util.FilterSpliterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainerAdapter;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.eviction.EvictionType;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.metadata.Metadata;

/**
 * @author wburns
 * @since 9.4
 */
public class BoundedOffHeapDataContainer extends SegmentedBoundedOffHeapDataContainer {
   @Inject KeyPartitioner keyPartitioner;

   protected final List<Consumer<Iterable<InternalCacheEntry<WrappedBytes, WrappedBytes>>>> listeners =
      new CopyOnWriteArrayList<>();

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

   @Override
   public void put(WrappedBytes key, WrappedBytes value, Metadata metadata) {
      super.put(0, key, value, metadata, -1, -1);
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
   public InternalCacheEntry<WrappedBytes, WrappedBytes> get(int segment, Object k) {
      return super.get(0, k);
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
   public void evict(int segment, WrappedBytes key) {
      super.evict(0, key);
   }

   @Override
   public void put(int segment, WrappedBytes key, WrappedBytes value, Metadata metadata, long createdTimestamp,
                   long lastUseTimestamp) {
      super.put(0, key, value, metadata, createdTimestamp, lastUseTimestamp);
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
   public void addRemovalListener(Consumer<Iterable<InternalCacheEntry<WrappedBytes, WrappedBytes>>> listener) {
      listeners.add(listener);
   }

   @Override
   public void removeRemovalListener(Object listener) {
      listeners.remove(listener);
   }

   @Override
   public void addSegments(IntSet segments) {
      // Don't have to do anything here
   }

   @Override
   public void removeSegments(IntSet segments) {
      InternalDataContainerAdapter.removeSegmentEntries(super.delegate(), segments, listeners, keyPartitioner);
   }
}
