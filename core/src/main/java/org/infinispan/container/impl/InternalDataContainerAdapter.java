package org.infinispan.container.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.ObjIntConsumer;

import org.infinispan.commons.util.FilterIterator;
import org.infinispan.commons.util.FilterSpliterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.metadata.Metadata;

/**
 * Wrapper around a {@link DataContainer} to provide operations for use with a {@link InternalDataContainer}. Single
 * key operations have the same performance semantics. However bulk operations that operate on a subset of segments,
 * such as {@link #iterator(IntSet)} and {@link #spliterator(IntSet)} require filtering out entries via segment using
 * the provided {@link org.infinispan.distribution.ch.KeyPartitioner} to determine the segments the key belongs to.
 * @author wburns
 * @since 9.3
 */
public class InternalDataContainerAdapter<K, V> extends AbstractDelegatingDataContainer<K, V>
      implements InternalDataContainer<K, V> {
   private final DataContainer<K, V> container;

   protected final List<Consumer<Iterable<InternalCacheEntry<K, V>>>> listeners = new CopyOnWriteArrayList<>();

   @Inject
   private KeyPartitioner keyPartitioner;

   public InternalDataContainerAdapter(DataContainer<K, V> container) {
      this.container = container;
   }

   @Override
   public DataContainer<K, V> delegate() {
      return container;
   }

   @Override
   public InternalCacheEntry<K, V> get(int segment, Object k) {
      return get(k);
   }

   @Override
   public InternalCacheEntry<K, V> peek(int segment, Object k) {
      return peek(k);
   }

   @Override
   public void put(int segment, K k, V v, Metadata metadata, long createdTimestamp, long lastUseTimestamp) {
      put(k, v, metadata);
   }

   @Override
   public boolean containsKey(int segment, Object k) {
      return containsKey(k);
   }

   @Override
   public InternalCacheEntry<K, V> remove(int segment, Object k) {
      return remove(k);
   }

   @Override
   public void evict(int segment, K key) {
      evict(key);
   }

   @Override
   public InternalCacheEntry<K, V> compute(int segment, K key, ComputeAction<K, V> action) {
      return compute(key, action);
   }

   @Override
   public Spliterator<InternalCacheEntry<K, V>> spliterator(IntSet segments) {
      return new FilterSpliterator<>(spliterator(), e -> segments.contains(keyPartitioner.getSegment(e.getKey())));
   }

   @Override
   public Spliterator<InternalCacheEntry<K, V>> spliteratorIncludingExpired(IntSet segments) {
      return new FilterSpliterator<>(spliteratorIncludingExpired(), e -> segments.contains(keyPartitioner.getSegment(e.getKey())));
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iterator(IntSet segments) {
      return new FilterIterator<>(iterator(), e -> segments.contains(keyPartitioner.getSegment(e.getKey())));
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iteratorIncludingExpired(IntSet segments) {
      return new FilterIterator<>(iteratorIncludingExpired(), e -> segments.contains(keyPartitioner.getSegment(e.getKey())));
   }

   @Override
   public void clear(IntSet segments) {
      Iterator<InternalCacheEntry<K, V>> iter = iteratorIncludingExpired(segments);
      while (iter.hasNext()) {
         InternalCacheEntry<K, V> ice = iter.next();
         remove(ice.getKey());
      }
   }

   @Override
   public void forEachIncludingExpired(ObjIntConsumer<? super InternalCacheEntry<K, V>> action) {
      iteratorIncludingExpired().forEachRemaining(ice -> action.accept(ice, keyPartitioner.getSegment(ice.getKey())));
   }

   private static final int REMOTE_SEGMENT_BATCH_SIZE = 32;

   @Override
   public void addSegments(IntSet segments) {
      // Don't have to do anything here
   }

   @Override
   public void removeSegments(IntSet segments) {
      if (!segments.isEmpty()) {
         List<InternalCacheEntry<K, V>> removedEntries;
         if (!listeners.isEmpty()) {
            removedEntries = new ArrayList<>(REMOTE_SEGMENT_BATCH_SIZE);
         } else {
            removedEntries = null;
         }
         Iterator<InternalCacheEntry<K, V>> iter = iteratorIncludingExpired(segments);
         while (iter.hasNext()) {
            InternalCacheEntry<K, V> ice = iter.next();
            remove(ice.getKey());

            if (removedEntries != null) {
               removedEntries.add(ice);
               if (removedEntries.size() == REMOTE_SEGMENT_BATCH_SIZE) {
                  List<InternalCacheEntry<K, V>> unmod = Collections.unmodifiableList(removedEntries);
                  listeners.forEach(c -> c.accept(unmod));
                  removedEntries.clear();
               }
            }
         }
         if (removedEntries != null && !removedEntries.isEmpty()) {
            List<InternalCacheEntry<K, V>> unmod = Collections.unmodifiableList(removedEntries);
            listeners.forEach(c -> c.accept(unmod));
         }
      }
   }

   @Override
   public void addRemovalListener(Consumer<Iterable<InternalCacheEntry<K, V>>> listener) {
      listeners.add(listener);
   }

   @Override
   public void removeRemovalListener(Object listener) {
      listeners.remove(listener);
   }
}
