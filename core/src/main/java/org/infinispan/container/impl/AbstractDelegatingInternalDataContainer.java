package org.infinispan.container.impl;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.ObjIntConsumer;

import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.metadata.Metadata;

/**
 * @author wburns
 * @since 9.0
 */
public abstract class AbstractDelegatingInternalDataContainer<K, V> extends AbstractDelegatingDataContainer<K, V> implements InternalDataContainer<K, V> {
   @Override
   protected abstract InternalDataContainer<K, V> delegate();

   @Override
   public InternalCacheEntry<K, V> get(int segment, Object k) {
      return delegate().get(segment, k);
   }

   @Override
   public InternalCacheEntry<K, V> peek(int segment, Object k) {
      return delegate().peek(segment, k);
   }

   @Override
   public void put(int segment, K k, V v, Metadata metadata, long createdTimestamp, long lastUseTimestamp) {
      delegate().put(segment, k, v, metadata, createdTimestamp, lastUseTimestamp);
   }

   @Override
   public boolean containsKey(int segment, Object k) {
      return delegate().containsKey(segment, k);
   }

   @Override
   public InternalCacheEntry<K, V> remove(int segment, Object k) {
      return delegate().remove(segment, k);
   }

   @Override
   public void evict(int segment, K key) {
      delegate().evict(segment, key);
   }

   @Override
   public InternalCacheEntry<K, V> compute(int segment, K key, ComputeAction<K, V> action) {
      return delegate().compute(segment, key, action);
   }

   @Override
   public void clear(IntSet segments) {
      delegate().clear(segments);
   }

   @Override
   public Spliterator<InternalCacheEntry<K, V>> spliterator(IntSet segments) {
      return delegate().spliterator(segments);
   }

   @Override
   public Spliterator<InternalCacheEntry<K, V>> spliteratorIncludingExpired(IntSet segments) {
      return delegate().spliteratorIncludingExpired(segments);
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iterator(IntSet segments) {
      return delegate().iterator(segments);
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iteratorIncludingExpired(IntSet segments) {
      return delegate().iteratorIncludingExpired(segments);
   }

   @Override
   public void forEachIncludingExpired(ObjIntConsumer<? super InternalCacheEntry<K, V>> action) {
      delegate().forEachIncludingExpired(action);
   }

   @Override
   public void forEach(IntSet segments, Consumer<? super InternalCacheEntry<K, V>> action) {
      delegate().forEach(segments, action);
   }

   @Override
   public int size(IntSet segments) {
      return delegate().size(segments);
   }

   @Override
   public int sizeIncludingExpired(IntSet segments) {
      return delegate().sizeIncludingExpired(segments);
   }

   @Override
   public void addSegments(IntSet segments) {
      delegate().addSegments(segments);
   }

   @Override
   public void removeSegments(IntSet segments) {
      delegate().removeSegments(segments);
   }

   @Override
   public void addRemovalListener(Consumer<Iterable<InternalCacheEntry<K, V>>> listener) {
      delegate().addRemovalListener(listener);
   }

   @Override
   public void removeRemovalListener(Object listener) {
      delegate().removeRemovalListener(listener);
   }
}
