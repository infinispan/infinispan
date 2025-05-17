package org.infinispan.container.impl;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.ObjIntConsumer;

import org.infinispan.commons.util.IntSet;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;

/**
 * Delegating data container that delegates all calls to the container returned from {@link #delegate()}
 * @author wburns
 * @since 9.0
 */
@Scope(Scopes.NAMED_CACHE)
public abstract class AbstractDelegatingInternalDataContainer<K, V> implements InternalDataContainer<K, V> {
   @Inject
   void inject(ComponentRegistry componentRegistry) {
      componentRegistry.wireDependencies(delegate(), false);
   }

   protected abstract InternalDataContainer<K, V> delegate();

   @Override
   public InternalCacheEntry<K, V> peek(Object k) {
      return delegate().peek(k);
   }

   @Override
   public InternalCacheEntry<K, V> peek(int segment, Object k) {
      return delegate().peek(segment, k);
   }

   @Override
   public boolean touch(int segment, Object k, long currentTimeMillis) {
      return delegate().touch(segment, k, currentTimeMillis);
   }

   @Override
   public void put(K k, V v, Metadata metadata) {
      delegate().put(k, v, metadata);
   }

   @Override
   public void put(int segment, K k, V v, Metadata metadata, PrivateMetadata internalMetadata, long createdTimestamp,
         long lastUseTimestamp) {
      delegate().put(segment, k, v, metadata, internalMetadata, createdTimestamp, lastUseTimestamp);
   }

   @Override
   public boolean containsKey(Object k) {
      return delegate().containsKey(k);
   }

   @Override
   public boolean containsKey(int segment, Object k) {
      return delegate().containsKey(segment, k);
   }

   @Override
   public InternalCacheEntry<K, V> remove(Object k) {
      return delegate().remove(k);
   }

   @Override
   public InternalCacheEntry<K, V> remove(int segment, Object k) {
      return delegate().remove(segment, k);
   }

   @Override
   public void evict(K key) {
      delegate().evict(key);
   }

   @Override
   public CompletionStage<Void> evict(int segment, K key) {
      return delegate().evict(segment, key);
   }

   @Override
   public InternalCacheEntry<K, V> compute(K key, ComputeAction<K, V> action) {
      return delegate().compute(key, action);
   }

   @Override
   public InternalCacheEntry<K, V> compute(int segment, K key, ComputeAction<K, V> action) {
      return delegate().compute(segment, key, action);
   }

   @Stop
   @Override
   public void clear() {
      delegate().clear();
   }

   @Override
   public void clear(IntSet segments) {
      delegate().clear(segments);
   }

   @Override
   public Spliterator<InternalCacheEntry<K, V>> spliterator() {
      return delegate().spliterator();
   }

   @Override
   public Spliterator<InternalCacheEntry<K, V>> spliterator(IntSet segments) {
      return delegate().spliterator(segments);
   }

   @Override
   public Spliterator<InternalCacheEntry<K, V>> spliteratorIncludingExpired() {
      return delegate().spliteratorIncludingExpired();
   }

   @Override
   public Spliterator<InternalCacheEntry<K, V>> spliteratorIncludingExpired(IntSet segments) {
      return delegate().spliteratorIncludingExpired(segments);
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iterator() {
      return delegate().iterator();
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iterator(IntSet segments) {
      return delegate().iterator(segments);
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iteratorIncludingExpired() {
      return delegate().iteratorIncludingExpired();
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iteratorIncludingExpired(IntSet segments) {
      return delegate().iteratorIncludingExpired(segments);
   }

   @Override
   public void forEach(Consumer<? super InternalCacheEntry<K, V>> action) {
      delegate().forEach(action);
   }

   @Override
   public void forEach(IntSet segments, Consumer<? super InternalCacheEntry<K, V>> action) {
      delegate().forEach(segments, action);
   }

   @Override
   public void forEachSegment(ObjIntConsumer<PeekableTouchableMap<K, V>> segmentMapConsumer) {
      delegate().forEachSegment(segmentMapConsumer);
   }

   @Override
   public int size() {
      return delegate().size();
   }

   @Override
   public int size(IntSet segments) {
      return delegate().size(segments);
   }

   @Override
   public int sizeIncludingExpired() {
      return delegate().sizeIncludingExpired();
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

   // Eviction related methods
   @Override
   public long capacity() {
      return delegate().capacity();
   }

   @Override
   public long evictionSize() {
      return delegate().evictionSize();
   }

   @Override
   public void resize(long newSize) {
      delegate().resize(newSize);
   }

   @Override
   public boolean hasExpirable() {
      return delegate().hasExpirable();
   }
}
