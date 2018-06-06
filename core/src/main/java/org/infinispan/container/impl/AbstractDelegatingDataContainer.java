package org.infinispan.container.impl;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.filter.KeyFilter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.metadata.Metadata;

/**
 * Delegating data container that delegates all calls to the container returned from {@link #delegate()}
 * @author wburns
 * @since 9.3
 */
public abstract class AbstractDelegatingDataContainer<K, V> implements DataContainer<K, V> {

   abstract DataContainer<K, V> delegate();

   @Override
   public InternalCacheEntry<K, V> get(Object k) {
      return delegate().get(k);
   }

   @Override
   public InternalCacheEntry<K, V> peek(Object k) {
      return delegate().peek(k);
   }

   @Override
   public void put(K k, V v, Metadata metadata) {
      delegate().put(k, v, metadata);
   }

   @Override
   public boolean containsKey(Object k) {
      return delegate().containsKey(k);
   }

   @Override
   public InternalCacheEntry<K, V> remove(Object k) {
      return delegate().remove(k);
   }

   @Override
   public int size() {
      return delegate().size();
   }

   @Override
   public int sizeIncludingExpired() {
      return delegate().sizeIncludingExpired();
   }

   @Override
   public void clear() {
      delegate().clear();
   }

   @Override
   public void evict(K key) {
      delegate().evict(key);
   }

   @Override
   public InternalCacheEntry<K, V> compute(K key, ComputeAction<K, V> action) {
      return delegate().compute(key, action);
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iterator() {
      return delegate().iterator();
   }

   @Override
   public Iterator<InternalCacheEntry<K, V>> iteratorIncludingExpired() {
      return delegate().iteratorIncludingExpired();
   }

   @Override
   public Spliterator<InternalCacheEntry<K, V>> spliterator() {
      return delegate().spliterator();
   }

   @Override
   public Spliterator<InternalCacheEntry<K, V>> spliteratorIncludingExpired() {
      return delegate().spliteratorIncludingExpired();
   }

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
   public void forEach(Consumer<? super InternalCacheEntry<K, V>> action) {
      delegate().forEach(action);
   }

   @Override
   public Set<K> keySet() {
      return delegate().keySet();
   }

   @Override
   public Collection<V> values() {
      return delegate().values();
   }

   @Override
   public Set<InternalCacheEntry<K, V>> entrySet() {
      return delegate().entrySet();
   }

   @Override
   public void executeTask(KeyFilter<? super K> filter, BiConsumer<? super K, InternalCacheEntry<K, V>> action) throws InterruptedException {
      delegate().executeTask(filter, action);
   }

   @Override
   public void executeTask(KeyValueFilter<? super K, ? super V> filter, BiConsumer<? super K, InternalCacheEntry<K, V>> action) throws InterruptedException {
      delegate().executeTask(filter, action);
   }
}
