package org.infinispan.commons.util;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public abstract class AbstractDelegatingMap<K, V> implements Map<K, V> {

   protected abstract Map<K, V> delegate();

   @Override
   public V putIfAbsent(K key, V value) {
      return delegate().putIfAbsent(key, value);
   }

   @Override
   public int size() {
      return delegate().size();
   }

   @Override
   public boolean isEmpty() {
      return delegate().isEmpty();
   }

   @Override
   public boolean containsKey(Object key) {
      return delegate().containsKey(key);
   }

   @Override
   public boolean containsValue(Object value) {
      return delegate().containsValue(value);
   }

   @Override
   public V get(Object key) {
      return delegate().get(key);
   }

   @Override
   public V getOrDefault(Object key, V defaultValue) {
      return delegate().getOrDefault(key, defaultValue);
   }

   @Override
   public V put(K key, V value) {
      return delegate().put(key, value);
   }

   @Override
   public V remove(Object key) {
      return delegate().remove(key);
   }

   @Override
   public boolean remove(Object key, Object value) {
      return delegate().remove(key, value);
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue) {
      return delegate().replace(key, oldValue, newValue);
   }

   @Override
   public V replace(K key, V value) {
      return delegate().replace(key, value);
   }

   @Override
   public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
      delegate().replaceAll(function);
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return delegate().compute(key, remappingFunction);
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
      return delegate().computeIfAbsent(key, mappingFunction);
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return delegate().computeIfPresent(key, remappingFunction);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      return delegate().merge(key, value, remappingFunction);
   }

   @Override
   public void forEach(BiConsumer<? super K, ? super V> action) {
      delegate().forEach(action);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> m) {
      delegate().putAll(m);
   }

   @Override
   public void clear() {
      delegate().clear();
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
   public Set<java.util.Map.Entry<K, V>> entrySet() {
      return delegate().entrySet();
   }

}
