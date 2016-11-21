package org.infinispan.util;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public abstract class AbstractDelegatingConcurrentMap<K, V> extends AbstractDelegatingMap<K, V> implements ConcurrentMap<K, V> {

   protected abstract ConcurrentMap<K, V> delegate();

   @Override
   public boolean remove(Object key, Object value) {
      return delegate().remove(key, value);
   }

   @Override
   public V replace(K key, V value) {
      return delegate().replace(key, value);
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue) {
      return delegate().replace(key, oldValue, newValue);
   }

   @Override
   public V getOrDefault(Object key, V defaultValue) {
      return delegate().getOrDefault(key, defaultValue);
   }

   @Override
   public void forEach(BiConsumer<? super K, ? super V> action) {
      delegate().forEach(action);
   }

   @Override
   public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
      delegate().replaceAll(function);
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
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return delegate().compute(key, remappingFunction);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      return delegate().merge(key, value, remappingFunction);
   }
}
