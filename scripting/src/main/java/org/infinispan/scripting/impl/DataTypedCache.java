package org.infinispan.scripting.impl;

import org.infinispan.Cache;
import org.infinispan.CacheCollection;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.cache.impl.AbstractDelegatingAdvancedCache;
import org.infinispan.commons.util.Immutables;
import org.infinispan.util.CollectionAsCacheCollection;
import org.infinispan.util.SetAsCacheSet;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class DataTypedCache<K, V> extends AbstractDelegatingAdvancedCache<K, V> {

   final DataTypedCacheManager dataTypedCacheManager;

   public DataTypedCache(DataTypedCacheManager dataTypedCacheManager, Cache<K, V> cache) {
      super(cache.getAdvancedCache());
      this.dataTypedCacheManager = dataTypedCacheManager;
   }

   private <T> T fromDataType(Object obj) {
      return (T) dataTypedCacheManager.dataType.transformer.fromDataType(obj, dataTypedCacheManager.marshaller);
   }

   private <T> T toDataType(Object obj) {
      return (T) dataTypedCacheManager.dataType.transformer.toDataType(obj, dataTypedCacheManager.marshaller);
   }

   @Override
   public void putForExternalRead(K key, V value) {
      getDelegate().putForExternalRead(fromDataType(key), fromDataType(value));
   }

   @Override
   public void putForExternalRead(K key, V value, long lifespan, TimeUnit unit) {
      getDelegate().putForExternalRead(fromDataType(key), fromDataType(value), lifespan, unit);
   }

   @Override
   public void putForExternalRead(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      getDelegate().putForExternalRead(fromDataType(key), fromDataType(value), lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public void evict(K key) {
      getDelegate().evict(fromDataType(key));
   }

   @Override
   public boolean containsKey(Object key) {
      return getDelegate().containsKey(fromDataType(key));
   }

   @Override
   public boolean containsValue(Object value) {
      return getDelegate().containsValue(fromDataType(value));
   }

   @Override
   public V get(Object key) {
      return toDataType(getDelegate().get(fromDataType(key)));
   }

   @Override
   public CacheSet<K> keySet() {
      CacheStream<K> stream = getDelegate().keySet().stream().map(this::toDataType);
      return new SetAsCacheSet<>(stream.collect(Collectors.toSet()));
   }

   @Override
   public CacheCollection<V> values() {
      CacheStream<V> stream = getDelegate().values().stream().map(this::toDataType);
      return new CollectionAsCacheCollection<>(stream.collect(Collectors.toList()));
   }

   @Override
   public CacheSet<Entry<K, V>> entrySet() {
      CacheStream<Entry<K, V>> stream = getDelegate().entrySet().stream()
            .map(e -> Immutables.immutableEntry(
                  toDataType(e.getKey()), toDataType(e.getValue())));
      return new SetAsCacheSet<>(stream.collect(Collectors.toSet()), stream);
   }

   @Override
   public V put(K key, V value) {
      return toDataType(getDelegate().put(fromDataType(key), fromDataType(value)));
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit unit) {
      return toDataType(getDelegate().put(
            fromDataType(key), fromDataType(value), lifespan, unit));
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit unit) {
      return toDataType(getDelegate().putIfAbsent(
            fromDataType(key), fromDataType(value), lifespan, unit));
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit) {
      Map<K, V> map2 = fromDataTypeMap(map);
      getDelegate().putAll(map2, lifespan, unit);
   }

   public Map<K, V> fromDataTypeMap(Map<? extends K, ? extends V> map) {
      Stream<Entry<K, V>> stream = map.entrySet().stream().map(
            e -> Immutables.immutableEntry(
                  fromDataType(e.getKey()), fromDataType(e.getValue())));
      return (Map<K, V>) stream.collect(Collectors.toMap(Entry::getKey, Entry::getValue));
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit unit) {
      return toDataType(getDelegate().replace(
            fromDataType(key), fromDataType(value), lifespan, unit));
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit) {
      return getDelegate().replace(fromDataType(key), fromDataType(oldValue),
            fromDataType(value), lifespan, unit);
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return toDataType(getDelegate().put(
            fromDataType(key), fromDataType(value), lifespan, lifespanUnit,
            maxIdleTime, maxIdleTimeUnit));
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return toDataType(getDelegate().putIfAbsent(
            fromDataType(key), fromDataType(value), lifespan, lifespanUnit,
            maxIdleTime, maxIdleTimeUnit));
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      Map<K, V> map2 = fromDataTypeMap(map);
      getDelegate().putAll(map2, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return toDataType(getDelegate().replace(fromDataType(key), fromDataType(value),
            lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit));
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return getDelegate().replace(fromDataType(key), fromDataType(oldValue),
            fromDataType(value), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public V remove(Object key) {
      return toDataType(getDelegate().remove(fromDataType(key)));
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> m) {
      Map<K, V> map2 = fromDataTypeMap(m);
      getDelegate().putAll(map2);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value) {
      return getDelegate().putAsync(fromDataType(key), fromDataType(value))
            .thenApply(this::toDataType);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit unit) {
      return getDelegate().putAsync(fromDataType(key), fromDataType(value), lifespan, unit)
            .thenApply(this::toDataType);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return getDelegate().putAsync(fromDataType(key), fromDataType(value),
            lifespan, lifespanUnit, maxIdle, maxIdleUnit)
            .thenApply(this::toDataType);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data) {
      Map<K, V> map2 = fromDataTypeMap(data);
      return getDelegate().putAllAsync(map2);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit) {
      Map<K, V> map2 = fromDataTypeMap(data);
      return getDelegate().putAllAsync(map2, lifespan, unit);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      Map<K, V> map2 = fromDataTypeMap(data);
      return getDelegate().putAllAsync(map2, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value) {
      return getDelegate().putIfAbsentAsync(fromDataType(key), fromDataType(value))
            .thenApply(this::toDataType);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit unit) {
      return getDelegate().putIfAbsentAsync(fromDataType(key), fromDataType(value), lifespan, unit)
            .thenApply(this::toDataType);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return getDelegate().putIfAbsentAsync(fromDataType(key), fromDataType(value),
            lifespan, lifespanUnit, maxIdle, maxIdleUnit)
            .thenApply(this::toDataType);
   }

   @Override
   public CompletableFuture<V> removeAsync(Object key) {
      return getDelegate().removeAsync(fromDataType(key))
            .thenApply(this::toDataType);
   }

   @Override
   public CompletableFuture<Boolean> removeAsync(Object key, Object value) {
      return getDelegate().removeAsync(fromDataType(key), fromDataType(value))
            .thenApply(this::toDataType);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value) {
      return getDelegate().replaceAsync(fromDataType(key), fromDataType(value))
            .thenApply(this::toDataType);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit unit) {
      return getDelegate().replaceAsync(fromDataType(key), fromDataType(value), lifespan, unit)
            .thenApply(this::toDataType);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return getDelegate().replaceAsync(fromDataType(key), fromDataType(value),
            lifespan, lifespanUnit, maxIdle, maxIdleUnit)
            .thenApply(this::toDataType);
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
      return getDelegate().replaceAsync(fromDataType(key),
            fromDataType(oldValue), fromDataType(newValue));
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit) {
      return getDelegate().replaceAsync(fromDataType(key),
            fromDataType(oldValue), fromDataType(newValue), lifespan, unit);
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return getDelegate().replaceAsync(fromDataType(key),
            fromDataType(oldValue), fromDataType(newValue),
            lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<V> getAsync(K key) {
      return getDelegate().getAsync(fromDataType(key))
            .thenApply(this::toDataType);
   }

   @Override
   public V putIfAbsent(K key, V value) {
      return toDataType(getDelegate().putIfAbsent(fromDataType(key), fromDataType(value)));
   }

   @Override
   public boolean remove(Object key, Object value) {
      return getDelegate().remove(fromDataType(key), fromDataType(value));
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue) {
      return getDelegate().replace(fromDataType(key), fromDataType(oldValue), fromDataType(newValue));
   }

   @Override
   public V replace(K key, V value) {
      return getDelegate().replace(fromDataType(key), fromDataType(value));
   }

}

