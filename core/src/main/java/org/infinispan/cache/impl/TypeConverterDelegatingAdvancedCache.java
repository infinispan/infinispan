package org.infinispan.cache.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheCollection;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.commands.read.AbstractCloseableIteratorCollection;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorMapper;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.CloseableSpliteratorMapper;
import org.infinispan.commons.util.InjectiveFunction;
import org.infinispan.compat.ConverterKeyMapper;
import org.infinispan.compat.ConverterEntryMapper;
import org.infinispan.compat.ConverterValueMapper;
import org.infinispan.compat.TypeConverter;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.compat.BaseTypeConverterInterceptor;
import org.infinispan.metadata.Metadata;

/**
 * Advanced cache that converts key/value passed in to the new type before passing to the underlying cache.  Results
 * are then also converted back using the converter.
 * @author wburns
 * @since 9.0
 */
public class TypeConverterDelegatingAdvancedCache<K, V> extends AbstractDelegatingAdvancedCache<K, V> {
   private final TypeConverter converter;
   private InternalEntryFactory entryFactory;

   public TypeConverterDelegatingAdvancedCache(AdvancedCache<K, V> cache, TypeConverter converter) {
      super(cache, c -> new TypeConverterDelegatingAdvancedCache<>(c, converter));
      this.converter = converter;
   }

   protected TypeConverterDelegatingAdvancedCache(AdvancedCache<K, V> cache, AdvancedCacheWrapper<K, V> wrapper,
                                                  TypeConverter converter) {
      super(cache, wrapper);
      this.converter = converter;
   }

   @Inject
   public void wireRealCache(ComponentRegistry registry, InternalEntryFactory entryFactory) {
      registry.wireDependencies(cache);
      this.entryFactory = entryFactory;
   }

   protected K boxKey(K key) {
      return (K) getConverter().boxKey(key);
   }

   protected V boxValue(V value) {
      return (V) getConverter().boxValue(value);
   }

   protected K unboxKey(K key) {
      return (K) getConverter().unboxKey(key);
   }

   protected V unboxValue(V value) {
      return (V) getConverter().unboxValue(value);
   }

   protected CacheEntry<K, V> convertEntry(K newKey, V newValue, CacheEntry<K, V> entry) {
      if (entry instanceof InternalCacheEntry) {
         return entryFactory.create(newKey, newValue, (InternalCacheEntry) entry);
      } else {
         return entryFactory.create(newKey, newValue, entry.getMetadata().version(), entry.getCreated(),
               entry.getLifespan(), entry.getLastUsed(), entry.getMaxIdle());
      }
   }

   protected TypeConverter getConverter() {
      return converter;
   }

   private Function<? super K, ? extends V> convertFunction(Function<? super K, ? extends V> mappingFunction) {
      return k -> mappingFunction.apply(boxKey(k));
   }

   private BiFunction<? super K, ? super V, ? extends V> convertFunction(
         BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return (k, v) -> remappingFunction.apply(unboxKey(k), unboxValue(v));
   }

   private Map<K, V> boxMap(Map<? extends K, ? extends V> map) {
      Map<K, V> newMap = new HashMap<>(map.size());
      map.forEach((k, v) -> newMap.put(boxKey(k), boxValue(v)));
      return newMap;
   }

   private Map<K, V> unboxMap(Map<K, V> map) {
      // To make sure results are ordered
      Map<K, V> newMap = new LinkedHashMap<>(map.size());
      map.forEach((k, v) -> newMap.put(unboxKey(k), unboxValue(v)));
      return newMap;
   }

   private Map<K, CacheEntry<K, V>> unboxEntryMap(Map<K, CacheEntry<K, V>> map) {
      Map<K, CacheEntry<K, V>> entryMap = new HashMap<>(map.size());
      map.values().forEach(v -> {
         K originalKey = v.getKey();
         K boxedKey = boxKey(originalKey);
         V originalValue = v.getValue();
         V boxedValue = boxValue(originalValue);
         if (boxedKey != originalKey || boxedValue != originalValue) {
            entryMap.put(boxedKey, convertEntry(boxedKey, boxedValue, v));
         }
      });
      return entryMap;
   }

   private Collection<K> convertKeys(Collection<? extends K> keys) {
      List<K> list = new ArrayList<>(keys.size());
      keys.forEach(k -> list.add(boxKey(k)));
      return list;
   }

   private Set<?> convertKeys(Set<?> keys) {
      // Use LinkedHashSet just incase if the set was ordered
      Set<K> newKeys = new LinkedHashSet<>(keys.size());
      keys.forEach(k -> newKeys.add(boxKey((K) k)));
      return newKeys;
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      V returned = super.compute(boxKey(key), convertFunction(remappingFunction));
      return unboxValue(returned);
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
      V returned = super.computeIfAbsent(boxKey(key), convertFunction(mappingFunction));
      return unboxValue(returned);
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      V returned = super.computeIfPresent(boxKey(key), convertFunction(remappingFunction));
      return unboxValue(returned);
   }

   @Override
   public V put(K key, V value) {
      V returned = super.put(boxKey(key), boxValue(value));
      return unboxValue(returned);
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      V returned = super.put(boxKey(key), boxValue(value), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return unboxValue(returned);
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit unit) {
      V returned = super.put(boxKey(key), boxValue(value), lifespan, unit);
      return unboxValue(returned);
   }

   @Override
   public V put(K key, V value, Metadata metadata) {
      V returned = super.put(boxKey(key), boxValue(value), metadata);
      return unboxValue(returned);
   }

   @Override
   public V putIfAbsent(K key, V value) {
      V returned = super.putIfAbsent(boxKey(key), boxValue(value));
      return unboxValue(returned);
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      V returned = super.putIfAbsent(boxKey(key), boxValue(value), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return unboxValue(returned);
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit unit) {
      V returned = super.putIfAbsent(boxKey(key), boxValue(value), lifespan, unit);
      return unboxValue(returned);
   }

   @Override
   public V putIfAbsent(K key, V value, Metadata metadata) {
      V returned = super.putIfAbsent(boxKey(key), boxValue(value), metadata);
      return unboxValue(returned);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      super.putAll(boxMap(map), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit) {
      super.putAll(boxMap(map), lifespan, unit);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> t) {
      super.putAll(boxMap(t));
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data) {
      return super.putAllAsync(boxMap(data));
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return super.putAllAsync(boxMap(data), lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit) {
      return super.putAllAsync(boxMap(data), lifespan, unit);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value) {
      return super.putAsync(boxKey(key), boxValue(value)).thenApply(this::unboxValue);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return super.putAsync(boxKey(key), boxValue(value), lifespan, lifespanUnit, maxIdle, maxIdleUnit).thenApply(this::unboxValue);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit unit) {
      return super.putAsync(boxKey(key), boxValue(value), lifespan, unit).thenApply(this::unboxValue);
   }

   @Override
   public void putForExternalRead(K key, V value) {
      super.putForExternalRead(boxKey(key), boxValue(value));
   }

   @Override
   public void putForExternalRead(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      super.putForExternalRead(boxKey(key), boxValue(value), lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public void putForExternalRead(K key, V value, long lifespan, TimeUnit unit) {
      super.putForExternalRead(boxKey(key), boxValue(value), lifespan, unit);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value) {
      return super.putIfAbsentAsync(boxKey(key), boxValue(value)).thenApply(this::unboxValue);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return super.putIfAbsentAsync(boxKey(key), boxValue(value), lifespan, lifespanUnit, maxIdle, maxIdleUnit).thenApply(this::unboxValue);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit unit) {
      return super.putIfAbsentAsync(boxKey(key), boxValue(value), lifespan, unit).thenApply(this::unboxValue);
   }

   @Override
   public V remove(Object key) {
      V returned = super.remove(boxKey((K) key));
      return unboxValue(returned);
   }

   @Override
   public boolean remove(Object key, Object value) {
      return super.remove(boxKey((K) key), boxValue((V) value));
   }

   @Override
   public CompletableFuture<V> removeAsync(Object key) {
      return super.removeAsync(boxKey((K) key)).thenApply(this::unboxValue);
   }

   @Override
   public CompletableFuture<Boolean> removeAsync(Object key, Object value) {
      return super.removeAsync(boxKey((K) key), boxValue((V) value));
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue) {
      return super.replace(boxKey(key), boxValue(oldValue), boxValue(newValue));
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return super.replace(boxKey(key), boxValue(oldValue), boxValue(value), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit) {
      return super.replace(boxKey(key), boxValue(oldValue), boxValue(value), lifespan, unit);
   }

   @Override
   public V replace(K key, V value) {
      V returned = super.replace(boxKey(key), boxValue(value));
      return unboxValue(returned);
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      V returned = super.replace(boxKey(key), boxValue(value), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return unboxValue(returned);
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit unit) {
      V returned = super.replace(boxKey(key), boxValue(value), lifespan, unit);
      return unboxValue(returned);
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
      return super.replaceAsync(boxKey(key), boxValue(oldValue), boxValue(newValue));
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return super.replaceAsync(boxKey(key), boxValue(oldValue), boxValue(newValue), lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit) {
      return super.replaceAsync(boxKey(key), boxValue(oldValue), boxValue(newValue), lifespan, unit);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value) {
      return super.replaceAsync(boxKey(key), boxValue(value)).thenApply(this::unboxValue);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return super.replaceAsync(boxKey(key), boxValue(value), lifespan, lifespanUnit, maxIdle, maxIdleUnit).thenApply(this::unboxValue);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit unit) {
      return super.replaceAsync(boxKey(key), boxValue(value), lifespan, unit).thenApply(this::unboxValue);
   }

   @Override
   protected void set(K key, V value) {
      super.set(boxKey(key), boxValue(value));
   }

   @Override
   public V get(Object key) {
      V returned = super.get(boxKey((K) key));
      return unboxValue(returned);
   }

   @Override
   public CompletableFuture<V> getAsync(K key) {
      return super.getAsync(boxKey(key)).thenApply(this::unboxValue);
   }

   @Override
   public void evict(K key) {
      super.evict(boxKey((K) key));
   }

   @Override
   public boolean containsKey(Object key) {
      return super.containsKey(boxKey((K) key));
   }

   @Override
   public boolean containsValue(Object value) {
      return super.containsValue(boxValue((V) value));
   }

   @Override
   public boolean replace(K key, V oldValue, V value, Metadata metadata) {
      return super.replace(boxKey(key), boxValue(oldValue), boxValue(value), metadata);
   }

   @Override
   public V replace(K key, V value, Metadata metadata) {
      V returned = super.replace(boxKey(key), boxValue(value), metadata);
      return unboxValue(returned);
   }

   @Override
   public void putForExternalRead(K key, V value, Metadata metadata) {
      super.putForExternalRead(boxKey(key), boxValue(value), metadata);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, Metadata metadata) {
      return super.putAsync(boxKey(key), boxValue(value), metadata).thenApply(this::unboxValue);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, Metadata metadata) {
      super.putAll(boxMap(map), metadata);
   }

   @Override
   public boolean lock(Collection<? extends K> keys) {
      return super.lock(convertKeys(keys));
   }

   @Override
   public boolean lock(K... keys) {
      K[] newKeys = (K[]) new Object[keys.length];
      for (int i = 0; i < keys.length; ++i) {
         newKeys[i] = boxKey(keys[i]);
      }
      return super.lock(newKeys);
   }

   @Override
   public void removeExpired(K key, V value, Long lifespan) {
      super.removeExpired(boxKey(key), boxValue(value), lifespan);
   }

   final ConverterEntryMapper entryMapper = new ConverterEntryMapper();

   private <E extends Entry<K, V>> CacheSet<E> cast(CacheSet set) {
      return (CacheSet<E>) set;
   }

   private class TypeConverterEntrySet extends AbstractCloseableIteratorCollection<CacheEntry<K, V>, K, V> implements CacheSet<CacheEntry<K, V>> {
      private final CacheSet<CacheEntry<K, V>> actualCollection;

      public TypeConverterEntrySet(Cache<K, V> cache, CacheSet<CacheEntry<K, V>> actualCollection) {
         super(cache);
         this.actualCollection = actualCollection;
      }

      @Override
      public CacheStream<CacheEntry<K, V>> stream() {
         return actualCollection.stream().map(entryMapper);
      }

      @Override
      public CacheStream<CacheEntry<K, V>> parallelStream() {
         return actualCollection.parallelStream().map(entryMapper);
      }

      @Override
      public CloseableIterator<CacheEntry<K, V>> iterator() {
         return new BaseTypeConverterInterceptor.TypeConverterIterator<>(actualCollection.iterator(), getConverter(),
               entryFactory);
      }

      @Override
      public CloseableSpliterator<CacheEntry<K, V>> spliterator() {
         return new CloseableSpliteratorMapper<>(actualCollection.spliterator(),
               (InjectiveFunction & Function<CacheEntry<K, V>, CacheEntry<K, V>>) entry -> {
                  K key = entry.getKey();
                  K unboxedKey = unboxKey(key);
                  V value = entry.getValue();
                  V unboxedValue = unboxValue(value);
                  // If boxed of either don't match unboxed, make sure to use unboxed
                  if (unboxedKey != key || unboxedValue != value) {
                     return convertEntry(unboxedKey, unboxedValue, entry);
                  }
                  return entry;
               });
      }

      @Override
      public boolean contains(Object o) {
         Map.Entry<K, V> entry = toEntry(o);
         if (entry != null) {
            return actualCollection.contains(entry);
         }
         return false;
      }

      @Override
      public boolean remove(Object o) {
         Map.Entry<K, V> entry = toEntry(o);
         if (entry != null) {
            return actualCollection.remove(entry);
         }
         return false;
      }

      Map.Entry toEntry(Object o) {
         if (o instanceof Map.Entry) {
            Map.Entry<K, V> entry = (Map.Entry<K, V>) o;
            K key = entry.getKey();
            K newKey = boxKey(key);
            V value = entry.getValue();
            V newValue = boxValue(value);
            if (key != newKey || value != newValue) {
               if (o instanceof CacheEntry) {
                  CacheEntry returned = (CacheEntry) o;
                  return convertEntry(newKey, newValue, returned);
               } else {
                  return entryFactory.create(newKey, newValue, (Metadata) null);
               }
            }
            return entry;
         }
         return null;
      }
   }

   @Override
   public CacheSet<Entry<K, V>> entrySet() {
      return cast(new TypeConverterEntrySet(this, cast(super.cacheEntrySet())));
   }

   @Override
   public CacheSet<CacheEntry<K, V>> cacheEntrySet() {
      return new TypeConverterEntrySet(this, super.cacheEntrySet());
   }

   final ConverterKeyMapper keyMapper = new ConverterKeyMapper();

   class TypeConverterKeySet extends AbstractCloseableIteratorCollection<K, K, V> implements CacheSet<K> {
      private final CacheSet<K> actualCollection;

      public TypeConverterKeySet(Cache<K, V> cache, CacheSet<K> actualCollection) {
         super(cache);
         this.actualCollection = actualCollection;
      }

      @Override
      public CacheStream<K> stream() {
         return actualCollection.stream().map(keyMapper);
      }

      @Override
      public CacheStream<K> parallelStream() {
         return actualCollection.parallelStream().map(keyMapper);
      }

      @Override
      public CloseableIterator<K> iterator() {
         return new CloseableIteratorMapper<>(actualCollection.iterator(), TypeConverterDelegatingAdvancedCache.this::unboxKey);
      }

      @Override
      public CloseableSpliterator<K> spliterator() {
         return new CloseableSpliteratorMapper<>(actualCollection.spliterator(), (InjectiveFunction & Function<K, K>)
               TypeConverterDelegatingAdvancedCache.this::unboxKey);
      }

      @Override
      public boolean contains(Object o) {
         return actualCollection.contains(boxKey((K) o));
      }

      @Override
      public boolean remove(Object o) {
         return actualCollection.remove(boxKey((K) o));
      }
   }

   @Override
   public CacheSet<K> keySet() {
      return new TypeConverterKeySet(this, super.keySet());
   }

   final ConverterValueMapper valueMapper = new ConverterValueMapper();

   class TypeConverterValuesCollection extends AbstractCloseableIteratorCollection<V, K, V> implements CacheCollection<V> {
      private final CacheCollection<V> actualCollection;

      public TypeConverterValuesCollection(Cache<K, V> cache, CacheCollection<V> actualCollection) {
         super(cache);
         this.actualCollection = actualCollection;
      }

      @Override
      public CacheStream<V> stream() {
         return actualCollection.stream().map(valueMapper);
      }

      @Override
      public CacheStream<V> parallelStream() {
         return actualCollection.parallelStream().map(valueMapper);
      }

      @Override
      public CloseableIterator<V> iterator() {
         return new CloseableIteratorMapper<>(actualCollection.iterator(), TypeConverterDelegatingAdvancedCache.this::unboxValue);
      }

      @Override
      public CloseableSpliterator<V> spliterator() {
         return new CloseableSpliteratorMapper<>(actualCollection.spliterator(), (InjectiveFunction & Function<V, V>)
               TypeConverterDelegatingAdvancedCache.this::unboxValue);
      }

      @Override
      public boolean contains(Object o) {
         return actualCollection.contains(boxValue((V) o));
      }

      @Override
      public boolean remove(Object o) {
         return actualCollection.remove(boxValue((V) o));
      }
   }

   @Override
   public CacheCollection<V> values() {
      return new TypeConverterValuesCollection(this, super.values());
   }

   @Override
   public Map<K, V> getGroup(String groupName) {
      Map<K, V> returned = super.getGroup(groupName);
      return unboxMap(returned);
   }

   @Override
   public CacheEntry<K, V> getCacheEntry(Object key) {
      K boxedKey = boxKey((K) key);
      CacheEntry<K, V> returned = super.getCacheEntry(boxedKey);
      if (returned != null) {
         V originalValue = returned.getValue();
         V unboxedValue = unboxValue(originalValue);
         // If boxed of either don't match unboxed, make sure to use unboxed
         if (boxedKey != key || unboxedValue != originalValue) {
            return convertEntry((K) key, unboxedValue, returned);
         }
      }
      return returned;
   }

   @Override
   public Map<K, V> getAll(Set<?> keys) {
      Map<K, V> returned = super.getAll(convertKeys(keys));
      return unboxMap(returned);
   }

   @Override
   public Map<K, CacheEntry<K, V>> getAllCacheEntries(Set<?> keys) {
      Map<K, CacheEntry<K, V>> returned = super.getAllCacheEntries(convertKeys(keys));
      return unboxEntryMap(returned);
   }

   @Override
   public void forEach(BiConsumer<? super K, ? super V> action) {
      super.forEach((k, v) -> {
         K newK = unboxKey(k);
         V newV = unboxValue(v);
         action.accept(newK, newV);
      });
   }

   @Override
   public V getOrDefault(Object key, V defaultValue) {
      V returned = super.getOrDefault(boxKey((K) key), defaultValue);
      if (returned == defaultValue) {
         return returned;
      }
      return unboxValue(returned);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      V returned = super.merge(boxKey(key), value, (oldV, newV) -> {
         // oldV will be boxed, however we left newV alone
         V oldVUnboxed = unboxValue(oldV);
         return remappingFunction.apply(oldVUnboxed, newV);
      });
      return unboxValue(returned);
   }

   @Override
   public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
      super.replaceAll(convertFunction(function));
   }

   @Override
   public AdvancedCache<K, V> with(ClassLoader classLoader) {
      AdvancedCache<K, V> returned = super.with(classLoader);
      if (returned != this && returned instanceof TypeConverterDelegatingAdvancedCache) {
         ((TypeConverterDelegatingAdvancedCache) returned).entryFactory = this.entryFactory;
      }
      return returned;
   }

   @Override
   public AdvancedCache<K, V> withFlags(Flag... flags) {
      AdvancedCache<K, V> returned = super.withFlags(flags);
      if (returned != this && returned instanceof TypeConverterDelegatingAdvancedCache) {
         ((TypeConverterDelegatingAdvancedCache) returned).entryFactory = this.entryFactory;
      }
      return returned;
   }
}
