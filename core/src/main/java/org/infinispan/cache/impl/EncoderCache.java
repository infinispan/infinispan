package org.infinispan.cache.impl;

import static org.infinispan.util.logging.Log.CONTAINER;

import java.lang.annotation.Annotation;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheCollection;
import org.infinispan.CachePublisher;
import org.infinispan.CacheSet;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.util.InjectiveFunction;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ForwardingCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.ListenerHolder;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.reactive.publisher.impl.SegmentPublisherSupplier;
import org.infinispan.util.WriteableCacheCollectionMapper;
import org.infinispan.util.WriteableCacheSetMapper;
import org.infinispan.util.function.SerializableBiFunction;
import org.infinispan.util.function.SerializableFunction;
import org.reactivestreams.Publisher;

/**
 * Cache decoration that makes use of the {@link Encoder} and {@link Wrapper} to convert between storage value and
 * read/write value.
 *
 * @since 9.1
 */
@Scope(Scopes.NAMED_CACHE)
public class EncoderCache<K, V> extends AbstractDelegatingAdvancedCache<K, V> {
   // InternalCacheFactory.buildEncodingCache doesn't have a component registry to pass to the constructor.
   // We inject these after the component registry has been created,
   // and every other caller of the constructor passes non-null values.
   @Inject InternalEntryFactory entryFactory;
   @Inject BasicComponentRegistry componentRegistry;

   private final DataConversion keyDataConversion;
   private final DataConversion valueDataConversion;

   private final Function<V, V> decodedValueForRead = this::valueFromStorage;

   public EncoderCache(AdvancedCache<K, V> cache, InternalEntryFactory entryFactory,
                       BasicComponentRegistry componentRegistry,
                       DataConversion keyDataConversion, DataConversion valueDataConversion) {
      super(cache);
      this.entryFactory = entryFactory;
      this.componentRegistry = componentRegistry;
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   @Override
   public AdvancedCache rewrap(AdvancedCache newDelegate) {
      return new EncoderCache(newDelegate, entryFactory, componentRegistry, keyDataConversion, valueDataConversion);
   }

   private Set<?> encodeKeysForWrite(Set<?> keys) {
      if (needsEncoding(keys)) {
         return keys.stream().map(this::keyToStorage).collect(Collectors.toCollection(LinkedHashSet::new));
      }
      return keys;
   }

   private boolean needsEncoding(Collection<?> keys) {
      return keys.stream().anyMatch(k -> !k.equals(keyToStorage(k)));
   }

   private Collection<? extends K> encodeKeysForWrite(Collection<? extends K> keys) {
      if (needsEncoding(keys)) {
         return keys.stream().map(this::keyToStorage).collect(Collectors.toCollection(ArrayList::new));
      }
      return keys;
   }

   public K keyToStorage(Object key) {
      return (K) keyDataConversion.toStorage(key);
   }

   public V valueToStorage(Object value) {
      return (V) valueDataConversion.toStorage(value);
   }

   public K keyFromStorage(Object key) {
      return (K) keyDataConversion.fromStorage(key);
   }

   public V valueFromStorage(Object value) {
      return (V) valueDataConversion.fromStorage(value);
   }

   @Inject
   public void wireRealCache() {
      componentRegistry.wireDependencies(keyDataConversion, false);
      componentRegistry.wireDependencies(valueDataConversion, false);
      componentRegistry.wireDependencies(cache, false);
   }

   private Map<K, V> encodeMapForWrite(Map<? extends K, ? extends V> map) {
      Map<K, V> newMap = new HashMap<>(map.size());
      map.forEach((k, v) -> newMap.put(keyToStorage(k), valueToStorage(v)));
      return newMap;
   }

   private Map<K, V> decodeMapForRead(Map<? extends K, ? extends V> map) {
      Map<K, V> newMap = new LinkedHashMap<>(map.size());
      map.forEach((k, v) -> newMap.put(keyFromStorage(k), valueFromStorage(v)));
      return newMap;
   }

   private CacheEntry<K, V> convertEntry(K newKey, V newValue, CacheEntry<K, V> entry) {
      if (entry instanceof InternalCacheEntry) {
         return entryFactory.create(newKey, newValue, (InternalCacheEntry) entry);
      } else {
         return entryFactory.create(newKey, newValue, entry.getMetadata().version(), entry.getCreated(),
               entry.getLifespan(), entry.getLastUsed(), entry.getMaxIdle());
      }
   }

   private BiFunction<? super K, ? super V, ? extends V> convertFunction(
         BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return (k, v) -> valueToStorage(remappingFunction.apply(keyFromStorage(k), valueFromStorage(v)));
   }

   private Map<K, CacheEntry<K, V>> decodeEntryMapForRead(Map<K, CacheEntry<K, V>> map) {
      Map<K, CacheEntry<K, V>> entryMap = new HashMap<>(map.size());
      map.forEach((k, v) -> {
         K unwrappedKey = keyFromStorage(k);
         V originalValue = v.getValue();
         V unwrappedValue = valueFromStorage(originalValue);
         CacheEntry<K, V> entryToPut;
         if (unwrappedKey != k || unwrappedValue != originalValue) {
            entryToPut = convertEntry(unwrappedKey, unwrappedValue, v);
         } else {
            entryToPut = v;
         }
         entryMap.put(unwrappedKey, entryToPut);
      });
      return entryMap;
   }

   @Override
   public void putForExternalRead(K key, V value) {
      cache.putForExternalRead(keyToStorage(key), valueToStorage(value));
   }

   @Override
   public void putForExternalRead(K key, V value, long lifespan, TimeUnit unit) {
      cache.putForExternalRead(keyToStorage(key), valueToStorage(value), lifespan, unit);
   }

   @Override
   public void putForExternalRead(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      cache.putForExternalRead(keyToStorage(key), valueToStorage(value), lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public void evict(K key) {
      cache.evict(keyToStorage(key));
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit unit) {
      V ret = cache.put(keyToStorage(key), valueToStorage(value), lifespan, unit);
      return valueFromStorage(ret);
   }

   @Override
   public DataConversion getKeyDataConversion() {
      return keyDataConversion;
   }

   @Override
   public DataConversion getValueDataConversion() {
      return valueDataConversion;
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit unit) {
      V v = cache.putIfAbsent(keyToStorage(key), valueToStorage(value), lifespan, unit);
      return valueFromStorage(v);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit) {
      cache.putAll(encodeMapForWrite(map), lifespan, unit);
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit unit) {
      V ret = cache.replace(keyToStorage(key), valueToStorage(value), lifespan, unit);
      return valueFromStorage(ret);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit) {
      return cache.replace(keyToStorage(key), valueToStorage(oldValue), valueToStorage(value), lifespan, unit);
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      V ret = cache.put(keyToStorage(key), valueToStorage(value), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return valueFromStorage(ret);
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      V ret = cache.putIfAbsent(keyToStorage(key), valueToStorage(value), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return valueFromStorage(ret);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      cache.putAll(encodeMapForWrite(map), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }


   @Override
   public V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      V ret = cache.replace(keyToStorage(key), valueToStorage(value), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return valueFromStorage(ret);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return cache.replace(keyToStorage(key), valueToStorage(oldValue), valueToStorage(value), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
      cache.replaceAll(convertFunction(function));
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value) {
      return cache.putAsync(keyToStorage(key), valueToStorage(value)).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit unit) {
      return cache.putAsync(keyToStorage(key), valueToStorage(value), lifespan, unit).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.putAsync(keyToStorage(key), valueToStorage(value), lifespan, lifespanUnit, maxIdle, maxIdleUnit).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data) {
      return cache.putAllAsync(encodeMapForWrite(data));
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit) {
      return cache.putAllAsync(encodeMapForWrite(data), lifespan, unit);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.putAllAsync(encodeMapForWrite(data), lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> map, Metadata metadata) {
      return cache.putAllAsync(encodeMapForWrite(map), metadata);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value) {
      return cache.putIfAbsentAsync(keyToStorage(key), valueToStorage(value)).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit unit) {
      return cache.putIfAbsentAsync(keyToStorage(key), valueToStorage(value), lifespan, unit).thenApply(decodedValueForRead);
   }

   @Override
   public boolean lock(K... keys) {
      K[] encoded = (K[]) Arrays.stream(keys).map(this::keyToStorage).toArray();
      return cache.lock(encoded);
   }

   @Override
   public boolean lock(Collection<? extends K> keys) {
      return cache.lock(encodeKeysForWrite(keys));
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.putIfAbsentAsync(keyToStorage(key), valueToStorage(value), lifespan, lifespanUnit, maxIdle, maxIdleUnit).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, Metadata metadata) {
      return cache.putIfAbsentAsync(keyToStorage(key), valueToStorage(value), metadata).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> putIfAbsentAsyncEntry(K key, V value, Metadata metadata) {
      K keyToStorage = keyToStorage(key);
      return cache.putIfAbsentAsyncEntry(keyToStorage, valueToStorage(value), metadata)
            .thenApply(e -> unwrapCacheEntry(key, keyToStorage, e));
   }

   @Override
   public CompletableFuture<V> removeAsync(Object key) {
      return cache.removeAsync(keyToStorage(key)).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<Boolean> removeAsync(Object key, Object value) {
      return cache.removeAsync(keyToStorage(key), valueToStorage(value));
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value) {
      return cache.replaceAsync(keyToStorage(key), valueToStorage(value)).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit unit) {
      return cache.replaceAsync(keyToStorage(key), valueToStorage(value), lifespan, unit).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, Metadata metadata) {
      return cache.replaceAsync(keyToStorage(key), valueToStorage(value), metadata).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> replaceAsyncEntry(K key, V value, Metadata metadata) {
      K keyToStorage = keyToStorage(key);
      return cache.replaceAsyncEntry(keyToStorage, valueToStorage(value), metadata)
            .thenApply(e -> unwrapCacheEntry(key, keyToStorage, e));
   }

   @Override
   public Map<K, V> getAll(Set<?> keys) {
      Map<K, V> ret = cache.getAll(encodeKeysForWrite(keys));
      return decodeMapForRead(ret);
   }

   @Override
   public CompletableFuture<Map<K, V>> getAllAsync(Set<?> keys) {
      return cache.getAllAsync(encodeKeysForWrite(keys)).thenApply(this::decodeMapForRead);
   }

   @Override
   public CacheEntry<K, V> getCacheEntry(Object key) {
      K keyToStorage = keyToStorage(key);
      CacheEntry<K, V> returned = cache.getCacheEntry(keyToStorage);
      return unwrapCacheEntry(key, keyToStorage, returned);
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> getCacheEntryAsync(Object key) {
      K keyToStorage = keyToStorage(key);
      CompletableFuture<CacheEntry<K, V>> stage = cache.getCacheEntryAsync(keyToStorage);
      if (stage.isDone() && !stage.isCompletedExceptionally()) {
         CacheEntry<K, V> result = stage.join();
         CacheEntry<K, V> unwrapped = unwrapCacheEntry(key, keyToStorage, result);
         if (result == unwrapped) {
            return stage;
         }

         return CompletableFuture.completedFuture(unwrapped);
      }
      return stage.thenApply(returned -> unwrapCacheEntry(key, keyToStorage, returned));
   }

   private CacheEntry<K, V> unwrapCacheEntry(Object key, K keyToStorage, CacheEntry<K, V> returned) {
      if (returned != null) {
         V originalValue = returned.getValue();
         V valueFromStorage = valueFromStorage(originalValue);
         if (keyToStorage != key || valueFromStorage != originalValue) {
            return convertEntry((K) key, valueFromStorage, returned);
         }
      }
      return returned;
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.replaceAsync(keyToStorage(key), valueToStorage(value), lifespan, lifespanUnit, maxIdle, maxIdleUnit)
            .thenApply(decodedValueForRead);
   }

   @Override
   public Map<K, CacheEntry<K, V>> getAllCacheEntries(Set<?> keys) {
      Map<K, CacheEntry<K, V>> returned = cache.getAllCacheEntries(encodeKeysForWrite(keys));
      return decodeEntryMapForRead(returned);
   }

   @Override
   public Map<K, V> getGroup(String groupName) {
      Map<K, V> ret = cache.getGroup(groupName);
      return decodeMapForRead(ret);
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
      return cache.replaceAsync(keyToStorage(key), valueToStorage(oldValue), valueToStorage(newValue));
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit) {
      return cache.replaceAsync(keyToStorage(key), valueToStorage(oldValue), valueToStorage(newValue), lifespan, unit);
   }

   @Override
   public V put(K key, V value, Metadata metadata) {
      V ret = cache.put(keyToStorage(key), valueToStorage(value), metadata);
      return valueFromStorage(ret);
   }

   @Override
   public V replace(K key, V value, Metadata metadata) {
      V ret = cache.replace(keyToStorage(key), valueToStorage(value), metadata);
      return valueFromStorage(ret);
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.replaceAsync(keyToStorage(key), valueToStorage(oldValue), valueToStorage(newValue), lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, Metadata metadata) {
      return cache.replace(keyToStorage(key), valueToStorage(oldValue), valueToStorage(value), metadata);
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, Metadata metadata) {
      return cache.replaceAsync(keyToStorage(key), valueToStorage(oldValue), valueToStorage(newValue), metadata);
   }

   @Override
   public V putIfAbsent(K key, V value, Metadata metadata) {
      V ret = cache.putIfAbsent(keyToStorage(key), valueToStorage(value), metadata);
      return valueFromStorage(ret);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, Metadata metadata) {
      return cache.putAsync(keyToStorage(key), valueToStorage(value), metadata).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> putAsyncEntry(K key, V value, Metadata metadata) {
      K keyToStorage = keyToStorage(key);
      return cache.putAsyncEntry(keyToStorage, valueToStorage(value), metadata)
            .thenApply(e -> unwrapCacheEntry(key, keyToStorage, e));
   }

   @Override
   public void putForExternalRead(K key, V value, Metadata metadata) {
      cache.putForExternalRead(keyToStorage(key), valueToStorage(value), metadata);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, Metadata metadata) {
      cache.putAll(encodeMapForWrite(map), metadata);
   }

   @Override
   public CacheSet<CacheEntry<K, V>> cacheEntrySet() {
      EncoderEntryMapper<K, V, CacheEntry<K, V>> cacheEntryMapper = EncoderEntryMapper.newCacheEntryMapper(
            keyDataConversion, valueDataConversion, entryFactory);
      return new WriteableCacheSetMapper<>(cache.cacheEntrySet(), cacheEntryMapper,
            e -> new CacheEntryWrapper<>(e, cacheEntryMapper.apply(e)), this::toCacheEntry, this::keyToStorage);
   }

   @Override
   public CompletableFuture<Boolean> removeLifespanExpired(K key, V value, Long lifespan) {
      return cache.removeLifespanExpired(keyToStorage(key), valueToStorage(value), lifespan);
   }

   @Override
   public CompletableFuture<Boolean> removeMaxIdleExpired(K key, V value) {
      return cache.removeMaxIdleExpired(keyToStorage(key), valueToStorage(value));
   }

   @Override
   public V putIfAbsent(K key, V value) {
      V ret = cache.putIfAbsent(keyToStorage(key), valueToStorage(value));
      return valueFromStorage(ret);
   }

   private void lookupEncoderWrapper() {
      componentRegistry.wireDependencies(keyDataConversion, true);
      componentRegistry.wireDependencies(valueDataConversion, true);
   }

   @Override
   public AdvancedCache<K, V> withEncoding(Class<? extends Encoder> keyEncoderClass, Class<? extends Encoder> valueEncoderClass) {
      checkSubclass(keyEncoderClass, Encoder.class);
      checkSubclass(valueEncoderClass, Encoder.class);

      DataConversion newKeyDataConversion = keyDataConversion.withEncoding(keyEncoderClass);
      DataConversion newValueDataConversion = valueDataConversion.withEncoding(valueEncoderClass);
      EncoderCache<K, V> encoderCache = new EncoderCache<>(cache, entryFactory, componentRegistry,
                                                           newKeyDataConversion, newValueDataConversion);
      encoderCache.lookupEncoderWrapper();
      return encoderCache;
   }

   @Override
   public AdvancedCache<K, V> withEncoding(Class<? extends Encoder> encoderClass) {
      checkSubclass(encoderClass, Encoder.class);

      DataConversion newKeyDataConversion = keyDataConversion.withEncoding(encoderClass);
      DataConversion newValueDataConversion = valueDataConversion.withEncoding(encoderClass);
      EncoderCache<K, V> encoderCache = new EncoderCache<>(cache, entryFactory, componentRegistry,
                                                           newKeyDataConversion, newValueDataConversion);
      encoderCache.lookupEncoderWrapper();
      return encoderCache;
   }

   @Override
   public AdvancedCache<K, V> withKeyEncoding(Class<? extends Encoder> encoderClass) {
      checkSubclass(encoderClass, Encoder.class);

      DataConversion newKeyDataConversion = keyDataConversion.withEncoding(encoderClass);
      EncoderCache<K, V> encoderCache = new EncoderCache<>(cache, entryFactory, componentRegistry,
                                                           newKeyDataConversion, valueDataConversion);
      encoderCache.lookupEncoderWrapper();
      return encoderCache;
   }

   private void checkSubclass(Class<?> configured, Class<?> required) {
      if (!required.isAssignableFrom(configured)) {
         throw CONTAINER.invalidEncodingClass(configured, required);
      }
   }

   @Override
   public AdvancedCache<K, V> withMediaType(String keyMediaType, String valueMediaType) {
      MediaType kType = MediaType.fromString(keyMediaType);
      MediaType vType = MediaType.fromString(valueMediaType);
      return withMediaType(kType, vType);
   }

   @Override
   public AdvancedCache<K, V> withMediaType(MediaType kType, MediaType vType) {
      DataConversion newKeyDataConversion = keyDataConversion.withRequestMediaType(kType);
      DataConversion newValueDataConversion = valueDataConversion.withRequestMediaType(vType);
      EncoderCache<K, V> encoderCache = new EncoderCache<>(cache, entryFactory, componentRegistry,
            newKeyDataConversion, newValueDataConversion);
      encoderCache.lookupEncoderWrapper();
      return encoderCache;
   }

   @Override
   public AdvancedCache<K, V> withStorageMediaType() {
      MediaType keyStorageMediaType = keyDataConversion.getStorageMediaType();
      MediaType valueStorageMediaType = valueDataConversion.getStorageMediaType();
      return withMediaType(keyStorageMediaType, valueStorageMediaType);
   }

   @Override
   public boolean remove(Object key, Object value) {
      return cache.remove(keyToStorage(key), valueToStorage(value));
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue) {
      return cache.replace(keyToStorage(key), valueToStorage(oldValue), valueToStorage(newValue));
   }

   @Override
   public V replace(K key, V value) {
      V ret = cache.replace(keyToStorage(key), valueToStorage(value));
      return valueFromStorage(ret);
   }

   @Override
   public boolean containsKey(Object key) {
      return cache.containsKey(keyToStorage(key));
   }

   @Override
   public boolean containsValue(Object value) {
      return cache.containsValue(valueToStorage(value));
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      Object returned = cache.compute(keyToStorage(key), wrapBiFunction(remappingFunction));
      return valueFromStorage(returned);
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      Object returned = cache.compute(keyToStorage(key), wrapBiFunction(remappingFunction), metadata);
      return valueFromStorage(returned);
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      Object returned = cache.compute(keyToStorage(key), wrapBiFunction(remappingFunction), lifespan, lifespanUnit);
      return valueFromStorage(returned);
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      Object returned = cache.compute(keyToStorage(key), wrapBiFunction(remappingFunction), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return valueFromStorage(returned);
   }

   @Override
   public V compute(K key, SerializableBiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      Object ret = cache.compute(keyToStorage(key), wrapBiFunction(remappingFunction), metadata);
      return valueFromStorage(ret);
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      Object returned = cache.computeIfPresent(keyToStorage(key), wrapBiFunction(remappingFunction));
      return valueFromStorage(returned);
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      Object returned = cache.computeIfPresent(keyToStorage(key), wrapBiFunction(remappingFunction), metadata);
      return valueFromStorage(returned);
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      Object returned = cache.computeIfPresent(keyToStorage(key), wrapBiFunction(remappingFunction), lifespan, lifespanUnit);
      return valueFromStorage(returned);
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      Object returned = cache.computeIfPresent(keyToStorage(key), wrapBiFunction(remappingFunction), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return valueFromStorage(returned);
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
      Object ret = cache.computeIfAbsent(keyToStorage(key), wrapFunction(mappingFunction));
      return valueFromStorage(ret);
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction, Metadata metadata) {
      Object ret = cache.computeIfAbsent(keyToStorage(key), wrapFunction(mappingFunction), metadata);
      return valueFromStorage(ret);
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit) {
      Object ret = cache.computeIfAbsent(keyToStorage(key), wrapFunction(mappingFunction), lifespan, lifespanUnit);
      return valueFromStorage(ret);
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      Object ret = cache.computeIfAbsent(keyToStorage(key), wrapFunction(mappingFunction), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return valueFromStorage(ret);
   }

   @Override
   public V computeIfAbsent(K key, SerializableFunction<? super K, ? extends V> mappingFunction, Metadata metadata) {
      Object ret = cache.computeIfAbsent(keyToStorage(key), wrapFunction(mappingFunction), metadata);
      return valueFromStorage(ret);
   }

   @Override
   public V merge(K key, V value, SerializableBiFunction<? super V, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      Object ret = cache.merge(keyToStorage(key), valueToStorage(value), wrapBiFunction(remappingFunction), metadata);
      return valueFromStorage(ret);
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, SerializableBiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return cache.computeAsync(keyToStorage(key), wrapBiFunction(remappingFunction), metadata).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, SerializableFunction<? super K, ? extends V> mappingFunction, Metadata metadata) {
      return cache.computeIfAbsentAsync(keyToStorage(key), wrapFunction(mappingFunction), metadata).thenApply(decodedValueForRead);
   }

   @Override
   public V get(Object key) {
      V v = cache.get(keyToStorage(key));
      return valueFromStorage(v);
   }

   @Override
   public V getOrDefault(Object key, V defaultValue) {
      V returned = cache.getOrDefault(keyToStorage(key), defaultValue);
      if (returned == defaultValue) {
         return returned;
      }
      return valueFromStorage(returned);
   }

   @Override
   public V put(K key, V value) {
      V ret = cache.put(keyToStorage(key), valueToStorage(value));
      if (ret == null) {
         return null;
      }
      return valueFromStorage(ret);
   }

   @Override
   public V remove(Object key) {
      V ret = cache.remove(keyToStorage(key));
      return valueFromStorage(ret);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> t) {
      cache.putAll(encodeMapForWrite(t));
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      Object returned = cache.merge(keyToStorage(key), valueToStorage(value), wrapBiFunction(remappingFunction));
      return valueFromStorage(returned);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      Object returned = cache.merge(keyToStorage(key), valueToStorage(value), wrapBiFunction(remappingFunction), metadata);
      return valueFromStorage(returned);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      Object returned = cache.merge(keyToStorage(key), valueToStorage(value), wrapBiFunction(remappingFunction), lifespan, lifespanUnit);
      return valueFromStorage(returned);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      Object returned = cache.merge(keyToStorage(key), valueToStorage(value), wrapBiFunction(remappingFunction), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return valueFromStorage(returned);
   }

   @Override
   public V computeIfPresent(K key, SerializableBiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      Object returned = cache.computeIfPresent(keyToStorage(key), wrapBiFunction(remappingFunction), metadata);
      return valueFromStorage(returned);
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, SerializableBiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return cache.computeIfPresentAsync(keyToStorage(key), wrapBiFunction(remappingFunction), metadata).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return cache.computeIfPresentAsync(keyToStorage(key), wrapBiFunction(remappingFunction), metadata).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return cache.computeIfPresentAsync(keyToStorage(key), wrapBiFunction(remappingFunction)).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return cache.computeIfPresentAsync(keyToStorage(key), wrapBiFunction(remappingFunction), lifespan, lifespanUnit).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.computeIfPresentAsync(keyToStorage(key), wrapBiFunction(remappingFunction), lifespan, lifespanUnit, maxIdle, maxIdleUnit).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, SerializableBiFunction<? super V, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return cache.mergeAsync(keyToStorage(key), valueToStorage(value), wrapBiFunction(remappingFunction), metadata).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return cache.mergeAsync(keyToStorage(key), valueToStorage(value), wrapBiFunction(remappingFunction), lifespan, lifespanUnit).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return cache.mergeAsync(keyToStorage(key), valueToStorage(value), wrapBiFunction(remappingFunction), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return cache.mergeAsync(keyToStorage(key), valueToStorage(value), wrapBiFunction(remappingFunction), metadata).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      return cache.mergeAsync(keyToStorage(key), valueToStorage(value), wrapBiFunction(remappingFunction)).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, Metadata metadata) {
      return cache.computeAsync(keyToStorage(key), wrapBiFunction(remappingFunction), metadata).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction, Metadata metadata) {
      return cache.computeIfAbsentAsync(keyToStorage(key), wrapFunction(mappingFunction), metadata).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return cache.computeAsync(keyToStorage(key), wrapBiFunction(remappingFunction)).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return cache.computeAsync(keyToStorage(key), wrapBiFunction(remappingFunction), lifespan, lifespanUnit).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.computeAsync(keyToStorage(key), wrapBiFunction(remappingFunction), lifespan, lifespanUnit, maxIdle, maxIdleUnit).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction) {
      return cache.computeIfAbsentAsync(keyToStorage(key), wrapFunction(mappingFunction)).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return cache.computeIfAbsentAsync(keyToStorage(key), wrapFunction(mappingFunction), lifespan, lifespanUnit).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.computeIfAbsentAsync(keyToStorage(key), wrapFunction(mappingFunction), lifespan, lifespanUnit, maxIdle, maxIdleUnit).thenApply(decodedValueForRead);
   }

   @Override
   public void forEach(BiConsumer<? super K, ? super V> action) {
      cache.forEach((k, v) -> {
         K newK = keyFromStorage(k);
         V newV = valueFromStorage(v);
         action.accept(newK, newV);
      });
   }

   @Override
   public CacheSet<K> keySet() {
      InjectiveFunction<Object, K> keyToStorage = this::keyToStorage;
      return new WriteableCacheSetMapper<>(cache.keySet(), new EncoderKeyMapper<>(keyDataConversion),
            keyToStorage, keyToStorage);
   }

   @Override
   public CacheSet<Map.Entry<K, V>> entrySet() {
      EncoderEntryMapper<K, V, Map.Entry<K, V>> entryMapper = EncoderEntryMapper.newEntryMapper(keyDataConversion,
            valueDataConversion, entryFactory);
      return new WriteableCacheSetMapper<>(cache.entrySet(), entryMapper,
            e -> new EntryWrapper<>(e, entryMapper.apply(e)), this::toEntry, this::keyToStorage);
   }


   @Override
   public CompletionStage<Boolean> touch(Object key, boolean touchEvenIfExpired) {
      return super.touch(keyToStorage(key), touchEvenIfExpired);
   }

   @Override
   public CompletionStage<Boolean> touch(Object key, int segment, boolean touchEvenIfExpired) {
      return super.touch(keyToStorage(key), segment, touchEvenIfExpired);
   }

   Map.Entry<K, V> toEntry(Object o) {
      if (o instanceof Map.Entry) {
         Map.Entry<K, V> entry = (Map.Entry<K, V>) o;
         K key = entry.getKey();
         K newKey = keyToStorage(key);
         V value = entry.getValue();
         V newValue = valueToStorage(value);
         if (key != newKey || value != newValue) {
            return new AbstractMap.SimpleEntry<>(newKey, newValue);
         }
         return entry;
      }
      return null;
   }

   CacheEntry<K, V> toCacheEntry(Object o) {
      if (o instanceof Map.Entry) {
         Map.Entry<K, V> entry = (Map.Entry<K, V>) o;
         K key = entry.getKey();
         K newKey = keyToStorage(key);
         V value = entry.getValue();
         V newValue = valueToStorage(value);
         if (key != newKey || value != newValue) {
            if (o instanceof CacheEntry returned) {
               return convertEntry(newKey, newValue, returned);
            } else {
               return entryFactory.create(newKey, newValue, (Metadata) null);
            }
         }
         if (entry instanceof CacheEntry) {
            return (CacheEntry<K, V>) entry;
         } else {
            return entryFactory.create(key, value, (Metadata) null);
         }
      }
      return null;
   }

   @Override
   public CacheCollection<V> values() {
      return new WriteableCacheCollectionMapper<>(cache.values(), new EncoderValueMapper<>(valueDataConversion),
            this::valueToStorage, this::keyToStorage);
   }

   private class EntryWrapper<A, B> implements Entry<A, B> {
      private final Entry<A, B> storageEntry;
      private final Entry<A, B> entry;

      EntryWrapper(Entry<A, B> storageEntry, Entry<A, B> entry) {
         this.storageEntry = storageEntry;
         this.entry = entry;
      }

      @Override
      public A getKey() {
         return entry.getKey();
      }

      @Override
      public B getValue() {
         return entry.getValue();
      }

      @Override
      public B setValue(B value) {
         storageEntry.setValue((B) valueToStorage(value));
         return entry.setValue(value);
      }

      @Override
      public String toString() {
         return "EntryWrapper{" +
               "key=" + entry.getKey() +
               ", value=" + entry.getValue() +
               "}";
      }
   }

   private class CacheEntryWrapper<A, B> extends ForwardingCacheEntry<A, B> {
      private final CacheEntry<A, B> previousEntry;
      private final CacheEntry<A, B> entry;

      CacheEntryWrapper(CacheEntry<A, B> previousEntry, CacheEntry<A, B> entry) {
         this.previousEntry = previousEntry;
         this.entry = entry;
      }

      @Override
      protected CacheEntry<A, B> delegate() {
         return entry;
      }

      @Override
      public B setValue(B value) {
         previousEntry.setValue((B) valueToStorage(value));
         return super.setValue(value);
      }
   }

   class CachePublisherWrapper<A, B> implements CachePublisher<A, B> {
      private final CachePublisher<A, B> delegate;

      private CachePublisherWrapper(CachePublisher<A, B> delegate) {
         this.delegate = delegate;
      }

      @Override
      public CachePublisher<A, B> parallelReduction() {
         CachePublisher<A, B> p = delegate.parallelReduction();
         if (p != delegate) {
            return new CachePublisherWrapper<>(p);
         }
         return this;
      }

      @Override
      public CachePublisher<A, B> sequentialReduction() {
         CachePublisher<A, B> p = delegate.sequentialReduction();
         if (p != delegate) {
            return new CachePublisherWrapper<>(p);
         }
         return this;
      }

      @Override
      public CachePublisher<A, B> batchSize(int batchSize) {
         CachePublisher<A, B> p = delegate.batchSize(batchSize);
         if (p != delegate) {
            return new CachePublisherWrapper<>(p);
         }
         return this;
      }

      @Override
      public CachePublisher<A, B> withKeys(Set<? extends A> keys) {
         CachePublisher<A, B> p = delegate.withKeys(keys);
         if (p != delegate) {
            return new CachePublisherWrapper<>(p);
         }
         return this;
      }

      @Override
      public CachePublisher<A, B> withAllKeys() {
         CachePublisher<A, B> p = delegate.withAllKeys();
         if (p != delegate) {
            return new CachePublisherWrapper<>(p);
         }
         return this;
      }

      @Override
      public CachePublisher<A, B> withSegments(IntSet segments) {
         CachePublisher<A, B> p = delegate.withSegments(segments);
         if (p != delegate) {
            return new CachePublisherWrapper<>(p);
         }
         return this;
      }

      @Override
      public CachePublisher<A, B> withAllSegments() {
         CachePublisher<A, B> p = delegate.withAllSegments();
         if (p != delegate) {
            return new CachePublisherWrapper<>(p);
         }
         return this;
      }

      @Override
      public CachePublisher<A, B> atMostOnce() {
         CachePublisher<A, B> p = delegate.atMostOnce();
         if (p != delegate) {
            return new CachePublisherWrapper<>(p);
         }
         return this;
      }

      @Override
      public CachePublisher<A, B> atLeastOnce() {
         CachePublisher<A, B> p = delegate.atLeastOnce();
         if (p != delegate) {
            return new CachePublisherWrapper<>(p);
         }
         return this;
      }

      @Override
      public CachePublisher<A, B> exactlyOnce() {
         CachePublisher<A, B> p = delegate.exactlyOnce();
         if (p != delegate) {
            return new CachePublisherWrapper<>(p);
         }
         return this;
      }

      @Override
      public <R> CompletionStage<R> keyReduction(Function<? super Publisher<A>, ? extends CompletionStage<R>> transformer, Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
         Function<Publisher<A>, CompletionStage<R>> castFunc = (Function<Publisher<A>, CompletionStage<R>>) transformer;
         return delegate.keyReduction(new KeyFunctionEncoder<>(castFunc, keyDataConversion), finalizer);
      }

      @Override
      public <R> CompletionStage<R> keyReduction(SerializableFunction<? super Publisher<A>, ? extends CompletionStage<R>> transformer, SerializableFunction<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
         return keyReduction((Function<? super Publisher<A>, ? extends CompletionStage<R>>) transformer, finalizer);
      }

      @Override
      public <R> CompletionStage<R> entryReduction(Function<? super Publisher<CacheEntry<A, B>>, ? extends CompletionStage<R>> transformer, Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
         Function<Publisher<CacheEntry<A, B>>, CompletionStage<R>> castFunc = (Function<Publisher<CacheEntry<A, B>>, CompletionStage<R>>) transformer;
         return delegate.entryReduction(new EntryFunctionEncoder<>(castFunc,
               EncoderEntryMapper.newCacheEntryMapper(keyDataConversion, valueDataConversion, entryFactory)), finalizer);
      }

      @Override
      public <R> CompletionStage<R> entryReduction(SerializableFunction<? super Publisher<CacheEntry<A, B>>, ? extends CompletionStage<R>> transformer, SerializableFunction<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
         return entryReduction((Function<? super Publisher<CacheEntry<A,B>>, ? extends CompletionStage<R>>) transformer, finalizer);
      }

      @Override
      public <R> SegmentPublisherSupplier<R> keyPublisher(Function<? super Publisher<A>, ? extends Publisher<R>> transformer) {
         Function<Publisher<A>, Publisher<R>> castFunc = (Function<Publisher<A>, Publisher<R>>) transformer;
         return delegate.keyPublisher(new KeyFunctionEncoder<>(castFunc, keyDataConversion));
      }

      @Override
      public <R> SegmentPublisherSupplier<R> keyPublisher(SerializableFunction<? super Publisher<A>, ? extends Publisher<R>> transformer) {
         return keyPublisher((Function<? super Publisher<A>, ? extends Publisher<R>>) transformer);
      }

      @Override
      public <R> SegmentPublisherSupplier<R> entryPublisher(Function<? super Publisher<CacheEntry<A, B>>, ? extends Publisher<R>> transformer) {
         Function<Publisher<CacheEntry<A, B>>, Publisher<R>> castFunc = (Function<Publisher<CacheEntry<A, B>>, Publisher<R>>) transformer;
         return delegate.entryPublisher(new EntryFunctionEncoder<>(castFunc,
               EncoderEntryMapper.newCacheEntryMapper(keyDataConversion, valueDataConversion, entryFactory)));
      }

      @Override
      public <R> SegmentPublisherSupplier<R> entryPublisher(SerializableFunction<? super Publisher<CacheEntry<A, B>>, ? extends Publisher<R>> transformer) {
         return entryPublisher((Function<? super Publisher<CacheEntry<A,B>>, ? extends Publisher<R>>) transformer);
      }
   }

   @Override
   public CompletableFuture<V> getAsync(K key) {
      CompletableFuture<V> future = cache.getAsync(keyToStorage(key));
      if (future.isDone() && CompletionStages.isCompletedSuccessfully(future)) {
         V value = future.join();
         V wrapped = valueFromStorage(value);
         if (value == wrapped) {
            return future;
         }
         return CompletableFuture.completedFuture(wrapped);
      }
      return future.thenApply(decodedValueForRead);
   }

   @Override
   public void addListener(Object listener) {
      CompletionStages.join(addListenerAsync(listener));
   }

   @Override
   public CompletionStage<Void> addListenerAsync(Object listener) {
      Cache unwrapped = unwrapCache(this.cache);
      if (unwrapped instanceof CacheImpl) {
         ListenerHolder listenerHolder = new ListenerHolder(listener, keyDataConversion, valueDataConversion, false);
         return ((CacheImpl) unwrapped).addListenerAsync(listenerHolder);
      } else {
         return cache.addListenerAsync(listener);
      }
   }

   @Override
   public <C> void addListener(Object listener, CacheEventFilter<? super K, ? super V> filter,
         CacheEventConverter<? super K, ? super V, C> converter) {
      CompletionStages.join(addListenerAsync(listener, filter, converter));
   }

   @Override
   public <C> CompletionStage<Void> addListenerAsync(Object listener, CacheEventFilter<? super K, ? super V> filter,
                               CacheEventConverter<? super K, ? super V, C> converter) {
      Cache unwrapped = unwrapCache(this.cache);
      if (unwrapped instanceof CacheImpl) {
         ListenerHolder listenerHolder = new ListenerHolder(listener, keyDataConversion, valueDataConversion, false);
         return ((CacheImpl) unwrapped).addListenerAsync(listenerHolder, filter, converter);
      } else {
         return cache.addListenerAsync(listener, filter, converter );
      }
   }

   @Override
   public <C> void addFilteredListener(Object listener,
         CacheEventFilter<? super K, ? super V> filter,
         CacheEventConverter<? super K, ? super V, C> converter,
         Set<Class<? extends Annotation>> filterAnnotations) {
      CompletionStages.join(addFilteredListenerAsync(listener, filter, converter, filterAnnotations));
   }

   @Override
   public <C> CompletionStage<Void> addFilteredListenerAsync(Object listener,
                                       CacheEventFilter<? super K, ? super V> filter,
                                       CacheEventConverter<? super K, ? super V, C> converter,
                                       Set<Class<? extends Annotation>> filterAnnotations) {
      Cache unwrapped = unwrapCache(this.cache);
      if (unwrapped instanceof CacheImpl) {
         ListenerHolder listenerHolder = new ListenerHolder(listener, keyDataConversion, valueDataConversion, false);
         return ((CacheImpl) unwrapped).addFilteredListenerAsync(listenerHolder, filter, converter, filterAnnotations);
      } else {
         return cache.addFilteredListenerAsync(listener, filter, converter, filterAnnotations);
      }
   }

   @Override
   public <C> void addStorageFormatFilteredListener(Object listener, CacheEventFilter<? super K, ? super V> filter,
         CacheEventConverter<? super K, ? super V, C> converter, Set<Class<? extends Annotation>> filterAnnotations) {
      CompletionStages.join(addStorageFormatFilteredListenerAsync(listener, filter, converter, filterAnnotations));
   }

   @Override
   public <C> CompletionStage<Void> addStorageFormatFilteredListenerAsync(Object listener, CacheEventFilter<? super K, ? super V> filter, CacheEventConverter<? super K, ? super V, C> converter, Set<Class<? extends Annotation>> filterAnnotations) {
      Cache<?, ?> unwrapped = unwrapCache(this.cache);
      if (unwrapped instanceof CacheImpl) {
         ListenerHolder listenerHolder = new ListenerHolder(listener, keyDataConversion, valueDataConversion, true);
         return ((CacheImpl) unwrapped).addFilteredListenerAsync(listenerHolder, filter, converter, filterAnnotations);
      } else {
         return cache.addFilteredListenerAsync(listener, filter, converter, filterAnnotations);
      }
   }

   private BiFunctionMapper wrapBiFunction(BiFunction<?, ?, ?> biFunction) {
      return biFunction == null ?
            null :
            new BiFunctionMapper(biFunction, keyDataConversion, valueDataConversion);
   }

   private FunctionMapper wrapFunction(Function<?, ?> function) {
      return function == null ?
            null :
            new FunctionMapper(function, keyDataConversion, valueDataConversion);
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> removeAsyncEntry(Object key) {
      K keyToStorage = keyToStorage(key);
      return cache.removeAsyncEntry(keyToStorage).thenApply(e -> unwrapCacheEntry(key, keyToStorage, e));
   }

   @Override
   public CachePublisher<K, V> cachePublisher() {
      return new CachePublisherWrapper<>(cache.cachePublisher());
   }
}
