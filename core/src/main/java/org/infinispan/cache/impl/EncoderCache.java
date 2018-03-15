package org.infinispan.cache.impl;

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
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheCollection;
import org.infinispan.CacheSet;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.IdentityEncoder;
import org.infinispan.commons.dataconversion.IdentityWrapper;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.util.InjectiveFunction;
import org.infinispan.compat.BiFunctionMapper;
import org.infinispan.compat.FunctionMapper;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ForwardingCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.ListenerHolder;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.util.WriteableCacheCollectionMapper;
import org.infinispan.util.WriteableCacheSetMapper;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Cache decoration that makes use of the {@link Encoder} and {@link Wrapper} to convert between storage value and
 * read/write value.
 *
 * @since 9.1
 */
public class EncoderCache<K, V> extends AbstractDelegatingAdvancedCache<K, V> {

   private static Log log = LogFactory.getLog(EncoderCache.class);

   @Inject private InternalEntryFactory entryFactory;
   @Inject private ComponentRegistry componentRegistry;

   private final DataConversion keyDataConversion;
   private final DataConversion valueDataConversion;

   private final Function<V, V> decodedValueForRead = this::valueFromStorage;

   public EncoderCache(AdvancedCache<K, V> cache, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      super(cache, new AdvancedCacheWrapper<K, V>() {
         @Override
         public AdvancedCache<K, V> wrap(AdvancedCache<K, V> cache) {
            throw new UnsupportedOperationException();
         }

         // we cannot pass a reference to self to superconstructor, so we need to provide it explicitly to the wrapper
         @Override
         public AdvancedCache<K, V> wrap(AdvancedCache<K, V> self, AdvancedCache<K, V> newDelegate) {
            EncoderCache newCache = new EncoderCache<>(newDelegate, keyDataConversion, valueDataConversion);
            ((EncoderCache) self).initState(newCache, ((EncoderCache) self));
            return newCache;
         }
      });
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
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
      componentRegistry.wireDependencies(keyDataConversion);
      componentRegistry.wireDependencies(valueDataConversion);
      componentRegistry.wireDependencies(cache);
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
      map.values().forEach(v -> {
         K originalKey = v.getKey();
         K unwrappedKey = keyFromStorage(originalKey);
         V originalValue = v.getValue();
         V unwrappedValue = valueFromStorage(originalValue);
         CacheEntry<K, V> entryToPut;
         if (unwrappedKey != originalKey || unwrappedValue != originalValue) {
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
      super.putForExternalRead(keyToStorage(key), valueToStorage(value));
   }

   @Override
   public void putForExternalRead(K key, V value, long lifespan, TimeUnit unit) {
      super.putForExternalRead(keyToStorage(key), valueToStorage(value), lifespan, unit);
   }

   @Override
   public void putForExternalRead(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      super.putForExternalRead(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public void evict(K key) {
      super.evict(keyToStorage(key));
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit unit) {
      V ret = super.put(keyToStorage(key), valueToStorage(value), lifespan, unit);
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
   protected void set(K key, V value) {
      super.set(keyToStorage(key), valueToStorage(value));
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit unit) {
      V v = super.putIfAbsent(keyToStorage(key), valueToStorage(value), lifespan, unit);
      return valueFromStorage(v);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit) {
      super.putAll(encodeMapForWrite(map), lifespan, unit);
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit unit) {
      V ret = super.replace(keyToStorage(key), valueToStorage(value), lifespan, unit);
      return valueFromStorage(ret);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit) {
      return super.replace(keyToStorage(key), valueToStorage(oldValue), valueToStorage(value), lifespan, unit);
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      V ret = super.put(keyToStorage(key), valueToStorage(value), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return valueFromStorage(ret);
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      V ret = super.putIfAbsent(keyToStorage(key), valueToStorage(value), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return valueFromStorage(ret);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      super.putAll(encodeMapForWrite(map), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }


   @Override
   public V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      V ret = super.replace(keyToStorage(key), valueToStorage(value), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
      return valueFromStorage(ret);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return super.replace(keyToStorage(key), valueToStorage(oldValue), valueToStorage(value), lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
      super.replaceAll(convertFunction(function));
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value) {
      return super.putAsync(keyToStorage(key), valueToStorage(value)).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit unit) {
      return super.putAsync(keyToStorage(key), valueToStorage(value), lifespan, unit).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return super.putAsync(keyToStorage(key), valueToStorage(value), lifespan, lifespanUnit, maxIdle, maxIdleUnit).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data) {
      return super.putAllAsync(encodeMapForWrite(data));
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit) {
      return super.putAllAsync(encodeMapForWrite(data), lifespan, unit);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return super.putAllAsync(encodeMapForWrite(data), lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> map, Metadata metadata) {
      return super.putAllAsync(encodeMapForWrite(map), metadata);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value) {
      return super.putIfAbsentAsync(keyToStorage(key), valueToStorage(value)).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit unit) {
      return super.putIfAbsentAsync(keyToStorage(key), valueToStorage(value), lifespan, unit).thenApply(decodedValueForRead);
   }

   @Override
   public boolean lock(K... keys) {
      K[] encoded = (K[]) Arrays.stream(keys).map(this::keyToStorage).toArray();
      return super.lock(encoded);
   }

   @Override
   public boolean lock(Collection<? extends K> keys) {
      return super.lock(encodeKeysForWrite(keys));
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return super.putIfAbsentAsync(keyToStorage(key), valueToStorage(value), lifespan, lifespanUnit, maxIdle, maxIdleUnit).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, Metadata metadata) {
      return super.putIfAbsentAsync(keyToStorage(key), valueToStorage(value), metadata).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> removeAsync(Object key) {
      return super.removeAsync(keyToStorage(key)).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<Boolean> removeAsync(Object key, Object value) {
      return super.removeAsync(keyToStorage(key), valueToStorage(value));
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value) {
      return super.replaceAsync(keyToStorage(key), valueToStorage(value)).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit unit) {
      return super.replaceAsync(keyToStorage(key), valueToStorage(value), lifespan, unit).thenApply(decodedValueForRead);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, Metadata metadata) {
      return super.replaceAsync(keyToStorage(key), valueToStorage(value), metadata).thenApply(decodedValueForRead);
   }

   @Override
   public Map<K, V> getAll(Set<?> keys) {
      Map<K, V> ret = super.getAll(encodeKeysForWrite(keys));
      return decodeMapForRead(ret);
   }

   @Override
   public CompletableFuture<Map<K, V>> getAllAsync(Set<?> keys) {
      return super.getAllAsync(encodeKeysForWrite(keys)).thenApply(this::decodeMapForRead);
   }

   @Override
   public CacheEntry<K, V> getCacheEntry(Object key) {
      K keyToStorage = keyToStorage(key);
      CacheEntry<K, V> returned = super.getCacheEntry(keyToStorage);
      return unwrapCacheEntry(key, keyToStorage, returned);
   }

   @Override
   public CompletableFuture<CacheEntry<K, V>> getCacheEntryAsync(Object key) {
      K keyToStorage = keyToStorage(key);
      return super.getCacheEntryAsync(keyToStorage)
            .thenApply(returned -> unwrapCacheEntry(key, keyToStorage, returned));
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
      return super.replaceAsync(keyToStorage(key), valueToStorage(value), lifespan, lifespanUnit, maxIdle, maxIdleUnit)
            .thenApply(decodedValueForRead);
   }

   @Override
   public Map<K, CacheEntry<K, V>> getAllCacheEntries(Set<?> keys) {
      Map<K, CacheEntry<K, V>> returned = super.getAllCacheEntries(encodeKeysForWrite(keys));
      return decodeEntryMapForRead(returned);
   }

   @Override
   public Map<K, V> getGroup(String groupName) {
      Map<K, V> ret = super.getGroup(groupName);
      return decodeMapForRead(ret);
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
      return super.replaceAsync(keyToStorage(key), valueToStorage(oldValue), valueToStorage(newValue));
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit) {
      return super.replaceAsync(keyToStorage(key), valueToStorage(oldValue), valueToStorage(newValue), lifespan, unit);
   }

   @Override
   public V put(K key, V value, Metadata metadata) {
      V ret = super.put(keyToStorage(key), valueToStorage(value), metadata);
      return valueFromStorage(ret);
   }

   @Override
   public V replace(K key, V value, Metadata metadata) {
      V ret = super.replace(keyToStorage(key), valueToStorage(value), metadata);
      return valueFromStorage(ret);
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return super.replaceAsync(keyToStorage(key), valueToStorage(oldValue), valueToStorage(newValue), lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, Metadata metadata) {
      return super.replace(keyToStorage(key), valueToStorage(oldValue), valueToStorage(value), metadata);
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, Metadata metadata) {
      return super.replaceAsync(keyToStorage(key), valueToStorage(oldValue), valueToStorage(newValue), metadata);
   }

   @Override
   public V putIfAbsent(K key, V value, Metadata metadata) {
      V ret = super.putIfAbsent(keyToStorage(key), valueToStorage(value), metadata);
      return valueFromStorage(ret);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, Metadata metadata) {
      return super.putAsync(keyToStorage(key), valueToStorage(value), metadata).thenApply(decodedValueForRead);
   }

   @Override
   public void putForExternalRead(K key, V value, Metadata metadata) {
      super.putForExternalRead(keyToStorage(key), valueToStorage(value), metadata);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, Metadata metadata) {
      super.putAll(encodeMapForWrite(map), metadata);
   }

   @Override
   public CacheSet<CacheEntry<K, V>> cacheEntrySet() {
      EncoderEntryMapper<K, V, CacheEntry<K, V>> cacheEntryMapper = EncoderEntryMapper.newCacheEntryMapper(
            keyDataConversion, valueDataConversion, entryFactory);
      return new WriteableCacheSetMapper<>(super.cacheEntrySet(), cacheEntryMapper,
            e -> new CacheEntryWrapper<>(cacheEntryMapper.apply(e), e), this::toCacheEntry, this::keyToStorage);
   }

   @Override
   public void removeExpired(K key, V value, Long lifespan) {
      super.removeExpired(keyToStorage(key), valueToStorage(value), lifespan);
   }

   @Override
   public V putIfAbsent(K key, V value) {
      V ret = super.putIfAbsent(keyToStorage(key), valueToStorage(value));
      return valueFromStorage(ret);
   }

   private void lookupEncoderWrapper() {
      ComponentStatus status = cache.getAdvancedCache().getComponentRegistry().getStatus();
      if (!status.equals(ComponentStatus.STOPPING) && !status.equals(ComponentStatus.TERMINATED)) {
         componentRegistry.wireDependencies(keyDataConversion);
         componentRegistry.wireDependencies(valueDataConversion);
      }
   }

   private void initState(EncoderCache<K, V> encoderCache, EncoderCache<K, V> template) {
      encoderCache.entryFactory = template.entryFactory;
      encoderCache.componentRegistry = template.componentRegistry;
   }

   @Override
   public AdvancedCache<K, V> withEncoding(Class<? extends Encoder> keyEncoderClass, Class<? extends Encoder> valueEncoderClass) {
      checkSubclass(keyEncoderClass, Encoder.class);
      checkSubclass(valueEncoderClass, Encoder.class);

      if (allIdentity(keyEncoderClass, valueEncoderClass, keyDataConversion.getWrapperClass(),
            valueDataConversion.getWrapperClass())) {
         return cache;
      }

      DataConversion newKeyDataConversion = keyDataConversion.withEncoding(keyEncoderClass);
      DataConversion newValueDataConversion = valueDataConversion.withEncoding(valueEncoderClass);
      EncoderCache<K, V> encoderCache = new EncoderCache<>(cache, newKeyDataConversion, newValueDataConversion);
      initState(encoderCache, this);
      encoderCache.lookupEncoderWrapper();
      return encoderCache;
   }

   @Override
   public AdvancedCache<K, V> withEncoding(Class<? extends Encoder> encoderClass) {
      checkSubclass(encoderClass, Encoder.class);

      if (allIdentity(encoderClass, encoderClass, keyDataConversion.getWrapperClass(),
            valueDataConversion.getWrapperClass())) {
         return cache;
      }

      DataConversion newKeyDataConversion = keyDataConversion.withEncoding(encoderClass);
      DataConversion newValueDataConversion = valueDataConversion.withEncoding(encoderClass);
      EncoderCache<K, V> encoderCache = new EncoderCache<>(cache, newKeyDataConversion, newValueDataConversion);
      initState(encoderCache, this);
      encoderCache.lookupEncoderWrapper();
      return encoderCache;
   }

   @Override
   public AdvancedCache<K, V> withKeyEncoding(Class<? extends Encoder> encoderClass) {
      checkSubclass(encoderClass, Encoder.class);

      if (allIdentity(encoderClass, valueDataConversion.getEncoderClass(), keyDataConversion.getWrapperClass(),
            valueDataConversion.getWrapperClass())) {
         return cache;
      }

      DataConversion newKeyDataConversion = keyDataConversion.withEncoding(encoderClass);
      EncoderCache<K, V> encoderCache = new EncoderCache<>(cache, newKeyDataConversion, valueDataConversion);
      initState(encoderCache, this);
      encoderCache.lookupEncoderWrapper();
      return encoderCache;
   }

   private void checkSubclass(Class<?> configured, Class<?> required) {
      if (!required.isAssignableFrom(configured)) {
         throw log.invalidEncodingClass(configured, required);
      }
   }

   /**
    * If encoders and wrappers are all identity we should just return the normal cache and avoid all wrappings
    * @param keyEncoderClass the key encoder class
    * @param valueEncoderClass the value encoder class
    * @param keyWrapperClass the key wrapper class
    * @param valueWrapperClass the value wrapper class
    * @return true if all classes are identity oness
    */
   private boolean allIdentity(Class<? extends Encoder> keyEncoderClass, Class<? extends Encoder> valueEncoderClass,
         Class<? extends Wrapper> keyWrapperClass, Class<? extends Wrapper> valueWrapperClass) {
      return keyEncoderClass == IdentityEncoder.class && valueEncoderClass == IdentityEncoder.class &&
            keyWrapperClass == IdentityWrapper.class && valueWrapperClass == IdentityWrapper.class;
   }

   @Override
   public AdvancedCache<K, V> withWrapping(Class<? extends Wrapper> keyWrapperClass, Class<? extends Wrapper> valueWrapperClass) {
      checkSubclass(keyWrapperClass, Wrapper.class);
      checkSubclass(valueWrapperClass, Wrapper.class);

      if (allIdentity(keyDataConversion.getEncoderClass(), valueDataConversion.getEncoderClass(), keyWrapperClass,
            valueWrapperClass)) {
         return cache;
      }

      DataConversion newKeyDataConversion = keyDataConversion.withWrapping(keyWrapperClass);
      DataConversion newValueDataConversion = valueDataConversion.withWrapping(valueWrapperClass);
      EncoderCache<K, V> encoderCache = new EncoderCache<>(cache, newKeyDataConversion, newValueDataConversion);
      initState(encoderCache, this);
      encoderCache.lookupEncoderWrapper();
      return encoderCache;
   }

   @Override
   public AdvancedCache<K, V> withWrapping(Class<? extends Wrapper> wrapper) {
      return withWrapping(wrapper, wrapper);
   }

   @Override
   public AdvancedCache<K, V> withMediaType(String keyMediaType, String valueMediaType) {
      MediaType kType = MediaType.fromString(keyMediaType);
      MediaType vType = MediaType.fromString(valueMediaType);
      DataConversion newKeyDataConversion = keyDataConversion.withRequestMediaType(kType);
      DataConversion newValueDataConversion = valueDataConversion.withRequestMediaType(vType);
      EncoderCache<K, V> encoderCache = new EncoderCache<>(cache, newKeyDataConversion, newValueDataConversion);
      initState(encoderCache, this);
      encoderCache.lookupEncoderWrapper();
      return encoderCache;
   }

   @Override
   public boolean remove(Object key, Object value) {
      return super.remove(keyToStorage(key), valueToStorage(value));
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue) {
      return super.replace(keyToStorage(key), valueToStorage(oldValue), valueToStorage(newValue));
   }

   @Override
   public V replace(K key, V value) {
      V ret = super.replace(keyToStorage(key), valueToStorage(value));
      return valueFromStorage(ret);
   }

   @Override
   public boolean containsKey(Object key) {
      return super.containsKey(keyToStorage(key));
   }

   @Override
   public boolean containsValue(Object value) {
      return super.containsValue(valueToStorage(value));
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      Object returned = super.compute(keyToStorage(key),
            new BiFunctionMapper(remappingFunction, keyDataConversion, valueDataConversion));
      return valueFromStorage(returned);
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      Object returned = super.computeIfPresent(keyToStorage(key),
            new BiFunctionMapper(remappingFunction, keyDataConversion, valueDataConversion));
      return valueFromStorage(returned);
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
      Object ret = super.computeIfAbsent(keyToStorage(key),
            new FunctionMapper(mappingFunction, keyDataConversion, valueDataConversion));
      return valueFromStorage(ret);
   }

   @Override
   public V get(Object key) {
      V v = super.get(keyToStorage(key));
      return valueFromStorage(v);
   }

   @Override
   public V getOrDefault(Object key, V defaultValue) {
      V returned = super.getOrDefault(keyToStorage(key), defaultValue);
      if (returned == defaultValue) {
         return returned;
      }
      return valueFromStorage(returned);
   }

   @Override
   public V put(K key, V value) {
      V ret = super.put(keyToStorage(key), valueToStorage(value));
      if (ret == null) {
         return null;
      }
      return valueFromStorage(ret);
   }

   @Override
   public V remove(Object key) {
      V ret = super.remove(keyToStorage(key));
      return valueFromStorage(ret);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> t) {
      super.putAll(encodeMapForWrite(t));
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      Object returned = super.merge(keyToStorage(key), valueToStorage(value),
            new BiFunctionMapper(remappingFunction, keyDataConversion, valueDataConversion));
      return valueFromStorage(returned);
   }

   @Override
   public void forEach(BiConsumer<? super K, ? super V> action) {
      super.forEach((k, v) -> {
         K newK = keyFromStorage(k);
         V newV = valueFromStorage(v);
         action.accept(newK, newV);
      });
   }

   @Override
   public CacheSet<K> keySet() {
      InjectiveFunction<Object, K> keyToStorage = this::keyToStorage;
      return new WriteableCacheSetMapper<>(super.keySet(), new EncoderKeyMapper<>(keyDataConversion),
            keyToStorage, keyToStorage);
   }

   @Override
   public CacheSet<Map.Entry<K, V>> entrySet() {
      EncoderEntryMapper<K, V, Map.Entry<K, V>> entryMapper = EncoderEntryMapper.newEntryMapper(keyDataConversion,
            valueDataConversion, entryFactory);
      return new WriteableCacheSetMapper<>(super.entrySet(), entryMapper,
            e -> new EntryWrapper<>(e, entryMapper.apply(e, true)), this::toEntry, this::keyToStorage);
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
            if (o instanceof CacheEntry) {
               CacheEntry returned = (CacheEntry) o;
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
      return new WriteableCacheCollectionMapper<>(super.values(), new EncoderValueMapper<>(valueDataConversion),
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

   @Override
   public CompletableFuture<V> getAsync(K key) {
      return super.getAsync(keyToStorage(key)).thenApply(decodedValueForRead);
   }

   @Override
   public void addListener(Object listener) {
      ListenerHolder listenerHolder = new ListenerHolder(listener, keyDataConversion, valueDataConversion);
      Cache unwrapped = super.unwrapCache(this.cache);
      if (unwrapped instanceof CacheImpl) {
         ((CacheImpl) unwrapped).addListener(listenerHolder);
      } else {
         super.addListener(listener);
      }
   }

   @Override
   public <C> void addListener(Object listener, CacheEventFilter<? super K, ? super V> filter,
                               CacheEventConverter<? super K, ? super V, C> converter) {
      ListenerHolder listenerHolder = new ListenerHolder(listener, keyDataConversion, valueDataConversion);
      Cache unwrapped = super.unwrapCache(this.cache);
      if (unwrapped instanceof CacheImpl) {
         ((CacheImpl) unwrapped).addListener(listenerHolder, filter, converter);
      } else {
         super.addListener(listener);
      }

   }

   @Override
   public <C> void addFilteredListener(Object listener,
                                       CacheEventFilter<? super K, ? super V> filter,
                                       CacheEventConverter<? super K, ? super V, C> converter,
                                       Set<Class<? extends Annotation>> filterAnnotations) {
      ListenerHolder listenerHolder = new ListenerHolder(listener, keyDataConversion, valueDataConversion);
      Cache unwrapped = super.unwrapCache(this.cache);
      if (unwrapped instanceof CacheImpl) {
         ((CacheImpl) unwrapped).addFilteredListener(listenerHolder, filter, converter, filterAnnotations);
      } else {
         super.addFilteredListener(listener, filter, converter, filterAnnotations);
      }
   }

   //HACK!
   public EncoderCache<K, V> withCache(AdvancedCache<K, V> otherCache) {
      EncoderCache<K, V> cache = new EncoderCache<>(otherCache, keyDataConversion, valueDataConversion);
      initState(cache, this);
      return cache;
   }
}
