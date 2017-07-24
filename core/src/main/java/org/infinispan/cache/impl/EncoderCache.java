package org.infinispan.cache.impl;

import static org.infinispan.commons.dataconversion.EncodingUtils.fromStorage;
import static org.infinispan.commons.dataconversion.EncodingUtils.toStorage;

import java.lang.annotation.Annotation;
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

import javax.security.auth.Subject;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.CacheCollection;
import org.infinispan.CacheSet;
import org.infinispan.CacheStream;
import org.infinispan.commands.read.AbstractCloseableIteratorCollection;
import org.infinispan.commons.dataconversion.Encoder;
import org.infinispan.commons.dataconversion.Wrapper;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorMapper;
import org.infinispan.commons.util.CloseableSpliterator;
import org.infinispan.commons.util.CloseableSpliteratorMapper;
import org.infinispan.compat.BiFunctionMapper;
import org.infinispan.compat.FunctionMapper;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ForwardingCacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.marshall.core.EncoderRegistry;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.ListenerHolder;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
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

   private final Class<? extends Encoder> keyEncoderClass;
   private final Class<? extends Encoder> valueEncoderClass;
   private final Class<? extends Wrapper> keyWrapperClass;
   private final Class<? extends Wrapper> valueWrapperClass;
   private Encoder keyEncoder;
   private Encoder valueEncoder;
   private Wrapper keyWrapper;
   private Wrapper valueWrapper;
   private InternalEntryFactory entryFactory;

   private final Function<V, V> decodedValueForRead = this::valueFromStorage;
   private EncoderRegistry encoderRegistry;


   public EncoderCache(AdvancedCache<K, V> cache, Class<? extends Encoder> keyEncoderClass,
                       Class<? extends Encoder> valueEncoderClass,
                       Class<? extends Wrapper> keyWrapperClass,
                       Class<? extends Wrapper> valueWrapperClass) {
      super(cache, c -> new EncoderCache<>(c, keyEncoderClass, valueEncoderClass, keyWrapperClass, valueWrapperClass));
      this.keyEncoderClass = keyEncoderClass;
      this.valueEncoderClass = valueEncoderClass;
      this.keyWrapperClass = keyWrapperClass;
      this.valueWrapperClass = valueWrapperClass;
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
      return (K) toStorage(key, keyEncoder, keyWrapper);
   }

   public V valueToStorage(Object value) {
      return (V) toStorage(value, valueEncoder, valueWrapper);
   }

   public K keyFromStorage(Object key) {
      return (K) fromStorage(key, keyEncoder, keyWrapper);
   }

   public V valueFromStorage(Object value) {
      return (V) fromStorage(value, valueEncoder, valueWrapper);
   }

   @Inject
   public void wireRealCache(ComponentRegistry registry, InternalEntryFactory entryFactory, EncoderRegistry encoderRegistry) {
      this.keyEncoder = encoderRegistry.getEncoder(keyEncoderClass);
      this.valueEncoder = encoderRegistry.getEncoder(valueEncoderClass);
      this.keyWrapper = encoderRegistry.getWrapper(keyWrapperClass);
      this.valueWrapper = encoderRegistry.getWrapper(valueWrapperClass);
      this.entryFactory = entryFactory;
      this.encoderRegistry = encoderRegistry;
      registry.wireDependencies(cache);
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
      return (k, v) -> remappingFunction.apply(keyFromStorage(k), valueFromStorage(v));
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

   private class EncodedKeySet extends AbstractCloseableIteratorCollection<K, K, V> implements CacheSet<K> {
      private final CacheSet<K> actualCollection;
      private final EncoderKeyMapper keyMapper = new EncoderKeyMapper(keyEncoderClass, keyWrapperClass);

      EncodedKeySet(Cache<K, V> cache, CacheSet<K> actualCollection) {
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
         return new CloseableIteratorMapper<>(actualCollection.iterator(), EncoderCache.this::keyFromStorage);
      }

      @Override
      public CloseableSpliterator<K> spliterator() {
         return new CloseableSpliteratorMapper<>(actualCollection.spliterator(), EncoderCache.this::keyFromStorage);
      }

      @Override
      public boolean contains(Object o) {
         return actualCollection.contains(keyToStorage(o));
      }

      @Override
      public boolean remove(Object o) {
         return actualCollection.remove(keyToStorage(o));
      }
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
   public Encoder getKeyEncoder() {
      return keyEncoder;
   }

   @Override
   public Encoder getValueEncoder() {
      return valueEncoder;
   }

   @Override
   public Wrapper getKeyWrapper() {
      return keyWrapper;
   }

   @Override
   public Wrapper getValueWrapper() {
      return valueWrapper;
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
   public Map<K, V> getAll(Set<?> keys) {
      Map<K, V> ret = super.getAll(encodeKeysForWrite(keys));
      return decodeMapForRead(ret);
   }

   @Override
   public CacheEntry<K, V> getCacheEntry(Object key) {
      K keyToStorage = keyToStorage(key);
      CacheEntry<K, V> returned = super.getCacheEntry(keyToStorage);
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
      return new EncoderEntrySet(this, super.cacheEntrySet());
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
      this.keyEncoder = encoderRegistry.getEncoder(keyEncoderClass);
      this.valueEncoder = encoderRegistry.getEncoder(valueEncoderClass);
      this.keyWrapper = encoderRegistry.getWrapper(keyWrapperClass);
      this.valueWrapper = encoderRegistry.getWrapper(valueWrapperClass);
   }

   private void initState(EncoderCache<K, V> encoderCache, EncoderCache<K, V> template) {
      encoderCache.entryFactory = template.entryFactory;
      encoderCache.encoderRegistry = template.encoderRegistry;
      encoderCache.lookupEncoderWrapper();
   }

   @Override
   public AdvancedCache<K, V> lockAs(Object lockOwner) {
      AdvancedCache<K, V> returned = super.lockAs(lockOwner);
      if (returned != this && returned instanceof EncoderCache) {
         initState((EncoderCache) returned, this);
      }
      return returned;
   }

   @Override
   public AdvancedCache<K, V> withEncoding(Class<? extends Encoder> keyEncoderClass, Class<? extends Encoder> valueEncoderClass) {
      EncoderCache<K, V> encoderCache = new EncoderCache<>(cache, keyEncoderClass, valueEncoderClass,
            this.keyWrapperClass, this.valueWrapperClass);
      checkSubclass(keyEncoderClass, Encoder.class);
      checkSubclass(valueEncoderClass, Encoder.class);
      initState(encoderCache, this);
      return encoderCache;
   }

   @Override
   public AdvancedCache<K, V> withEncoding(Class<? extends Encoder> encoderClass) {
      EncoderCache<K, V> encoderCache = new EncoderCache<>(cache, encoderClass, encoderClass,
            this.keyWrapperClass, this.valueWrapperClass);
      checkSubclass(encoderClass, Encoder.class);
      initState(encoderCache, this);
      return encoderCache;
   }

   private void checkSubclass(Class<?> configured, Class<?> required) {
      if (!required.isAssignableFrom(configured)) {
         throw log.invalidEncodingClass(configured, required);
      }
   }

   @Override
   public AdvancedCache<K, V> withWrapping(Class<? extends Wrapper> keyWrapperClass, Class<? extends Wrapper> valueWrapperClass) {
      EncoderCache<K, V> encoderCache = new EncoderCache<>(cache, this.keyEncoderClass, this.valueEncoderClass,
            keyWrapperClass, valueWrapperClass);
      checkSubclass(keyWrapperClass, Wrapper.class);
      checkSubclass(valueWrapperClass, Wrapper.class);
      initState(encoderCache, this);
      return encoderCache;
   }

   @Override
   public AdvancedCache<K, V> withFlags(Flag... flags) {
      AdvancedCache<K, V> returned = super.withFlags(flags);
      if (returned != this && returned instanceof EncoderCache) {
         initState((EncoderCache) returned, this);
      }
      return returned;
   }

   @Override
   public AdvancedCache<K, V> withSubject(Subject subject) {
      AdvancedCache<K, V> returned = super.withSubject(subject);
      if (returned != this && returned instanceof EncoderCache) {
         initState((EncoderCache) returned, this);
      }
      return returned;
   }

   @Override
   public AdvancedCache<K, V> with(ClassLoader classLoader) {
      AdvancedCache<K, V> returned = super.with(classLoader);
      if (returned != this && returned instanceof EncoderCache) {
         initState((EncoderCache) returned, this);
      }
      return returned;
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
            new BiFunctionMapper(remappingFunction, keyEncoderClass, valueEncoderClass, keyWrapperClass, valueWrapperClass));
      return valueFromStorage(returned);
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      Object returned = super.computeIfPresent(keyToStorage(key),
            new BiFunctionMapper(remappingFunction, keyEncoderClass, valueEncoderClass, keyWrapperClass, valueWrapperClass));
      return valueFromStorage(returned);
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
      Object ret = super.computeIfAbsent(keyToStorage(key),
            new FunctionMapper(mappingFunction, keyEncoderClass, valueEncoderClass, keyWrapperClass, valueWrapperClass));
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
            new BiFunctionMapper(remappingFunction, keyEncoderClass, valueEncoderClass, keyWrapperClass, valueWrapperClass));
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
      return new EncodedKeySet(this, super.keySet());
   }


   private class EncoderIterator<A, B> implements CloseableIterator<CacheEntry<A, B>> {
      private final CloseableIterator<CacheEntry<A, B>> iterator;
      private final InternalEntryFactory entryFactory;

      EncoderIterator(CloseableIterator<CacheEntry<A, B>> iterator, InternalEntryFactory entryFactory) {
         this.iterator = iterator;
         this.entryFactory = entryFactory;
      }

      @Override
      public void close() {
         iterator.close();
      }

      @Override
      public boolean hasNext() {
         return iterator.hasNext();
      }

      @Override
      public CacheEntry<A, B> next() {
         CacheEntry<A, B> entry = iterator.next();
         return new EntryWrapper<>(entry, convert(entry));
      }

      private CacheEntry<A, B> convert(CacheEntry<A, B> entry) {
         A newKey = (A) keyFromStorage(entry.getKey());
         B newValue = (B) valueFromStorage(entry.getValue());

         // If either value changed then make a copy
         if (newKey != entry.getKey() || newValue != entry.getValue()) {
            if (entry instanceof InternalCacheEntry) {
               return entryFactory.create(newKey, newValue, (InternalCacheEntry) entry);
            }
            return entryFactory.create(newKey, newValue, entry.getMetadata());
         }
         return entry;
      }

      @Override
      public void remove() {
         iterator.remove();
      }
   }

   private class EncoderEntrySet extends AbstractCloseableIteratorCollection<CacheEntry<K, V>, K, V> implements CacheSet<CacheEntry<K, V>> {
      private CacheSet<CacheEntry<K, V>> actualCollection;
      private EncoderEntryMapper entryMapper;


      EncoderEntrySet(Cache<K, V> cache, CacheSet<CacheEntry<K, V>> actualCollection) {
         super(cache);
         this.entryMapper = new EncoderEntryMapper(keyEncoderClass, valueEncoderClass, keyWrapperClass, valueWrapperClass);
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
         return new EncoderIterator<>(actualCollection.iterator(), entryFactory);
      }

      @Override
      public CloseableSpliterator<CacheEntry<K, V>> spliterator() {
         return new CloseableSpliteratorMapper<>(actualCollection.spliterator(),
               entry -> {
                  K key = entry.getKey();
                  K keyFromStorage = keyFromStorage(key);
                  V value = entry.getValue();
                  V valueFromStorage = valueFromStorage(value);
                  if (keyFromStorage != key || valueFromStorage != value) {
                     return convertEntry(keyFromStorage, valueFromStorage, entry);
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
            K newKey = keyToStorage(key);
            V value = entry.getValue();
            V newValue = valueFromStorage(value);
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

   private class EntryWrapper<A, B> extends ForwardingCacheEntry<A, B> {
      private final CacheEntry<A, B> previousEntry;
      private final CacheEntry<A, B> entry;

      EntryWrapper(CacheEntry<A, B> previousEntry, CacheEntry<A, B> entry) {
         this.previousEntry = previousEntry;
         this.entry = entry;
      }

      @Override
      protected CacheEntry<A, B> delegate() {
         return entry;
      }

      @Override
      public B setValue(B value) {
         previousEntry.setValue(value);
         return super.setValue(value);
      }
   }

   @Override
   public CacheSet<Entry<K, V>> entrySet() {
      return cast(new EncoderEntrySet(this, cast(super.cacheEntrySet())));

   }

   private <E extends Entry<K, V>> CacheSet<E> cast(CacheSet set) {
      return (CacheSet<E>) set;
   }


   private class EncoderValuesCollection extends AbstractCloseableIteratorCollection<V, K, V> implements CacheCollection<V> {
      private final CacheCollection<V> actualCollection;
      final EncoderValueMapper valueMapper = new EncoderValueMapper(valueEncoderClass, valueWrapperClass);

      EncoderValuesCollection(Cache<K, V> cache, CacheCollection<V> actualCollection) {
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
         return new CloseableIteratorMapper<>(actualCollection.iterator(), EncoderCache.this::valueFromStorage);
      }

      @Override
      public CloseableSpliterator<V> spliterator() {
         return new CloseableSpliteratorMapper<>(actualCollection.spliterator(), EncoderCache.this::valueFromStorage);
      }

      @Override
      public boolean contains(Object o) {
         return actualCollection.contains(valueToStorage(o));
      }

      @Override
      public boolean remove(Object o) {
         return actualCollection.remove(valueToStorage(o));
      }
   }

   @Override
   public CacheCollection<V> values() {
      return new EncoderValuesCollection(this, super.values());
   }

   @Override
   public CompletableFuture<V> getAsync(K key) {
      return super.getAsync(keyToStorage(key)).thenApply(decodedValueForRead);
   }

   @Override
   public void addListener(Object listener) {
      ListenerHolder listenerHolder = new ListenerHolder(listener, keyEncoderClass, valueEncoderClass, keyWrapperClass, valueWrapperClass);
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
      ListenerHolder listenerHolder = new ListenerHolder(listener, keyEncoderClass, valueEncoderClass, keyWrapperClass, valueWrapperClass);
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
      ListenerHolder listenerHolder = new ListenerHolder(listener, keyEncoderClass, valueEncoderClass, keyWrapperClass, valueWrapperClass);
      Cache unwrapped = super.unwrapCache(this.cache);
      if (unwrapped instanceof CacheImpl) {
         ((CacheImpl) unwrapped).addFilteredListener(listenerHolder, filter, converter, filterAnnotations);
      } else {
         super.addFilteredListener(listener, filter, converter, filterAnnotations);
      }
   }

   //HACK!
   public EncoderCache<K, V> withCache(AdvancedCache<K, V> otherCache) {
      EncoderCache<K, V> cache = new EncoderCache<>(otherCache, keyEncoderClass, valueEncoderClass, keyWrapperClass, valueWrapperClass);
      initState(cache, this);
      return cache;
   }
}
