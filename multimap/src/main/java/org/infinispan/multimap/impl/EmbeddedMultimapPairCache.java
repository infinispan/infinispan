package org.infinispan.multimap.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.multimap.impl.function.hmap.HashMapKeySetFunction;
import org.infinispan.multimap.impl.function.hmap.HashMapPutFunction;
import org.infinispan.multimap.impl.function.hmap.HashMapRemoveFunction;
import org.infinispan.multimap.impl.function.hmap.HashMapReplaceFunction;
import org.infinispan.multimap.impl.function.hmap.HashMapValuesFunction;

import static java.util.Objects.requireNonNull;

/**
 * Multimap which holds a collection of key-values pairs.
 * <p>
 *    This multimap can create arbitrary objects by holding a collection of key-value pairs under a single cache
 *    entry. It is possible to add or remove attributes dynamically, varying the structure format between keys.
 * </p>
 * Note that the structure is not distributed, it is under a single key, and the distribution happens per key.
 *
 * @param <K>: The type of key to identify the structure.
 * @param <HK>: The structure type for keys.
 * @param <HV>: The structure type for values.
 * @since 15.0
 * @author José Bolina
 */
public class EmbeddedMultimapPairCache<K, HK, HV> {

   public static final String ERR_KEY_CAN_T_BE_NULL = "key can't be null";
   public static final String ERR_PROPERTY_CANT_BE_NULL = "property can't be null";
   public static final String ERR_VALUE_CANT_BE_NULL = "property value can't be null";
   public static final String ERR_PROPERTIES_CANT_BE_EMPTY = "properties can't be empty";
   public static final String ERR_COUNT_MUST_BE_POSITIVE = "count must be positive";

   private final FunctionalMap.ReadWriteMap<K, HashMapBucket<HK, HV>> readWriteMap;
   private final AdvancedCache<K, HashMapBucket<HK, HV>> cache;

   public EmbeddedMultimapPairCache(Cache<K, HashMapBucket<HK, HV>> cache) {
      this.cache = cache.getAdvancedCache();
      FunctionalMapImpl<K, HashMapBucket<HK, HV>> functionalMap = FunctionalMapImpl.create(this.cache);
      this.readWriteMap = ReadWriteMapImpl.create(functionalMap);
   }

   /**
    * Set the given key-value pair in the multimap under the given key.
    * If the key is not present, a new multimap is created.
    *
    * @param key: Cache key to store the values.
    * @param entries: Key-value pairs to store.
    * @return {@link CompletionStage} with the number of created entries.
    */
   @SafeVarargs
   public final CompletionStage<Integer> set(K key, Map.Entry<HK, HV>... entries) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      List<Map.Entry<HK, HV>> values = new ArrayList<>(Arrays.asList(entries));
      return readWriteMap.eval(key, new HashMapPutFunction<>(values));
   }

   /**
    * Set the given key-value in the multimap under the given key.
    * <p>
    * If the key is not present, a new multimap is created. If the property already exists, the operation has no effect.
    * </p>
    *
    * @param key: Cache key to store key-value pair.
    * @param propertyKey: The property key.
    * @param propertyValue: The property value.
    * @return <code>true</code> if the value is set, <code>false</code>, otherwise.
    */
   public final CompletionStage<Boolean> setIfAbsent(K key, HK propertyKey, HV propertyValue) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(propertyKey, ERR_PROPERTY_CANT_BE_NULL);
      requireNonNull(propertyValue, ERR_VALUE_CANT_BE_NULL);
      return readWriteMap.eval(key, new HashMapPutFunction<>(List.of(Map.entry(propertyKey, propertyValue)), true))
            .thenApply(v -> v > 0L);
   }

   /**
    * Get the key-value pairs under the given key.
    *
    * @param key: Cache key to retrieve the values.
    * @return {@link CompletionStage} containing a {@link Map} with the key-value pairs or an empty map if the key
    *         is not found.
    */
   public CompletionStage<Map<HK, HV>> get(K key) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return cache.getCacheEntryAsync(key)
            .thenApply(entry -> {
               if (entry == null) {
                  return Map.of();
               }

               HashMapBucket<HK, HV> bucket = entry.getValue();
               return bucket.converted();
            });
   }

   public CompletionStage<HV> get(K key, HK property) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(property, ERR_KEY_CAN_T_BE_NULL);
      return cache.getCacheEntryAsync(key)
            .thenApply(entry -> {
               if (entry == null) {
                  return null;
               }

               HashMapBucket<HK, HV> bucket = entry.getValue();
               return bucket.get(property);
            });
   }

   /**
    * Return the values of the given {@param properties} stored under the given {@param key}.
    *
    * @param key: Cache key to retrieve the hash map.
    * @param properties: Properties to retrieve.
    * @return {@link CompletionStage} containing a {@link Map} with the key-value pairs or an empty map if the key
    *        is not found.
    */
   @SafeVarargs
   public final CompletionStage<Map<HK, HV>> get(K key, HK ... properties) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(properties, ERR_PROPERTY_CANT_BE_NULL);
      requireTrue(properties.length > 0, ERR_PROPERTIES_CANT_BE_EMPTY);

      Set<HK> propertySet = Set.of(properties);
      requireNonNullArgument(propertySet, ERR_PROPERTY_CANT_BE_NULL);

      return cache.getCacheEntryAsync(key)
            .thenApply(entry -> {
               if (entry == null) {
                  return Map.of();
               }

               HashMapBucket<HK, HV> bucket = entry.getValue();
               return bucket.getAll(propertySet);
            });
   }

   /**
    * Get the size of the hash map stored under key.
    *
    * @param key: Cache key to retrieve the hash map.
    * @return {@link CompletionStage} containing the size of the hash map or 0 if the key is not found.
    */
   public CompletionStage<Integer> size(K key) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return cache.getCacheEntryAsync(key)
            .thenApply(entry -> {
               if (entry == null) {
                  return 0;
               }

               HashMapBucket<HK, HV> bucket = entry.getValue();
               return bucket.size();
            });
   }

   /**
    * Get the key set from the hash map stored under the given key.
    *
    * @param key: Cache key to retrieve the hash map.
    * @return {@link CompletionStage} containing a {@link Set} with the keys or an empty set if the key is not found.
    */
   public CompletionStage<Set<HK>> keySet(K key) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return readWriteMap.eval(key, HashMapKeySetFunction.instance());
   }

   /**
    * Get the values from the hash map stored under the given key.
    *
    * @param key: Cache key to retrieve the hash map.
    * @return {@link CompletionStage} containing a {@link Collection} with the values or an empty collection if the key
    *        is not found.
    */
   public CompletionStage<Collection<HV>> values(K key) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return readWriteMap.eval(key, HashMapValuesFunction.instance());
   }

   /**
    * Verify if the property is present in the hash map stored under the given key.
    *
    * @param key: Cache key to retrieve the hash map.
    * @param property: Property to verify in the hash map.
    * @return {@link CompletionStage} with {@code true} if the property is present or {@code false} otherwise.
    */
   public CompletionStage<Boolean> contains(K key, HK property) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(property, ERR_PROPERTY_CANT_BE_NULL);
      return cache.getCacheEntryAsync(key)
            .thenApply(entry -> {
               if (entry == null) {
                  return false;
               }

               HashMapBucket<HK, HV> bucket = entry.getValue();
               return bucket.containsKey(property);
            });
   }

   /**
    * Remove the properties in the hash map under the given key.
    *
    * @param key: Cache key to remove the properties.
    * @param property: Property to remove.
    * @param properties: Additional properties to remove.
    * @return {@link CompletionStage} with the number of removed properties.
    */
   @SafeVarargs
   public final CompletionStage<Integer> remove(K key, HK property, HK ...properties) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(property, ERR_PROPERTY_CANT_BE_NULL);
      List<HK> propertiesList = new ArrayList<>();
      propertiesList.add(property);
      propertiesList.addAll(Arrays.asList(properties));
      return readWriteMap.eval(key, new HashMapRemoveFunction<>(propertiesList));
   }

   /**
    * Remove the given properties in the hash map stored under the given key.
    *
    * @param key:  Cache key to remove the properties.
    * @param properties: Properties to remove.
    * @return {@link CompletionStage} with the number of removed properties.
    */
   public final CompletionStage<Integer> remove(K key, Collection<HK> properties) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(properties, ERR_PROPERTY_CANT_BE_NULL);

      if (properties.isEmpty()) return CompletableFuture.completedFuture(0);
      return readWriteMap.eval(key, new HashMapRemoveFunction<>(properties));
   }

   /**
    * Execute the given {@param biConsumer} on the value mapped to the {@param property} stored under {@param key}.
    * <p>
    * If the key is not present, a new entry is created. The {@param biConsumer} is executed until the value is
    * replaced with the new value. This is implementation behaves like {@link Map#compute(Object, BiFunction)}.
    *
    * @param key: The key to retrieve the {@link HashMapBucket}.
    * @param property: The property to be updated.
    * @param biConsumer: Receives the key and previous value and returns the new value.
    * @return A {@link CompletionStage} with the new value returned from {@param biConsumer}.
    * @see Map#compute(Object, BiFunction)
    */
   public CompletionStage<HV> compute(K key, HK property, BiFunction<HK, HV, HV> biConsumer) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(property, ERR_PROPERTY_CANT_BE_NULL);
      return cache.getCacheEntryAsync(key)
            .thenCompose(entry -> {
               HV newValue;
               HV oldValue = null;

               if (entry != null) {
                  oldValue = entry.getValue().get(property);
               }

               newValue = biConsumer.apply(property, oldValue);
               CompletionStage<Boolean> done = readWriteMap.eval(key, new HashMapReplaceFunction<>(property, oldValue, newValue));
               return done.thenCompose(replaced -> {
                  if (replaced)
                     return CompletableFuture.completedFuture(newValue);

                  return compute(key, property, biConsumer);
               });
            });
   }

   /**
    * Select {@param count} key-value pairs from the hash map stored under the given key.
    * <p>
    * The returned values are randomly selected from the underlying map. An {@param count} bigger than the
    * size of the stored map does not raise an exception.
    *
    * @param key: Cache key to retrieve the stored hash map.
    * @param count: Number of key-value pairs to select.
    * @return {@link CompletionStage} containing a {@link Map} with the key-value pairs or null if not found.
    */
   public CompletionStage<Map<HK, HV>> subSelect(K key, int count) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requirePositive(count, ERR_COUNT_MUST_BE_POSITIVE);
      return cache.getCacheEntryAsync(key)
            .thenApply(entry -> {
               if (entry == null) return null;

               Map<HK, HV> converted = entry.getValue().converted();
               if (count >= converted.size()) return converted;

               List<Map.Entry<HK, HV>> entries = new ArrayList<>(converted.entrySet());
               Collections.shuffle(entries, ThreadLocalRandom.current());
               return entries.stream()
                     .limit(count)
                     .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            });
   }

   private static void requirePositive(int value, String message) {
      if (value <= 0) {
         throw new IllegalArgumentException(message);
      }
   }

   private static void requireTrue(boolean condition, String message) {
      if (!condition) {
         throw new IllegalArgumentException(message);
      }
   }

   private static void requireNonNullArgument(Collection<?> arguments, String message) {
      for (Object argument : arguments) {
         requireNonNull(argument, message);
      }
   }
}
