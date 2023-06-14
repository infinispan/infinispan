package org.infinispan.multimap.impl;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.multimap.impl.function.hmap.HashMapPutFunction;

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

   private final FunctionalMap.ReadWriteMap<K, HashMapBucket<HK, HV>> readWriteMap;
   private final AdvancedCache<K, HashMapBucket<HK, HV>> cache;

   public EmbeddedMultimapPairCache(Cache<K, HashMapBucket<HK, HV>> cache) {
      this.cache = cache.getAdvancedCache();
      FunctionalMapImpl<K, HashMapBucket<HK, HV>> functionalMap = FunctionalMapImpl.create(this.cache);
      this.readWriteMap = ReadWriteMapImpl.create(functionalMap);
   }

   /**
    * Set the given key-value pair in the multimap under the given key.
    * </p>
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
               return bucket.values();
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
}
