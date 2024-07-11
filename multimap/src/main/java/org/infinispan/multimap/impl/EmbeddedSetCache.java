package org.infinispan.multimap.impl;

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.multimap.impl.function.set.SAddFunction;
import org.infinispan.multimap.impl.function.set.SGetFunction;
import org.infinispan.multimap.impl.function.set.SMIsMember;
import org.infinispan.multimap.impl.function.set.SPopFunction;
import org.infinispan.multimap.impl.function.set.SRemoveFunction;
import org.infinispan.multimap.impl.function.set.SSetFunction;

/**
 * SetCache with Set methods implementation
 *
 * @author Vittorio Rigamonti
 * @since 15.0
 */
public class EmbeddedSetCache<K, V> {
   public static final String ERR_KEY_CAN_T_BE_NULL = "key can't be null";
   public static final String ERR_VALUE_CAN_T_BE_NULL = "value can't be null";
   protected final FunctionalMap.ReadWriteMap<K, SetBucket<V>> readWriteMap;
   protected final AdvancedCache<K, SetBucket<V>> cache;
   protected final InternalEntryFactory entryFactory;

   public EmbeddedSetCache(Cache<K, SetBucket<V>> cache) {
      this.cache = cache.getAdvancedCache();
      FunctionalMapImpl<K, SetBucket<V>> functionalMap = FunctionalMapImpl.create(this.cache);
      this.readWriteMap = ReadWriteMapImpl.create(functionalMap);
      this.entryFactory = ComponentRegistry.of(this.cache).getInternalEntryFactory().running();
   }

   /**
    * Get the entry by key and return it as a set
    *
    * @param key, the name of the set
    * @return the set with values if such exist, or null if the key is not present
    */
   public CompletionStage<SetBucket<V>> get(K key) {
      return readWriteMap.eval(key, new SGetFunction<K, V>());
   }

   /**
    * Get the entry by key and return it as a set
    *
    * @param key, the name of the set
    * @return the set with values if such exist, or null if the key is not present
    */
   public CompletionStage<Set<V>> getAsSet(K key) {
      return readWriteMap.eval(key, new SGetFunction<K, V>()).thenApply(v->v.toSet());
   }

   /**
    * Add the specified element to the set, creates the set in case
    *
    * @param key,   the name of the set
    * @param value, the element to be inserted
    * @return {@link CompletionStage} containing a {@link Void}
    */
   public CompletionStage<Long> add(K key, V value) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(value, ERR_VALUE_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new SAddFunction<>(Arrays.asList(value)));
   }

   /**
    * Add the specified elements to the set, creates the set in case
    *
    * @param key,   the name of the set
    * @param value, the element to be inserted
    * @return {@link CompletionStage} containing a {@link Void}
    */
   public CompletionStage<Long> add(K key, Collection<V> values) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(values, ERR_VALUE_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new SAddFunction<>(values));
   }

   /**
    * Remove the specified element to the set, removes the set if empty
    *
    * @param key,   the name of the set
    * @param value, the element to be inserted
    * @return {@link CompletionStage} containing a {@link Void}
    */
   public CompletionStage<Long> remove(K key, V value) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(value, ERR_VALUE_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new SRemoveFunction<>(Arrays.asList(value)));
   }

   /**
    * Remove the specified elements from the set, remove the set if empty
    *
    * @param key,   the name of the set
    * @param value, the element to be inserted
    * @return {@link CompletionStage} containing a {@link Void}
    */
   public CompletionStage<Long> remove(K key, Collection<V> values) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(values, ERR_VALUE_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new SRemoveFunction<>(values));
   }

   /**
    * Get all the entries specified in the keys collection
    *
    * @param keys, collection of keys to be get
    * @return {@link CompletionStage} containing a {@link }
    */
   public CompletableFuture<Map<K, SetBucket<V>>> getAll(Set<K> keys) {
      requireNonNull(keys, ERR_KEY_CAN_T_BE_NULL);
      return cache.getAllAsync(keys);
   }

   /**
    * Returns the number of elements in the set.
    * If the entry does not exit, returns size 0.
    *
    * @param key, the name of the set
    * @return {@link CompletionStage} containing a {@link Long}
    */
   public CompletionStage<Long> size(K key) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return cache.getAsync(key).thenApply(b -> b == null ? 0 : (long) b.size());
   }

   /**
    * Create the set with given values
    *
    * @param key,    the name of the set
    * @param values, the elements to be inserted
    * @return {@link CompletionStage} containing the number of elements
    */
   public CompletionStage<Long> set(K key, Collection<V> values) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(values, ERR_VALUE_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new SSetFunction<>(values));
   }

   /**
    * Return a collection representign a subset of the set
    *
    * @param key,    the name of the set
    * @param count, the number of elements to return
    * @return {@link CompletionStage} the random subset elements
    */
   public CompletionStage<Collection<V>> pop(K key, Long count, boolean remove) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new SPopFunction<K,V>(count != null ? count : 1, remove));
   }

   /**
    * Returns a list L of 0/1. L(i) = 1 if the set contains elements(i)
    *
    * @param key,      the name of the set
    * @param elements, list of elements
    * @return {@link CompletionStage} containing a List<Long> or null if elements
    *         null or empty
    */
   public CompletionStage<List<Long>> mIsMember(K key, V... elements) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      if (elements == null || elements.length == 0) {
         return CompletableFutures.completedNull();
      }
      return readWriteMap.eval(key, new SMIsMember<>(elements));
   }

}
