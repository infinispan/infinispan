package org.infinispan.multimap.impl;

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.multimap.impl.function.AddFunction;

/**
 * SetCache with Set Implementation methods
 *
 * @author  Vittorio Rigamonti
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
      this.entryFactory = this.cache.getComponentRegistry().getInternalEntryFactory().running();
   }

   /**
    * Get the value as a collection
    *
    * @param key, the name of the set
    * @return the collection with values if such exist, or an empty collection if the key is not present
    */
   public CompletionStage<Collection<V>> get(K key) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return getEntry(key).thenApply(entry -> {
         if (entry != null) {
            return entry.getValue();
         }
         return Set.of();
      });
   }

   private CompletionStage<CacheEntry<K, Collection<V>>> getEntry(K key) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return cache.getAdvancedCache().getCacheEntryAsync(key)
            .thenApply(entry -> {
               if (entry == null)
                  return null;

               return entryFactory.create(entry.getKey(),(entry.getValue().toSet()) , entry.getMetadata());
            });
   }

   /**
    * Add the specified element to the set
    *
    * @param key, the name of the set
    * @param value, the element to be inserted
    * @return {@link CompletionStage} containing a {@link Void}
    */
   public CompletionStage<Boolean> add(K key, V value) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(value, ERR_VALUE_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new AddFunction<>(value));
   }

   /**
    * Returns the number of elements in the set.
    * If the entry does not exit, returns size 0.
    *
    * @param key, the name of the list
    * @return {@link CompletionStage} containing a {@link Long}
    */
   public CompletionStage<Long> size(K key) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return cache.getAsync(key).thenApply(b -> b == null ? 0 : (long) b.size());
   }

   public CompletionStage<Void> set(K key, V value, int index) {
      throw new UnsupportedOperationException();
   }
}
