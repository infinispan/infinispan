package org.infinispan.multimap.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.multimap.impl.function.OfferFunction;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

import static java.util.Objects.requireNonNull;

/**
 * Multimap with Linked List Implementation methods
 *
 * @author  Katia Aresti
 * @since 15.0
 */
public class EmbeddedMultimapListCache<K, V> {
   public static final String ERR_KEY_CAN_T_BE_NULL = "key can't be null";
   public static final String ERR_VALUE_CAN_T_BE_NULL = "value can't be null";
   protected final FunctionalMap.ReadWriteMap<K, ListBucket<V>> readWriteMap;
   protected final AdvancedCache<K, ListBucket<V>> cache;
   protected final InternalEntryFactory entryFactory;

   public EmbeddedMultimapListCache(Cache<K, ListBucket<V>> cache) {
      this.cache = cache.getAdvancedCache();
      FunctionalMapImpl<K, ListBucket<V>> functionalMap = FunctionalMapImpl.create(this.cache);
      this.readWriteMap = ReadWriteMapImpl.create(functionalMap);
      this.entryFactory = this.cache.getComponentRegistry().getInternalEntryFactory().running();
   }

   /**
    * Get the value as a collection
    *
    * @param key, the name of the list
    * @return the collection with values if such exist, or an empty collection if the key is not present
    */
   public CompletionStage<Collection<V>> get(K key) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return getEntry(key).thenApply(entry -> {
         if (entry != null) {
            return entry.getValue();
         }
         return List.of();
      });
   }

   private CompletionStage<CacheEntry<K, Collection<V>>> getEntry(K key) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return cache.getAdvancedCache().getCacheEntryAsync(key)
            .thenApply(entry -> {
               if (entry == null)
                  return null;

               return entryFactory.create(entry.getKey(),(entry.getValue().toDeque()) , entry.getMetadata());
            });
   }

   /**
    * Inserts the specified element at the front of the specified list.
    *
    * @param key, the name of the list
    * @param value, the element to be inserted
    * @return {@link CompletionStage} containing a {@link Void}
    */
   public CompletionStage<Void> offerFirst(K key, V value) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(value, ERR_VALUE_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new OfferFunction<>(value, true));
   }

   /**
    * Inserts the specified element at the end of the specified list.
    *
    * @param key, the name of the list
    * @param value, the element to be inserted
    * @return {@link CompletionStage} containing a {@link Void}
    */
   public CompletionStage<Void> offerLast(K key, V value) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(value, ERR_VALUE_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new OfferFunction<>(value, false));
   }

   /**
    * Returns true if the list associated with the key exists.
    *
    * @param key, the name of the list
    * @return {@link CompletionStage} containing a {@link Boolean}
    */
   public CompletionStage<Boolean> containsKey(K key) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return cache.containsKeyAsync(key);
   }

   /**
    * Returns the number of elements in the list.
    * If the entry does not exit, returns size 0.
    *
    * @param key, the name of the list
    * @return {@link CompletionStage} containing a {@link Long}
    */
   public CompletionStage<Long> size(K key) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return cache.getAsync(key).thenApply(b -> b == null ? 0 : (long) b.size());
   }

   public CompletionStage<Collection<V>> subList(int from, int to) {
      throw new UnsupportedOperationException();
   }

   public CompletionStage<Collection<V>> pollFirst(int count) {
      throw new UnsupportedOperationException();
   }

   public CompletionStage<Collection<V>> pollLast(int count) {
      throw new UnsupportedOperationException();
   }

   public CompletionStage<Void> set(K key, V value, int index) {
      throw new UnsupportedOperationException();
   }
}
