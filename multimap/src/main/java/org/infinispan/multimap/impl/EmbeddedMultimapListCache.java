package org.infinispan.multimap.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.multimap.impl.function.list.RotateFunction;
import org.infinispan.multimap.impl.function.list.IndexFunction;
import org.infinispan.multimap.impl.function.list.IndexOfFunction;
import org.infinispan.multimap.impl.function.list.InsertFunction;
import org.infinispan.multimap.impl.function.list.OfferFunction;
import org.infinispan.multimap.impl.function.list.PollFunction;
import org.infinispan.multimap.impl.function.list.RemoveCountFunction;
import org.infinispan.multimap.impl.function.list.SetFunction;
import org.infinispan.multimap.impl.function.list.SubListFunction;
import org.infinispan.multimap.impl.function.list.TrimFunction;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
   public static final String ERR_ELEMENT_CAN_T_BE_NULL = "element can't be null";
   public static final String ERR_VALUE_CAN_T_BE_NULL = "value can't be null";
   public static final String ERR_PIVOT_CAN_T_BE_NULL = "pivot can't be null";
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

   /**
    * Returns the element at the given index. Index is zero-based.
    * 0 means fist element. Negative index counts index from the tail. For example -1 is the last element.
    *
    * @param key, the name of the list
    * @param index, the position of the element.
    * @return The existing value. Returns null if the key does not exist or the index is out of bounds.
    */
   public CompletionStage<V> index(K key, long index) {
      requireNonNull(key, "key can't be null");
      return readWriteMap.eval(key, new IndexFunction<>(index));
   }

   /**
    * Retrieves a sub list of elements, starting from 0.
    * Negative indexes point positions counting from the tail of the list.
    * For example 0 is the first element, 1 is the second element, -1 the last element.
    *
    * @param key, the name of the list
    * @param from, the starting offset
    * @param to, the final offset
    * @return The subList. Returns null if the key does not exist.
    */
   public CompletionStage<Collection<V>> subList(K key, long from, long to) {
      requireNonNull(key, "key can't be null");
      return readWriteMap.eval(key, new SubListFunction<>(from, to));
   }

   /**
    * Removes the given count of elements from the head of the list.
    *
    * @param key, the name of the list
    * @param count, the number of elements. Must be positive.
    * @return {@link CompletionStage} containing a {@link Collection<V>} of values removed,
    * or null if the key does not exit
    */
   public CompletionStage<Collection<V>> pollFirst(K key, long count) {
      return poll(key, count, true);
   }

   /**
    * Removes the given count of elements from the tail of the list.
    *
    * @param key, the name of the list
    * @param count, the number of elements. Must be positive.
    * @return {@link CompletionStage} containing a {@link Collection<V>} of values removed,
    * or null if the key does not exit
    */
   public CompletionStage<Collection<V>> pollLast(K key, long count) {
      return poll(key, count, false);
   }

   /**
    * Removes the given count of elements from the tail or the head of the list
    * @param key, the name of the list
    * @param count, the number of elements
    * @param first, true if it's the head, false for the tail
    * @return  {@link CompletionStage} containing a {@link Collection<V>} of values removed,
    * or null if the key does not exit
    */
   public CompletableFuture<Collection<V>> poll(K key, long count, boolean first) {
      requireNonNull(key, "key can't be null");
      requirePositive(count, "count can't be negative");
      return readWriteMap.eval(key, new PollFunction<>(first, count));
   }

   /**
    * Sets a value in the given index.
    * 0 means fist element. Negative index counts index from the tail. For example -1 is the last element.
    * @param key, the name of the list
    * @param index, the position of the element to be inserted. Can be negative
    * @param value, the element to be inserted in the index position
    * @return {@link CompletionStage} with true if the value was set, false if the key does not exist
    * @throws org.infinispan.commons.CacheException when the index is out of range
    */
   public CompletionStage<Boolean> set(K key, long index, V value) {
      requireNonNull(key, "key can't be null");
      return readWriteMap.eval(key, new SetFunction<>(index, value));
   }

   /**
    * Retrieves indexes of matching elements inside a list.
    * Scans the list looking for the elements that match  the provided element.
    *
    * @param key, the name of the list, can't be null.
    * @param element, the element to compare, can't be null.
    * @param count, number of matches. If null, count is 1 by default. Can't be negative.
    *               If count is 0, means all the matches.
    * @param rank, the "rank" of the first element to return, in case there are multiple matches.
    *              A rank of 1 means the first match, 2 the second match, and so forth. Negative rank iterates
    *              from the tail. If null, rank is 1 by default. Can't be 0.
    * @param maxlen, compares the provided element only with a given maximum number of list items.
    *                If null, defaults to 0 that means all the elements. Can't be negative.
    * @return {@link Collection<Long>} containing the zero-based positions in the list counting from the head of the list.
    * Returns null when the list does not exist or empty list when matches are not found.
    *
    */
   public CompletionStage<Collection<Long>> indexOf(K key, V element, Long count, Long rank, Long maxlen) {
      requireNonNull(key, "key can't be null");
      requireNonNull(element, ERR_ELEMENT_CAN_T_BE_NULL);
      long requestedCount = 1;
      long requestedRank = 1;
      long requestedMaxLen = 0;
      if (count != null) {
         requirePositive(count, "count can't be negative");
         requestedCount = count;
      }
      if (rank != null) {
         requireNotZero(rank, "rank can't be zero");
         requestedRank = rank;
      }
      if (maxlen != null) {
         requirePositive(maxlen, "maxLen can't be negative");
         requestedMaxLen = maxlen;
      }

      return readWriteMap.eval(key, new IndexOfFunction<>(element, requestedCount, requestedRank, requestedMaxLen));
   }

   /**
    * Inserts an element before or after the pivot element.
    * If the key does not exist, returns 0.
    * If the pivot does not exist, returns -1.
    * If the element was inserted, returns the size of the list.
    * The list is traversed from head to tail, the insertion is done before or after the first element found.
    *
    * @param key, the name of the list
    * @param isBefore, insert before true, after false
    * @param pivot, the element to compare
    * @param element, the element to insert
    * @return, the size of the list after insertion, 0 or -1
    */
   public CompletionStage<Long> insert(K key, boolean isBefore, V pivot, V element) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(pivot, ERR_PIVOT_CAN_T_BE_NULL);
      requireNonNull(element, ERR_ELEMENT_CAN_T_BE_NULL);

      return readWriteMap.eval(key, new InsertFunction<>(isBefore, pivot, element));
   }

   /**
    * Removes from the list the provided element. The number of copies to be removed is
    * provided by the count parameter. When count is 0, all the elements that match the
    * provided element will be removed. If count is negative, the iteration will be done
    * from the tail of the list instead of the head.
    * count = 0, removes all, iterates over the whole list
    * count = 1, removes one match, starts iteration from the head
    * count = -1, removed one match, starts iteration from the tail
    *
    * @param key, the name of the list
    * @param count, number of elements to remove
    * @param element, the element to remove
    * @return how many elements have actually been removed, 0 if the list does not exist
    */
   public CompletionStage<Long> remove(K key, long count, V element) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(element, ERR_ELEMENT_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new RemoveCountFunction<>(count, element));
   }

   /**
    * Trims the list removing the elements with the provided indexes 'from' and 'to'.
    * Negative indexes point positions counting from the tail of the list.
    * For example 0 is the first element, 1 is the second element, -1 the last element.
    * Iteration is done from head to tail.
    *
    * @param key, the name of the list
    * @param from, the starting offset
    * @param to, the final offset
    * @return {@link CompletionStage<Boolean>} true when the list exist and the trim has been done.
    */
   public CompletionStage<Boolean> trim(K key, long from, long to) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new TrimFunction<>(from, to));
   }

   /**
    * Rotates an element in the list from head to tail or tail to head, depending on the rotateRight parameter.
    * @param key, the name of the list
    * @param rotateRight, true to rotate an element from the left to the right (head -> tail)
    * @return the rotated element value, null if the list does not exist
    */
   public CompletionStage<V> rotate(K key, boolean rotateRight) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new RotateFunction<>(rotateRight));
   }

   private static void requirePositive(long number, String message) {
      if (number < 0) {
         throw new IllegalArgumentException(message);
      }
   }

   private static void requireNotZero(long number, String message) {
      if (number == 0) {
         throw new IllegalArgumentException(message);
      }
   }
}
