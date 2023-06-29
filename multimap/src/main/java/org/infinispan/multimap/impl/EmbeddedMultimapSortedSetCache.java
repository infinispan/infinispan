package org.infinispan.multimap.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.multimap.impl.function.sortedset.AddManyFunction;
import org.infinispan.multimap.impl.function.sortedset.CountFunction;
import org.infinispan.multimap.impl.function.sortedset.PopFunction;

import java.util.Collection;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static java.util.Objects.requireNonNull;

/**
 * Multimap with Sorted Map Implementation methods
 *
 * @author  Katia Aresti
 * @since 15.0
 */
public class EmbeddedMultimapSortedSetCache<K, V> {
   public static final String ERR_KEY_CAN_T_BE_NULL = "key can't be null";
   public static final String ERR_SCORES_CAN_T_BE_NULL = "scores can't be null";
   public static final String ERR_VALUES_CAN_T_BE_NULL = "values can't be null";
   public static final String ERR_SCORES_VALUES_MUST_HAVE_SAME_SIZE = "scores and values must have the same size";
   protected final FunctionalMap.ReadWriteMap<K, SortedSetBucket<V>> readWriteMap;
   protected final AdvancedCache<K, SortedSetBucket<V>> cache;
   protected final InternalEntryFactory entryFactory;

   public EmbeddedMultimapSortedSetCache(Cache<K, SortedSetBucket<V>> cache) {
      this.cache = cache.getAdvancedCache();
      FunctionalMapImpl<K, SortedSetBucket<V>> functionalMap = FunctionalMapImpl.create(this.cache);
      this.readWriteMap = ReadWriteMapImpl.create(functionalMap);
      this.entryFactory = this.cache.getComponentRegistry().getInternalEntryFactory().running();
   }

   /**
    * Adds and/or updates, depending on the provided options, the value and the associated score.
    *
    * @param key, the name of the sorted set
    * @param scores, the score for each of the elements in the values
    * @param values, the values to be added
    * @param args to provide different options:
    *       addOnly -> adds new elements only, ignore existing ones.
    *       updateOnly -> updates existing elements only, ignore new elements.
    *       updateLessScoresOnly -> creates new elements and updates existing elements if the score is less than current score.
    *       updateGreaterScoresOnly -> creates new elements and updates existing elements if the score is greater than current score.
    *       returnChangedCount -> by default returns number of new added elements. If true, counts created and updated elements.
    * @return {@link CompletionStage} containing the number of entries added and/or updated depending on the provided arguments
    */
   public CompletionStage<Long> addMany(K key, double[] scores, V[] values, SortedSetAddArgs args) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(scores, ERR_SCORES_CAN_T_BE_NULL);
      requireNonNull(values, ERR_VALUES_CAN_T_BE_NULL);
      requireSameLength(scores, values);
      if (scores.length == 0) {
         return CompletableFuture.completedFuture(0L);
      }

      return readWriteMap.eval(key,
            new AddManyFunction<>(scores, values, args.addOnly, args.updateOnly, args.updateLessScoresOnly,
                  args.updateGreaterScoresOnly, args.returnChangedCount));
   }

   /**
    * Retieves the size of the sorted set value
    * @param key, the name of the sorted set
    * @return, the size
    */
   public CompletionStage<Long> size(K key) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return cache.getAsync(key).thenApply(b -> b == null ? 0 : (long) b.size());
   }

   /**
    * Get the value as a collection
    *
    * @param key, the name of the list
    * @return the collection with values if such exist, or an empty collection if the key is not present
    */
   public CompletionStage<Collection<SortedSetBucket.ScoredValue<V>>> get(K key) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return getEntry(key).thenApply(entry -> {
         if (entry != null) {
            return entry.getValue();
         }
         return Set.of();
      });
   }

   private CompletionStage<CacheEntry<K, Collection<SortedSetBucket.ScoredValue<V>>>> getEntry(K key) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return cache.getAdvancedCache().getCacheEntryAsync(key)
            .thenApply(entry -> {
               if (entry == null)
                  return null;

               return entryFactory.create(entry.getKey(),(entry.getValue().toTreeSet()) , entry.getMetadata());
            });
   }

   /**
    * Returns the sorted set value if such exists.
    *
    * @param key, the name of the sorted set
    * @return the value of the Sorted Set
    */
   public CompletionStage<SortedSet<SortedSetBucket.ScoredValue<V>>> getValue(K key) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return cache.getAsync(key).thenApply(b -> {
         if (b != null) {
            return b.getScoredEntries();
         }
         return null;
      });
   }

   /**
    * Counts the number of elements between the given min and max scores.
    *
    * @param key, the name of the sorted set
    * @param min, the min score
    * @param max, the max score
    * @param includeMin, include elements with the min score in the count
    * @param includeMax, include elements with the max score in the count
    * @return the number of elements in between min and max scores
    */
   public CompletionStage<Long> count(K key, double min, boolean includeMin, double max, boolean includeMax) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return readWriteMap.eval(key,
            new CountFunction<>(min, includeMin, max, includeMax));
   }

   /**
    * Pops the number of elements provided by the count parameter.
    * Elements are pop from the head or the tail, depending on the min parameter.
    * @param key, the sorted set name
    * @param min, if true pops lower scores, if false pops higher scores
    * @param count, number of values
    * @return, empty if the sorted set does not exist
    */
   public CompletionStage<Collection<SortedSetBucket.ScoredValue<V>>> pop(K key, boolean min, long count) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new PopFunction<>(min, count));
   }

   private void requireSameLength(double[] scores, V[] values) {
      if (scores.length != values.length) {
         throw new IllegalArgumentException(ERR_SCORES_VALUES_MUST_HAVE_SAME_SIZE);
      }
   }
}
