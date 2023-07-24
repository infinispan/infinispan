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
import org.infinispan.multimap.impl.function.sortedset.ScoreFunction;
import org.infinispan.multimap.impl.function.sortedset.SubsetFunction;

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
   public static final String ERR_MEMBER_CAN_T_BE_NULL = "member can't be null";
   public static final String ERR_ARGS_CAN_T_BE_NULL = "args can't be null";
   public static final String ERR_ARGS_INDEXES_CAN_T_BE_NULL = "min and max indexes (from-to) can't be null";
   public static final String ERR_SCORES_CAN_T_BE_NULL = "scores can't be null";
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
    * @param scoreValues, scores and values pair list to be added
    * @param args to provide different options:
    *       addOnly -> adds new elements only, ignore existing ones.
    *       updateOnly -> updates existing elements only, ignore new elements.
    *       updateLessScoresOnly -> creates new elements and updates existing elements if the score is less than current score.
    *       updateGreaterScoresOnly -> creates new elements and updates existing elements if the score is greater than current score.
    *       returnChangedCount -> by default returns number of new added elements. If true, counts created and updated elements.
    * @return {@link CompletionStage} containing the number of entries added and/or updated depending on the provided arguments
    */
   public CompletionStage<Long> addMany(K key, Collection<SortedSetBucket.ScoredValue<V>> scoreValues, SortedSetAddArgs args) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(scoreValues, ERR_SCORES_CAN_T_BE_NULL);
      requireNonNull(args, ERR_ARGS_CAN_T_BE_NULL);
      if (scoreValues.size() == 0 && !args.replace) {
         return CompletableFuture.completedFuture(0L);
      }
      return readWriteMap.eval(key, new AddManyFunction<>(scoreValues, args));
   }

   /**
    * Retieves the size of the sorted set value
    * @param key, the name of the sorted set
    * @return, the size
    */
   public CompletionStage<Long> size(K key) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return cache.getAsync(key).thenApply(b -> b == null ? 0 : b.size());
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

   /**
    * Returns the score of member in the sorted.
    *
    * @param key, the name of the sorted set
    * @param member, the score value to be retrieved
    * @return {@link CompletionStage} with the score, or null if the score of the key does not exist.
    */
   public CompletionStage<Double> score(K key, V member) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(member, ERR_MEMBER_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new ScoreFunction<>(member));
   }

   /**
    * Subset elements in the sorted set by index. Indexes can be explained as negative numbers.
    * Subset can be reversed, depending on args.
    * @param key, the name of the sorted set
    * @param args, options for the operation
    * @return resulting collection
    */
   public CompletionStage<Collection<SortedSetBucket.ScoredValue<V>>> subsetByIndex(K key, SortedSetSubsetArgs<Long> args) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(args, ERR_ARGS_CAN_T_BE_NULL);
      requireNonNull(args.getStart(), ERR_ARGS_INDEXES_CAN_T_BE_NULL);
      requireNonNull(args.getStop(), ERR_ARGS_INDEXES_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new SubsetFunction<>(args, SubsetFunction.SubsetType.INDEX));
   }

   /**
    * Subset elements using the score.
    * Subset can be reversed, depending on args.
    * @param key, the name of the sorted set
    * @param args, options for the operation
    * @return resulting collection
    */
   public CompletionStage<Collection<SortedSetBucket.ScoredValue<V>>> subsetByScore(K key, SortedSetSubsetArgs<Double> args) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(args, ERR_ARGS_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new SubsetFunction<>(args, SubsetFunction.SubsetType.SCORE));
   }

   /**
    * Subset elements using the natural ordering of the values.
    * All the elements in the sorted set must have the same score, so they are ordered by natural ordering.
    * Subset can be reversed, depending on args.
    * @param key, the name of the sorted set
    * @param args, options for the operation
    * @return resulting collection
    */
   public CompletionStage<Collection<SortedSetBucket.ScoredValue<V>>> subsetByLex(K key, SortedSetSubsetArgs<V> args) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(args, ERR_ARGS_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new SubsetFunction<>(args, SubsetFunction.SubsetType.LEX));
   }
}
