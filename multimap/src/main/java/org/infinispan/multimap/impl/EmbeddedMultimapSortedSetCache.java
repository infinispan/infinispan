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
import org.infinispan.multimap.impl.function.sortedset.IncrFunction;
import org.infinispan.multimap.impl.function.sortedset.IndexOfSortedSetFunction;
import org.infinispan.multimap.impl.function.sortedset.PopFunction;
import org.infinispan.multimap.impl.function.sortedset.RemoveManyFunction;
import org.infinispan.multimap.impl.function.sortedset.ScoreFunction;
import org.infinispan.multimap.impl.function.sortedset.SortedSetAggregateFunction;
import org.infinispan.multimap.impl.function.sortedset.SortedSetOperationType;
import org.infinispan.multimap.impl.function.sortedset.SortedSetRandomFunction;
import org.infinispan.multimap.impl.function.sortedset.SubsetFunction;
import org.infinispan.multimap.impl.internal.MultimapObjectWrapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.infinispan.multimap.impl.function.sortedset.SortedSetAggregateFunction.AggregateType.INTER;
import static org.infinispan.multimap.impl.function.sortedset.SortedSetAggregateFunction.AggregateType.UNION;
import static org.infinispan.multimap.impl.function.sortedset.SortedSetOperationType.INDEX;
import static org.infinispan.multimap.impl.function.sortedset.SortedSetOperationType.LEX;
import static org.infinispan.multimap.impl.function.sortedset.SortedSetOperationType.OTHER;
import static org.infinispan.multimap.impl.function.sortedset.SortedSetOperationType.SCORE;

/**
 * Multimap with Sorted Map Implementation methods
 *
 * @author  Katia Aresti
 * @since 15.0
 */
public class EmbeddedMultimapSortedSetCache<K, V> {
   public static final String ERR_KEY_CAN_T_BE_NULL = "key can't be null";
   public static final String ERR_MIN_CAN_T_BE_NULL = "min can't be null";
   public static final String ERR_MAX_CAN_T_BE_NULL = "max can't be null";
   public static final String ERR_MEMBER_CAN_T_BE_NULL = "member can't be null";
   public static final String ERR_ARGS_CAN_T_BE_NULL = "args can't be null";
   public static final String ERR_ARGS_INDEXES_CAN_T_BE_NULL = "min and max indexes (from-to) can't be null";
   public static final String ERR_MEMBERS_CAN_T_BE_NULL = "members can't be null";
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
    * @param scoredValues, scores and values pair list to be added
    * @param args to provide different options:
    *       addOnly -> adds new elements only, ignore existing ones.
    *       updateOnly -> updates existing elements only, ignore new elements.
    *       updateLessScoresOnly -> creates new elements and updates existing elements if the score is less than current score.
    *       updateGreaterScoresOnly -> creates new elements and updates existing elements if the score is greater than current score.
    *       returnChangedCount -> by default returns number of new added elements. If true, counts created and updated elements.
    * @return {@link CompletionStage} containing the number of entries added and/or updated depending on the provided arguments
    */
   public CompletionStage<Long> addMany(K key, Collection<SortedSetBucket.ScoredValue<V>> scoredValues, SortedSetAddArgs args) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(scoredValues, ERR_SCORES_CAN_T_BE_NULL);
      requireNonNull(args, ERR_ARGS_CAN_T_BE_NULL);
      if (scoredValues.size() == 0 && !args.replace) {
         return CompletableFuture.completedFuture(0L);
      }
      return readWriteMap.eval(key, new AddManyFunction<>(scoredValues, args));
   }

   public CompletionStage<Double> incrementScore(K key, double score, V member, SortedSetAddArgs args) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(member, ERR_MEMBER_CAN_T_BE_NULL);
      requireNonNull(args, ERR_ARGS_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new IncrFunction<>(score, member, args));
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

   public CompletionStage<CacheEntry<K, Collection<SortedSetBucket.ScoredValue<V>>>> getEntry(K key) {
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
    * Returns the sorted set value if such exists.
    *
    * @param key, the name of the sorted set
    * @return the value of the Sorted Set
    */
   public CompletionStage<List<SortedSetBucket.ScoredValue<V>>> getValueAsList(K key) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return cache.getAsync(key).thenApply(b -> {
         if (b != null) {
            return b.getScoredEntriesAsList();
         }
         return null;
      });
   }

   /**
    * Returns the set values if such exists.
    *
    * @param key, the name of the sorted set
    * @return the values in the sorted set as a set
    */
   public CompletionStage<Set<MultimapObjectWrapper<V>>> getValuesSet(K key) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return cache.getAsync(key).thenApply(b -> {
         if (b != null) {
            return b.getScoredEntriesAsValuesSet();
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
            new CountFunction<>(min, includeMin, max, includeMax, SCORE));
   }

   /**
    * Counts the number of elements between the given min and max scores.
    *
    * @param key, the name of the sorted set
    * @param min, the min value
    * @param max, the max value
    * @param includeMin, include elements with the min value in the count
    * @param includeMax, include elements with the max value in the count
    * @return the number of elements in between min and max values
    */
   public CompletionStage<Long> count(K key, V min, boolean includeMin, V max, boolean includeMax) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return readWriteMap.eval(key,
            new CountFunction<>(min, includeMin, max, includeMax, LEX));
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
    * @return {@link CompletionStage} with the score, or null if the score of the member does not exist.
    */
   public CompletionStage<Double> score(K key, V member) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(member, ERR_MEMBER_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new ScoreFunction<>(Collections.singletonList(member))).thenApply(c ->{
         if (c == null || c.isEmpty()) {
            return null;
         }
         return c.get(0);
      });
   }

   /**
    * Returns the scores of members in the sorted.
    *
    * @param key, the name of the sorted set
    * @param members, the scores to be retrieved
    * @return {@link CompletionStage} with the list of the scores, with null values if the score does not exist.
    */
   public CompletionStage<List<Double>> scores(K key, List<V> members) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(members, ERR_MEMBERS_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new ScoreFunction<>(members)).thenApply(c ->{
         if (c == null || c.isEmpty()) {
            return members.stream().map(m -> (Double) null).collect(Collectors.toList());
         } else {
            return c;
         }
      });
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
      return readWriteMap.eval(key, new SubsetFunction<>(args, INDEX));
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
      return readWriteMap.eval(key, new SubsetFunction<>(args, SCORE));
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
      return readWriteMap.eval(key, new SubsetFunction<>(args, LEX));
   }

   /**
    * Returns the index of member in the sorted set, with the scores ordered from high to low.
    * Index is 0-based.
    * When isRev is false, the member with the lowest score has index 0.
    * When isRev is true, the member with the highest score has index 0
    * @param key, the name of the sorted set
    * @param member, the member to be found
    * @param isRev, perform the operation in reverse order
    * @return the index of the member and the score
    */
   public CompletionStage<SortedSetBucket.IndexValue> indexOf(K key, V member, boolean isRev) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(member, ERR_MEMBER_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new IndexOfSortedSetFunction(member, isRev));
   }

   /**
    * Computes the union of the collection and the given sorted sets key, if such exist.
    *
    * @param key, the name of the sorted set
    * @param scoredValues, collection of scored values
    * @param weight, specify a multiplication factor for each input sorted set. Every element in the
    *                sorted set is multiplied by this factor.
    * @param aggFunction, how the results of the union are aggregated. Defaults to SUM.
    * @return, union collection, sorted by score
    */
   public CompletionStage<Collection<SortedSetBucket.ScoredValue<V>>> union(K key, Collection<SortedSetBucket.ScoredValue<V>> scoredValues,
                                                                      double weight,
                                                                      SortedSetBucket.AggregateFunction aggFunction) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      SortedSetBucket.AggregateFunction agg = aggFunction == null ? SortedSetBucket.AggregateFunction.SUM : aggFunction;
      return readWriteMap.eval(key, new SortedSetAggregateFunction(UNION, scoredValues, weight, agg));
   }


   /**
    * Computes the intersection of the collection and the given sorted sets key, if such exist.
    *
    * @param key, the name of the sorted set
    * @param scoredValues, collection of scored values
    * @param weight, specify a multiplication factor for each input sorted set. Every element in the
    *                sorted set is multiplied by this factor.
    * @param aggFunction, how the results of the union are aggregated. Defaults to SUM.
    * @return, intersected collection, sorted by score
    */
   public CompletionStage<Collection<SortedSetBucket.ScoredValue<V>>> inter(K key, Collection<SortedSetBucket.ScoredValue<V>> scoredValues,
                                                                            double weight,
                                                                            SortedSetBucket.AggregateFunction aggFunction) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      SortedSetBucket.AggregateFunction agg = aggFunction == null ? SortedSetBucket.AggregateFunction.SUM : aggFunction;
      return readWriteMap.eval(key, new SortedSetAggregateFunction(INTER, scoredValues, weight, agg));
   }
   /**
    * Removes the given elements from the sorted set, if such exist.
    * @param key, the name of the sorted set
    * @param members, members to be removed
    * @return removed members count
    */
   public CompletionStage<Long> removeAll(K key, List<V> members) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      requireNonNull(members, ERR_MEMBERS_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new RemoveManyFunction<>(members, OTHER));
   }

   /**
    * Removes range from index from to index to.
    *
    * @param key, the name of the sorted set
    * @param min, index from. Index can't be null and must be provided
    * @param max, index to. Index can't be null and must be provided
    * @return long representing the number of members removed
    */
   public CompletionStage<Long> removeAll(K key, Long min, Long max) {
      requireNonNull(min, ERR_MIN_CAN_T_BE_NULL);
      requireNonNull(max, ERR_MAX_CAN_T_BE_NULL);
      return removeAll(key, min, false,  max, false, INDEX);
   }

   /**
    * Removes elements from a range of scores.
    *
    * @param key, the sorted set name
    * @param min, smallest score value to be removed. If null, removes from the head of the sorted set
    * @param includeMin, indicates if the min score is included in the remove range
    * @param max, greatest score value to be removed. If null, removes to the tail of the sorted set
    * @param includeMax, indicates if the max score is included in the remove range
    *
    * @return the number of removed elements
    */
   public CompletionStage<Long> removeAll(K key, Double min, boolean includeMin, Double max, boolean includeMax) {
      return removeAll(key, min, includeMin, max, includeMax, SCORE);
   }

   /**
    * When the elements have the same score, removes elements from a range of elements ordered by elements
    * natural ordering.
    *
    * @param key, the sorted set name
    * @param min, smallest element to be removed. If null, removes from the head of the sorted set
    * @param includeMin, indicates if the smallest element is included in the remove range
    * @param max, greatest element to be removed. If null, removes to the tail of the sorted set
    * @param includeMax, indicates if the greater element is included in the remove range
    *
    * @return the number of removed elements
    */
   public CompletionStage<Long> removeAll(K key, V min, boolean includeMin, V max, boolean includeMax) {
      return removeAll(key, min, includeMin, max, includeMax, LEX);
   }

   private CompletionStage<Long> removeAll(K key, Object min, boolean includeMin, Object max, boolean includeMax, SortedSetOperationType subsetType) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      List<Object> list = new ArrayList<>(2);
      list.add(min);
      list.add(max);
      return readWriteMap.eval(key, new RemoveManyFunction<>(list, includeMin, includeMax, subsetType));
   }

   /**
    * If the count argument is positive, return an array of distinct elements. The
    * If the count argument is negative, it is allowed to return the same element multiple times.
    * In this case, the number of returned elements is the absolute value of the specified count.
    * @param key, the sorted set name
    * @param count, number of random members to retrieve
    * @return, collection of the random scored entries
    */
   public CompletionStage<List<SortedSetBucket.ScoredValue<V>>> randomMembers(K key, int count) {
      requireNonNull(key, ERR_KEY_CAN_T_BE_NULL);
      return readWriteMap.eval(key, new SortedSetRandomFunction(count));
   }
}
