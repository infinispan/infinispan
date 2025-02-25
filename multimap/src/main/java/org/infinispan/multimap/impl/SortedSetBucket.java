package org.infinispan.multimap.impl;

import static org.infinispan.commons.marshall.ProtoStreamTypeIds.MULTIMAP_INDEX_VALUE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.multimap.impl.internal.MultimapObjectWrapper;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Bucket used to store Sorted Set data type.
 *
 * @author Katia Aresti
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_SORTED_SET_BUCKET)
public class SortedSetBucket<V> implements SortableBucket<V>, BaseSetBucket<V> {
   private final TreeSet<ScoredValue<V>> scoredEntries;
   private final Map<MultimapObjectWrapper<V>, Double> entries;

   @Proto
   @ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_SORTED_SET_BUCKET_AGGREGATE_FUNCTION)
   public enum AggregateFunction {
      /**
       *
       */
      SUM {
         @Override
         public double apply(double first, double second) {
            return first + second;
         }
      }, MIN {
         @Override
         public double apply(double first, double second) {
            return Math.min(first, second);
         }
      }, MAX {
         @Override
         public double apply(double first, double second) {
            return Math.max(first, second);
         }
      };

      public abstract double apply(double first, double second);
   }

   @Override
   public Set<ScoredValue<V>> getAsSet() {
      return scoredEntries;
   }

   @Override
   public List<ScoredValue<V>> getAsList() {
      return getScoredEntriesAsList();
   }

   @Override
   public Double getScore(MultimapObjectWrapper<V> key) {
      return entries.get(key);
   }

   public List<ScoredValue<V>> randomMembers(int count) {
      if (count == 1 || count == -1) {
         int rank = ThreadLocalRandom.current().nextInt(scoredEntries.size());
         return this.subsetByIndex(rank, rank, false);
      }

      if (count < 0) {
         // we allow duplicates and returns count size random entries
         int totalCount = Math.abs(count);
         List<ScoredValue<V>> randomEntries = new ArrayList<>(totalCount);
         ThreadLocalRandom.current().ints(totalCount, 0, entries.size())
               .forEach(randomPos -> randomEntries.add(this.subsetByIndex(randomPos, randomPos, false).get(0)));
         return randomEntries;
      }

      // duplicates are not allowed.
      List<Integer> positions = new ArrayList<>(entries.size());
      while (positions.size() < entries.size()) {
         positions.add(positions.size());
      }
      Collections.shuffle(positions);

      List<ScoredValue<V>> randomEntries = new ArrayList<>();
      Iterator<Integer> ite = positions.iterator();
      while (randomEntries.size() < count && randomEntries.size() < entries.size()) {
         Integer pos = ite.next();
         randomEntries.add(this.subsetByIndex(pos, pos, false).get(0));
      }

      return randomEntries;
   }


   @ProtoFactory
   SortedSetBucket(Collection<ScoredValue<V>> wrappedValues) {
      scoredEntries = new TreeSet<>();
      scoredEntries.addAll(wrappedValues);
      entries = new HashMap<>();
      wrappedValues.forEach(e -> entries.put(e.wrappedValue(), e.score()));
   }

   @ProtoField(number = 1, collectionImplementation = ArrayList.class)
   Collection<ScoredValue<V>> getWrappedValues() {
      return new ArrayList<>(scoredEntries);
   }

   /**
    * Returns a copy of the entries;
    * @return entries copy
    */
   public SortedSet<ScoredValue<V>> getScoredEntries() {
      return new TreeSet<>(scoredEntries);
   }

   /**
    * Returns a copy of the entries;
    * @return entries copy
    */
   public List<ScoredValue<V>> getScoredEntriesAsList() {
      return new ArrayList<>(scoredEntries);
   }

   public SortedSetBucket() {
      this.scoredEntries = new TreeSet<>();
      this.entries = new HashMap<>();
   }

   public SortedSetResult<Collection<ScoredValue<V>>, V> pop(boolean min, long count) {
      Iterator<ScoredValue<V>> it = min
            ? scoredEntries.iterator()
            : scoredEntries.descendingIterator();

      int i = 0;
      List<ScoredValue<V>> popped = new ArrayList<>();
      List<ScoredValue<V>> remaining = new ArrayList<>();
      while (it.hasNext()) {
         ScoredValue<V> sv = it.next();
         if (++i > count) {
            remaining.add(sv);
         } else {
            popped.add(sv);
         }
      }
      return new SortedSetResult<>(popped, new SortedSetBucket<>(remaining));
   }

   public List<Double> scores(List<V> members) {
     return members.stream().map(m -> entries.get(new MultimapObjectWrapper<>(m))).collect(Collectors.toList());
   }

   public IndexValue indexOf(V member, boolean isRev) {
      MultimapObjectWrapper<V> wrapMember = new MultimapObjectWrapper<>(member);
      Double score = entries.get(wrapMember);
      if (score == null) {
         return null;
      }
      SortedSet<ScoredValue<V>> tailedHead = scoredEntries.headSet(new ScoredValue<>(score, wrapMember));
      return isRev? IndexValue.of(score, scoredEntries.size() - tailedHead.size() - 1)
            : IndexValue.of(score, tailedHead.size());
   }

   public SortedSetBucket<V> replace(Collection<ScoredValue<V>> scoredValues) {
      return new SortedSetBucket<>(scoredValues);
   }

   public static class AddOrUpdatesCounters {

      public long created = 0;

      public long updated = 0;
   }
   public SortedSetResult<AddOrUpdatesCounters, V> addMany(Collection<ScoredValue<V>> scoredValues,
                                       boolean addOnly,
                                       boolean updateOnly,
                                       boolean updateLessScoresOnly,
                                       boolean updateGreaterScoresOnly) {

      AddOrUpdatesCounters addResult = new AddOrUpdatesCounters();
      SortedSetBucket<V> next = new SortedSetBucket<>(scoredEntries);
      long startSize = next.size();

      for (ScoredValue<V> scoredValue : scoredValues) {
         if (addOnly) {
            next.addOnly(scoredValue);
         } else if (updateOnly && !updateGreaterScoresOnly && !updateLessScoresOnly) {
            next.updateOnly(addResult, scoredValue);
         } else if (updateGreaterScoresOnly) {
            next.addOrUpdateGreaterScores(updateOnly, addResult, scoredValue);
         } else if (updateLessScoresOnly) {
            next.addOrUpdateLessScores(updateOnly, addResult, scoredValue);
         } else {
            next.addOrUpdate(addResult, scoredValue);
         }
      }
      addResult.created = next.size() - startSize;
      return new SortedSetResult<>(addResult, next);
   }

   public SortedSetResult<Double, V> incrScore(double incr, V member, boolean addOnly, boolean updateOnly, boolean updateLessScoresOnly, boolean updateGreaterScoresOnly) {
      MultimapObjectWrapper<V> wrappedValue = new MultimapObjectWrapper<>(member);
      Double existingScore = entries.get(wrappedValue);
      if ((existingScore != null && addOnly) || (existingScore == null && updateOnly)) {
         // do nothing
         return null;
      }

      Double newScore = existingScore == null ? incr : existingScore + incr;
      if (existingScore != null) {
         if ((updateGreaterScoresOnly && newScore <= existingScore) || (updateLessScoresOnly && newScore >= existingScore)) {
            // do nothing;
            return null;
         }

         if (Double.isNaN(newScore))
            throw new IllegalStateException("resulting score is not a number (NaN)");
      }

      SortedSetBucket<V> next = new SortedSetBucket<>(scoredEntries);
      next.addOrUpdate(new AddOrUpdatesCounters(), new ScoredValue<>(newScore, wrappedValue));
      return new SortedSetResult<>(newScore, next);
   }

   private void addOnly(ScoredValue<V> scoredValue) {
      Double existingScore = entries.get(scoredValue.wrappedValue());
      if (existingScore == null){
         addScoredValue(scoredValue);
      }
   }

   private void updateOnly(AddOrUpdatesCounters addResult, ScoredValue<V> scoredValue) {
      Double existingScore = entries.get(scoredValue.wrappedValue());
      if (existingScore != null && !existingScore.equals(scoredValue.score())) {
         updateScoredValue(scoredValue, existingScore);
         addResult.updated++;
      }
   }

   private void addOrUpdateGreaterScores(boolean updateOnly, AddOrUpdatesCounters addResult, ScoredValue<V> scoredValue) {
      Double existingScore = entries.get(scoredValue.wrappedValue());
      if (existingScore == null && !updateOnly) {
         addScoredValue(scoredValue);
      } else if (existingScore != null && scoredValue.score() > existingScore) {
         updateScoredValue(scoredValue, existingScore);
         addResult.updated++;
      }
   }

   private void addOrUpdateLessScores(boolean updateOnly, AddOrUpdatesCounters addResult, ScoredValue<V> scoredValue) {
      Double existingScore = entries.get(scoredValue.wrappedValue());
      if (existingScore == null && !updateOnly) {
         addScoredValue(scoredValue);
      } else if (existingScore != null && scoredValue.score() < existingScore) {
         updateScoredValue(scoredValue, existingScore);
         addResult.updated++;
      }
   }

   private void addOrUpdate(AddOrUpdatesCounters addResult, ScoredValue<V> scoredValue) {
      Double existingScore = entries.get(scoredValue.wrappedValue());
      if (existingScore == null) {
         addScoredValue(scoredValue);
      } else if (!scoredValue.score().equals(existingScore)) {
         // entry exists, check score
         updateScoredValue(scoredValue, existingScore);
         addResult.updated++;
      }
   }

   private void updateScoredValue(ScoredValue<V> newScoredValue, Double existingScore) {
      ScoredValue<V> oldScoredValue = new ScoredValue<>(existingScore, newScoredValue.wrappedValue());
      scoredEntries.remove(oldScoredValue);
      scoredEntries.add(newScoredValue);
      entries.put(newScoredValue.wrappedValue(), newScoredValue.score());
   }

   private void addScoredValue(ScoredValue<V> scoredValue) {
      scoredEntries.add(scoredValue);
      entries.put(scoredValue.wrappedValue(), scoredValue.score());
   }

   public SortedSetResult<Long, V> removeAll(Collection<V> values) {
      Collection<ScoredValue<V>> subset = new ArrayList<>();
      for (V value: values) {
         MultimapObjectWrapper<V> wrappedValue = new MultimapObjectWrapper<>(value);
         Double score = entries.get(wrappedValue);
         if (score != null)
            subset.add(new ScoredValue<>(score, wrappedValue));
      }
      return removeAllInternal(subset);
   }

   public SortedSetResult<Long, V> removeAll(V min, boolean includeMin, V max, boolean includeMax) {
      List<ScoredValue<V>> subset = subset(min, includeMin, max, includeMax, false, null, null);
      return removeAllInternal(subset);
   }

   public SortedSetResult<Long, V> removeAll(Double min, boolean includeMin, Double max, boolean includeMax) {
      List<ScoredValue<V>> subset = subset(min, includeMin, max, includeMax, false, null, null);
      return removeAllInternal(subset);
   }

   public SortedSetResult<Long, V> removeAll(Long min, Long max) {
      List<ScoredValue<V>> subset = subsetByIndex(min, max, false);
      return removeAllInternal(subset);
   }

   private SortedSetResult<Long, V> removeAllInternal(Collection<ScoredValue<V>> subset) {
      if (subset.isEmpty())
         return new SortedSetResult<>(0L, this);

      List<ScoredValue<V>> remaining = new ArrayList<>(scoredEntries.size());
      for (ScoredValue<V> sv : scoredEntries) {
         if (!subset.contains(sv)) remaining.add(sv);
      }
      long size = subset.size();
      return new SortedSetResult<>(size, new SortedSetBucket<>(remaining));
   }

   public List<ScoredValue<V>> subsetByIndex(long from, long to, boolean rev) {
      // from and to are + but from is bigger
      // example: from 2 > to 1 -> empty result
      // from and to are - and to is smaller
      // example: from -1 > to -2 -> empty result
      if ((from > 0 && to > 0 && from > to) || (from < 0 && to < 0 && from > to)) {
         return Collections.emptyList();
      }

      long fromIte = from < 0 ? scoredEntries.size() + from : from;
      long toIte = to < 0 ? scoredEntries.size() + to : to;

      if (fromIte > toIte) {
         return Collections.emptyList();
      }

      List<ScoredValue<V>> results = new ArrayList<>();
      Iterator<ScoredValue<V>> ite;
      if (rev) {
         ite = scoredEntries.descendingIterator();
      } else {
         ite = scoredEntries.iterator();
      }

      long pos = 0;
      while ((pos < fromIte) && ite.hasNext()) {
         ite.next();
         pos++;
      }

      while ((pos <= toIte) && ite.hasNext()) {
         results.add(ite.next());
         pos++;
      }

      return results;
   }

   @SuppressWarnings({ "unchecked", "rawtypes" })
   public List<ScoredValue<V>> subset(Double startScore, boolean includeStart, Double stopScore, boolean includeStop, boolean isRev, Long offset, Long count) {
      if ((stopScore != null && stopScore.equals(startScore) && (!includeStart || !includeStop)) || (count != null && count == 0) || (offset != null && offset.equals(entries.size()))) {
         return Collections.emptyList();
      }

      Double min = isRev ? stopScore : startScore;
      boolean includeMin = isRev ? includeStop : includeStart;
      Double max = isRev ? startScore : stopScore;
      boolean includeMax = isRev ? includeStart : includeStop;
      boolean unboundedMin = min == null || min == Double.MIN_VALUE;
      boolean unboundedMax = max == null || max == Double.MAX_VALUE;

      if (unboundedMin && unboundedMax) {
         return applyLimit(scoredEntries, offset, count, isRev);
      }

      ScoredValue<V> startSv;
      ScoredValue<V> stopSv;
      if (unboundedMin) {
         startSv = scoredEntries.first();
      } else {
         if (includeMin) {
            startSv = scoredEntries.lower(ScoredValue.of(min));
            if (startSv == null) {
               startSv = scoredEntries.first();
            }
         } else {
            startSv = scoredEntries.higher(ScoredValue.of(min));
            if (startSv == null) {
               startSv = scoredEntries.last();
            }
         }
      }

      if (unboundedMax) {
         stopSv = scoredEntries.last();
      } else {
         if (includeMax) {
            stopSv = scoredEntries.higher(ScoredValue.of(max));
         } else {
            stopSv = scoredEntries.lower(ScoredValue.of(max));
         }

         if (stopSv == null) {
            stopSv = scoredEntries.last();
         }
      }

      if (startSv.score() > stopSv.score()) {
         return Collections.emptyList();
      }

      NavigableSet<ScoredValue<V>> subset = scoredEntries.subSet(
            startSv,
            unboundedMin || (startSv.score() > min || (includeMin && startSv.score().equals(min))),
            stopSv,
            unboundedMax || (stopSv.score() < max || (includeMax && stopSv.score().equals(max))));

      return applyLimit(subset, offset, count, isRev);
   }

   public List<ScoredValue<V>> subset(V startValue, boolean includeStart, V stopValue, boolean includeStop, boolean isRev, Long offset, Long count) {
      V minValue = isRev ? stopValue : startValue;
      V maxValue = isRev ? startValue : stopValue;
      boolean includeMin = isRev ? includeStop : includeStart;
      boolean includeMax = isRev ? includeStart : includeStop;

      if (maxValue != null && maxValue.equals(minValue) && (!includeMin || !includeMax) || (offset != null && offset.equals(entries.size()) || (count!= null && count == 0))) {
         return Collections.emptyList();
      }
      boolean unboundedMin = minValue == null;
      boolean unboundedMax = maxValue == null;

      if (unboundedMin && unboundedMax) {
         return applyLimit(scoredEntries, offset, count, isRev);
      }
      // if all the scoredEntries have the same score, then we can pick up first score for lex
      // when all the entries don't have the same score, this method can't work. This is the expected behaviour.
      double score = scoredEntries.first().score();

      ScoredValue<V> minScoredValue = ScoredValue.of(score, minValue);
      ScoredValue<V> maxScoredValue = ScoredValue.of(score, maxValue);

      if (unboundedMin) {
         NavigableSet<ScoredValue<V>> entries = scoredEntries.headSet(maxScoredValue, includeMax);
         return applyLimit(entries, offset, count, isRev);
      }

      if (unboundedMax) {
         NavigableSet<ScoredValue<V>> entries = scoredEntries.tailSet(minScoredValue, includeMin);
         return applyLimit(entries, offset, count, isRev);
      }

      try {
         NavigableSet<ScoredValue<V>> entries = scoredEntries.subSet(minScoredValue, includeMin, maxScoredValue, includeMax);
         return applyLimit(entries, offset, count, isRev);
      } catch (IllegalArgumentException e) {
         return Collections.emptyList();
      }
   }

   private List<ScoredValue<V>> applyLimit(NavigableSet<ScoredValue<V>> subset, final Long offset, final Long count, boolean isRev) {
      if (!isLimited(offset, count)) {
         List<ScoredValue<V>> result = new ArrayList<>(entries.size());
         Iterator<ScoredValue<V>> ite = isRev ? subset.descendingIterator() : subset.iterator();
         while (ite.hasNext()) {
            result.add(ite.next());
         }
         return result;
      }

      List<ScoredValue<V>> result = new ArrayList<>();
      Iterator<ScoredValue<V>> ite = isRev ? subset.descendingIterator() : subset.iterator();
      if (count < 0) {
         skipOffset(offset, ite);
         while (ite.hasNext()) {
            result.add(ite.next());
         }
      } else {
         skipOffset(offset, ite);
         long localCount = 0;
         while (ite.hasNext() && localCount++ < count) {
            result.add(ite.next());
         }
      }
      return result;
   }

   private void skipOffset(Long offset, Iterator<ScoredValue<V>> ite) {
      long localOffset = 0;
      while (localOffset++ < offset && ite.hasNext()) {
         ite.next();
      }
   }

   private static boolean isLimited(Long offset, Long count) {
      return offset != null && count != null;
   }

   public Collection<ScoredValue<V>> toTreeSet() {
      return new TreeSet<>(scoredEntries);
   }

   public long size() {
      return scoredEntries.size();
   }

   @ProtoTypeId(MULTIMAP_INDEX_VALUE)
   public static class IndexValue {
      @ProtoField(number = 1, defaultValue = "0")
      final double score;

      @ProtoField(number = 2, defaultValue = "0")
      final long index;

      @ProtoFactory
      IndexValue(double score, long index) {
         this.score = score;
         this.index = index;
      }

      public static IndexValue of(double score, long index) {
         return new IndexValue(score, index);
      }

      public long getValue() {
         return index;
      }

      public double getScore() {
         return score;
      }
   }

   @Override
   public Stream<MultimapObjectWrapper<V>> stream() {
      return scoredEntries.stream().map(ScoredValue::wrappedValue);
   }

   @Override
   public List<ScoredValue<V>> sort(SortOptions sortOptions) {
      // Skip sort takes precedence over all other options, which means the sorted set is returned as is.
      if (sortOptions.skipSort)
         return getScoredEntriesAsList();

      Stream<ScoredValue<V>> scoredValueStream;
      if (sortOptions.alpha) {
         scoredValueStream = scoredEntries.stream()
               .map(v -> new ScoredValue<>(1d, v.wrappedValue()));
      } else {
         scoredValueStream = scoredEntries.stream()
               .map(v -> new ScoredValue<>(v.wrappedValue().asDouble(), v.wrappedValue()));
      }
      return sort(scoredValueStream, sortOptions);
   }


   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SortedSetBucket<?> that = (SortedSetBucket<?>) o;
      return Objects.equals(scoredEntries, that.scoredEntries)
            && Objects.equals(entries, that.entries);
   }

   @Override
   public int hashCode() {
      return Objects.hash(scoredEntries, entries);
   }

   public record SortedSetResult<R, E>(R result, SortedSetBucket<E> bucket) { }
}
