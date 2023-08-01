package org.infinispan.multimap.impl;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.multimap.impl.internal.MultimapObjectWrapper;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

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

/**
 * Bucket used to store Sorted Set data type.
 *
 * @author Katia Aresti
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_SORTED_SET_BUCKET)
public class SortedSetBucket<V> {
   private final static Object NO_VALUE = new Object();
   private final TreeSet<ScoredValue<V>> scoredEntries;
   private final Map<MultimapObjectWrapper<V>, Double> entries;

   /**
    *
    */
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
            return first < second ? first : second;
         }
      }, MAX {
         @Override
         public double apply(double first, double second) {
            return first > second ? first : second;
         }
      };

      private static final SortedSetBucket.AggregateFunction[] CACHED_VALUES = values();
      public static SortedSetBucket.AggregateFunction valueOf(int ordinal) {
         return CACHED_VALUES[ordinal];
      }

      public abstract double apply(double first, double second);
   }

   public Collection<ScoredValue<V>> union(Collection<ScoredValue<V>> inputValues,
                                     double weight,
                                     AggregateFunction function) {
      SortedSet<ScoredValue<V>> sortedMergeScoredValues = new TreeSet<>();
      Map<MultimapObjectWrapper<V>, Double> mergedEntries = new HashMap<>();
      Iterator<ScoredValue<V>> ite;

      if (inputValues != null) {
         ite = inputValues.iterator();
         while (ite.hasNext()) {
            ScoredValue<V> element = ite.next();
            Double existingScore = entries.get(element.wrappedValue());
            Double unionScore;
            if (existingScore == null) {
               unionScore = element.score;
            } else {
               unionScore = function.apply(element.score, existingScore * weight);
            }
            sortedMergeScoredValues.add(new ScoredValue<>(unionScore, element.wrappedValue()));
            mergedEntries.put(element.wrappedValue(), unionScore);
         }
      }

      ite = scoredEntries.iterator();
      while (ite.hasNext()) {
         ScoredValue<V> element = ite.next();
         Double existingScore = mergedEntries.get(element.wrappedValue());
         if (existingScore == null) {
            sortedMergeScoredValues.add(new ScoredValue<>(element.score * weight, element.wrappedValue()));
         }
      }
      return sortedMergeScoredValues;
   }

   public Collection<ScoredValue<V>> inter(Collection<ScoredValue<V>> inputValues,
                                          double weight,
                                          AggregateFunction function) {
      if (inputValues == null) {
         return scoredEntries.stream()
               .map(s -> new ScoredValue<>(s.score * weight, s.wrappedValue()))
               .collect(Collectors.toList());
      }

      SortedSet<ScoredValue<V>> sortedMergeScoredValues = new TreeSet<>();
      Iterator<ScoredValue<V>> ite = inputValues.iterator();
      while (ite.hasNext()) {
         ScoredValue<V> element = ite.next();
         Double existingScore = entries.get(element.wrappedValue());
         if (existingScore != null) {
            double score = function.apply(element.score, existingScore * weight);
            sortedMergeScoredValues.add(new ScoredValue<>(score, element.wrappedValue()));
         }
      }
      return sortedMergeScoredValues;
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

   public Collection<ScoredValue<V>> pop(boolean min, long count) {
      List<ScoredValue<V>> popValuesList = new ArrayList<>();
      for (long i = 0; i < count && !scoredEntries.isEmpty(); i++) {
         ScoredValue<V> popedScoredValue = min ? scoredEntries.pollFirst() : scoredEntries.pollLast();
         entries.remove(popedScoredValue.wrappedValue());
         popValuesList.add(popedScoredValue);
      }
      return popValuesList;
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

   public void replace(Collection<ScoredValue<V>> scoredValues) {
      entries.clear();
      scoredEntries.clear();
      for (ScoredValue<V> scoredValue : scoredValues) {
         addScoredValue(scoredValue);
      }
   }

   public Set<MultimapObjectWrapper<V>> getScoredEntriesAsValuesSet() {
      return entries.keySet().stream().collect(Collectors.toSet());
   }

   public static class AddOrUpdatesCounters {

      public long created = 0;

      public long updated = 0;
   }
   public AddOrUpdatesCounters addMany(Collection<ScoredValue<V>> scoredValues,
                                       boolean addOnly,
                                       boolean updateOnly,
                                       boolean updateLessScoresOnly,
                                       boolean updateGreaterScoresOnly) {

      AddOrUpdatesCounters addResult = new AddOrUpdatesCounters();
      int startSize = entries.size();

      for (ScoredValue<V> scoredValue : scoredValues) {
         if (addOnly) {
            addOnly(scoredValue);
         } else if (updateOnly) {
            updateOnly(addResult, scoredValue);
         } else if (updateGreaterScoresOnly) {
            addOrUpdateGreaterScores(addResult, scoredValue);
         } else if (updateLessScoresOnly) {
            addOrUpdateLessScores(addResult, scoredValue);
         } else {
            addOrUpdate(addResult, scoredValue);
         }
      }
      addResult.created = entries.size() - startSize;
      return addResult;
   }

   public Double incrScore(double incr, V member, boolean addOnly, boolean updateOnly, boolean updateLessScoresOnly, boolean updateGreaterScoresOnly) {
      MultimapObjectWrapper<V> wrappedValue = new MultimapObjectWrapper<>(member);
      Double existingScore = entries.get(wrappedValue);
      if ((existingScore != null && addOnly) || (existingScore == null && updateOnly)) {
         // do nothing
         return null;
      }

      Double newScore = existingScore == null ? incr : existingScore + incr;
      if (existingScore != null && ((updateGreaterScoresOnly && newScore <= existingScore) || (updateLessScoresOnly && newScore >= existingScore))) {
         // do nothing
         return null;
      }
      addOrUpdate(new AddOrUpdatesCounters(), new ScoredValue<>(newScore, wrappedValue));
      return newScore;
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

   private void addOrUpdateGreaterScores(AddOrUpdatesCounters addResult, ScoredValue<V> scoredValue) {
      Double existingScore = entries.get(scoredValue.wrappedValue());
      if (existingScore == null) {
         addScoredValue(scoredValue);
      } else if (scoredValue.score() > existingScore) {
         updateScoredValue(scoredValue, existingScore);
         addResult.updated++;
      }
   }

   private void addOrUpdateLessScores(AddOrUpdatesCounters addResult, ScoredValue<V> scoredValue) {
      Double existingScore = entries.get(scoredValue.wrappedValue());
      if (existingScore == null) {
         addScoredValue(scoredValue);
      } else if (scoredValue.score() < existingScore) {
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

   public <V> long removeAll(Collection<V> values) {
      long removeCount = 0;
      for (V value: values) {
         MultimapObjectWrapper<V> wrappedValue = new MultimapObjectWrapper(value);
         Double score = entries.get(wrappedValue);
         if (score != null) {
            entries.remove(wrappedValue);
            scoredEntries.remove(new ScoredValue<>(score, wrappedValue));
            removeCount++;
         }
      }
      return removeCount;
   }

   public long removeAll(V min, boolean includeMin, V max, boolean includeMax) {
      List<ScoredValue<V>> subset = subset(min, includeMin, max, includeMax, false, null, null);
      for (ScoredValue<V> value : subset) {
         entries.remove(value.wrappedValue());
         scoredEntries.remove(value);
      }
      return subset.size();
   }

   public long removeAll(Double min, boolean includeMin, Double max, boolean includeMax) {
      List<ScoredValue<V>> subset = subset(min, includeMin, max, includeMax, false, null, null);
      for (ScoredValue<V> value : subset) {
         entries.remove(value.wrappedValue());
         scoredEntries.remove(value);
      }
      return subset.size();
   }

   public long removeAll(Long min, Long max) {
      List<ScoredValue<V>> subset = subsetByIndex(min, max, false);
      for (ScoredValue<V> value : subset) {
         entries.remove(value.wrappedValue());
         scoredEntries.remove(value);
      }
      return subset.size();
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
            startSv = scoredEntries.lower(new ScoredValue(min, NO_VALUE));
         } else {
            startSv = scoredEntries.higher(new ScoredValue(min, NO_VALUE));
         }

         if (startSv == null) {
            startSv = scoredEntries.first();
         }
      }

      if (unboundedMax) {
         stopSv = scoredEntries.last();
      } else {
         if (includeMax) {
            stopSv = scoredEntries.higher(new ScoredValue(max, NO_VALUE));
         } else {
            stopSv = scoredEntries.lower(new ScoredValue(max, NO_VALUE));
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
      double score = scoredEntries.first().score;

      ScoredValue<V> minScoredValue = new ScoredValue<>(score, minValue);
      ScoredValue<V> maxScoredValue = new ScoredValue<>(score, maxValue);

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

   public static class IndexValue {
      private final double score;
      private final long index;

      private IndexValue(double score, long index) {
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

   @ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_SORTED_SET_SCORED_ENTRY)
   public static class ScoredValue<V> implements Comparable<ScoredValue<V>> {
      private final Double score;
      private final MultimapObjectWrapper<V> value;

      private ScoredValue(Double score, V value) {
         this.score = score;
         this.value = new MultimapObjectWrapper<>(value);
      }

      public static <V> ScoredValue<V> of(double score, V value) {
         return new ScoredValue<>(score, value);
      }

      @ProtoFactory
      public ScoredValue(Double score, MultimapObjectWrapper<V> wrappedValue) {
         this.score = score;
         this.value = wrappedValue;
      }

      @ProtoField(1)
      public Double score() {
         return score;
      }

      @ProtoField(2)
      public MultimapObjectWrapper<V> wrappedValue() {
         return value;
      }

      public V getValue() {
         return value.get();
      }

      @Override
      public int hashCode() {
         return Objects.hash(value, score);
      }

      @Override
      public boolean equals(Object entry) {
         if (this == entry) return true;
         if (entry == null || getClass() != entry.getClass()) return false;
         @SuppressWarnings("unchecked")
         ScoredValue<V> other = (ScoredValue<V>) entry;

         return this.value.equals(other.value) && this.score.equals(other.score);
      }

      @Override
      public String toString() {
         return "ScoredValue{" + "score=" + score + ", value=" + value.toString() + '}';
      }

      @Override
      public int compareTo(ScoredValue<V> other) {
         if (this == other) return 0;
         int compare = Double.compare(this.score, other.score);
         if (compare == 0) {
            if (other.getValue() == NO_VALUE || this.getValue() == NO_VALUE) {
               return 0;
            }
            return value.compareTo(other.wrappedValue());
         }
         return compare;
      }
   }
}
