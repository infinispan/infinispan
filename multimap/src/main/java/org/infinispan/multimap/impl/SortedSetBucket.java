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
import java.util.SortedSet;
import java.util.TreeSet;

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

   public Double score(V member) {
      return entries.get(new MultimapObjectWrapper<>(member));
   }

   public static class AddResult {

      public long created = 0;
      public long updated = 0;
   }
   public AddResult addMany(double[] scores,
                   V[] values,
                   boolean addOnly,
                   boolean updateOnly,
                   boolean updateLessScoresOnly,
                   boolean updateGreaterScoresOnly) {

      AddResult addResult = new AddResult();
      int startSize = entries.size();

      for (int i = 0 ; i < values.length; i++) {
         Double newScore = scores[i];
         MultimapObjectWrapper<V> value = new MultimapObjectWrapper<>(values[i]);
         if (addOnly) {
            Double existingScore = entries.get(value);
            if (existingScore == null){
               addScoredValue(newScore, value);
            }
         } else if (updateOnly) {
            Double existingScore = entries.get(value);
            if (existingScore != null && !existingScore.equals(newScore)) {
               updateScoredValue(addResult, newScore, value, existingScore);
            }
         } else if (updateGreaterScoresOnly) {
            // Adds or updates if the new score is greater than the current score
            Double existingScore = entries.get(value);
            if (existingScore == null) {
               addScoredValue(newScore, value);
            } else if (newScore > existingScore) {
               // Update
               updateScoredValue(addResult, newScore, value, existingScore);
            }
         } else if (updateLessScoresOnly) {
            // Adds or updates if the new score is less than the current score
            Double existingScore = entries.get(value);
            if (existingScore == null) {
               addScoredValue(newScore, value);
            } else if (newScore < existingScore) {
               updateScoredValue(addResult, newScore, value, existingScore);
            }
         } else {
            Double existingScore = entries.get(value);
            if (existingScore == null) {
               addScoredValue(newScore, value);
            } else if (!newScore.equals(existingScore)) {
               // entry exists, check score
               updateScoredValue(addResult, newScore, value, existingScore);
            }
         }
      }
      addResult.created = entries.size() - startSize;
      return addResult;
   }

   private void updateScoredValue(AddResult addResult, Double newScore, MultimapObjectWrapper<V> value, Double existingScore) {
      ScoredValue<V> oldScoredValue = new ScoredValue<>(existingScore, value);
      ScoredValue<V> newScoredValue = new ScoredValue<>(newScore, value);
      scoredEntries.remove(oldScoredValue);
      scoredEntries.add(newScoredValue);
      entries.put(value, newScore);
      addResult.updated++;
   }

   private void addScoredValue(Double newScore, MultimapObjectWrapper<V> value) {
      scoredEntries.add(new ScoredValue<>(newScore, value));
      entries.put(value, newScore);
   }

   public Collection<ScoredValue<V>> subsetByIndex(long from, long to, boolean rev) {
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
   public SortedSet<ScoredValue<V>> subset(Double startScore, boolean includeStart, Double stopScore, boolean includeStop, boolean isRev) {
      if (stopScore != null && stopScore.equals(startScore) && (!includeStart || !includeStop)) {
         return Collections.emptySortedSet();
      }

      Double min = isRev? stopScore : startScore;
      boolean includeMin = isRev? includeStop : includeStart;
      Double max = isRev? startScore : stopScore;
      boolean includeMax = isRev? includeStart : includeStop;
      boolean unboundedMin = min == null || min == Double.MIN_VALUE;
      boolean unboundedMax = max == null || max == Double.MAX_VALUE;

      if (unboundedMin && unboundedMax) {
         return isRev ? scoredEntries.descendingSet() : new TreeSet<>(scoredEntries);
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
         return Collections.emptySortedSet();
      }

      NavigableSet<ScoredValue<V>> subset = scoredEntries.subSet(
            startSv,
            unboundedMin || (startSv.score() > min || (includeMin && startSv.score() == min)),
            stopSv,
            unboundedMax || (stopSv.score() < max || (includeMax && stopSv.score() == max)));

      return isRev? subset.descendingSet(): subset;
   }

   public SortedSet<ScoredValue<V>> subset(V startValue, boolean includeStart, V stopValue, boolean includeStop, boolean isRev) {
      V minValue = isRev ? stopValue : startValue;
      V maxValue = isRev ? startValue : stopValue;
      boolean includeMin = isRev ? includeStop : includeStart;
      boolean includeMax = isRev ? includeStart : includeStop;

      if (maxValue != null && maxValue.equals(minValue) && (!includeMin || !includeMax)) {
         return Collections.emptySortedSet();
      }
      boolean unboundedMin = minValue == null;
      boolean unboundedMax = maxValue == null;

      if (unboundedMin && unboundedMax) {
         return isRev ? scoredEntries.descendingSet() : new TreeSet<>(scoredEntries);
      }
      // if all the scoredEntries have the same score, then we can pick up first score for lex
      // when all the entries don't have the same score, this method can't work. This is the expected behaviour.
      double score = scoredEntries.first().score;

      ScoredValue<V> minScoredValue = new ScoredValue<>(score, minValue);
      ScoredValue<V> maxScoredValue = new ScoredValue<>(score, maxValue);

      if (unboundedMin) {
         NavigableSet<ScoredValue<V>> entries = scoredEntries.headSet(maxScoredValue, includeMax);
         return isRev? entries.descendingSet() : entries;
      }

      if (unboundedMax) {
         NavigableSet<ScoredValue<V>> entries = scoredEntries.tailSet(minScoredValue, includeMin);
         return isRev? entries.descendingSet() : entries;
      }

      try {
         NavigableSet<ScoredValue<V>> entries = scoredEntries.subSet(minScoredValue, includeMin, maxScoredValue, includeMax);
         return isRev? entries.descendingSet() : entries;
      } catch (IllegalArgumentException e) {
         return Collections.emptySortedSet();
      }
   }

   public Collection<ScoredValue<V>> toTreeSet() {
      return new TreeSet<>(scoredEntries);
   }

   public long size() {
      return scoredEntries.size();
   }

   @ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_SORTED_SET_SCORED_ENTRY)
   public static class ScoredValue<V> implements Comparable<ScoredValue<V>> {
      private final double score;
      private final MultimapObjectWrapper<V> value;

      private ScoredValue(double score, V value) {
         this.score = score;
         this.value = new MultimapObjectWrapper<>(value);
      }

      public static <V> ScoredValue<V> of(double score, V value) {
         return new ScoredValue<>(score, value);
      }

      @ProtoFactory
      ScoredValue(double score, MultimapObjectWrapper<V> wrappedValue) {
         this.score = score;
         this.value = wrappedValue;
      }

      @ProtoField(value = 1, required = true)
      public double score() {
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

         return this.value.equals(other.value) && this.score == other.score;
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
