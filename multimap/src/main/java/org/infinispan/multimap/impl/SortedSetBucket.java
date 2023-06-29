package org.infinispan.multimap.impl;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.multimap.impl.internal.MultimapObjectWrapper;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

   private final Comparator<ScoredValue<V>> scoreComparator = Comparator.naturalOrder();

   @ProtoFactory
   SortedSetBucket(Collection<ScoredValue<V>> wrappedValues) {
      scoredEntries = new TreeSet<>(scoreComparator);
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
      this.scoredEntries = new TreeSet<>(scoreComparator);
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

   @SuppressWarnings({ "unchecked", "rawtypes" })
   public SortedSet<ScoredValue<V>> subsetByScores(double min, boolean includeMin, double max, boolean includeMax) {
      ScoredValue<V> minSv;
      ScoredValue<V> maxSv;
      boolean unboundedMin = false;
      boolean unboundedMax = false;
      if (min == max && (!includeMin || !includeMax)) {
         return Collections.emptySortedSet();
      }

      if (min == Double.MIN_VALUE) {
         minSv = scoredEntries.first();
         unboundedMin = true;
      } else {
         if (includeMin) {
            minSv = scoredEntries.lower(new ScoredValue(min, NO_VALUE));
         } else {
            minSv = scoredEntries.higher(new ScoredValue(min, NO_VALUE));
         }

         if (minSv == null) {
            minSv = scoredEntries.first();
         }
      }

      if (max == Double.MAX_VALUE) {
         maxSv = scoredEntries.last();
         unboundedMax = true;
      } else {
         if (includeMax) {
            maxSv = scoredEntries.higher(new ScoredValue(max, NO_VALUE));
         } else {
            maxSv = scoredEntries.lower(new ScoredValue(max, NO_VALUE));
         }

         if (maxSv == null) {
            maxSv = scoredEntries.last();
         }
      }

      if (minSv.score() > maxSv.score()) {
         return Collections.emptySortedSet();
      }

      return scoredEntries
            .subSet(minSv,
                  unboundedMin || (minSv.score() > min || (includeMin && minSv.score() == min)),
                  maxSv,
                  unboundedMax || (maxSv.score() < max || (includeMax && maxSv.score() == max)));
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
