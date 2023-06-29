package org.infinispan.multimap.impl;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.multimap.impl.internal.MultimapObjectWrapper;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Bucket used to store Sorted Set data type.
 *
 * @author Katia Aresti
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_SORTED_SET_BUCKET)
public class SortedSetBucket<V> {
   private final SortedSet<ScoredValue<V>> scoredEntries;
   private final Map<MultimapObjectWrapper<V>, Double> entries;

   private final Comparator<ScoredValue<V>> scoreComparator = Comparator.naturalOrder();

   @ProtoFactory
   SortedSetBucket(Collection<ScoredValue<V>> wrappedValues) {
      scoredEntries = new TreeSet<>(scoreComparator);
      scoredEntries.addAll(wrappedValues);
      entries = new HashMap<>();
      wrappedValues.stream().forEach(e -> entries.put(e.wrappedValue(), e.score()));
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
      this.scoredEntries = new ConcurrentSkipListSet<>(scoreComparator);
      this.entries = new HashMap<>();
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
         ScoredValue<V> newScoredValue = new ScoredValue<>(scores[i], values[i]);
         if (addOnly) {
            Double score = entries.get(newScoredValue.wrappedValue());
            if (score == null){
               scoredEntries.add(newScoredValue);
               entries.put(newScoredValue.wrappedValue(), newScoredValue.score());
            }
         } else if (updateOnly) {
            Double actualScore = entries.get(newScoredValue.wrappedValue());
            if (actualScore != null && actualScore != newScoredValue.score()) {
               ScoredValue<V> oldScoredValue = ScoredValue.of(actualScore, newScoredValue.getValue());
               scoredEntries.remove(oldScoredValue);
               scoredEntries.add(newScoredValue);
               entries.put(newScoredValue.wrappedValue(), newScoredValue.score());
               addResult.updated++;
            }
         } else if (updateGreaterScoresOnly) {
            // Adds or updates if the new score is greater than the current score
            Double actualScore = entries.get(newScoredValue.wrappedValue());
            if (actualScore == null) {
               scoredEntries.add(newScoredValue);
               entries.put(newScoredValue.wrappedValue(), newScoredValue.score());
            } else if (newScoredValue.score() > actualScore) {
               // Update
               ScoredValue<V> oldScoredValue = ScoredValue.of(actualScore, newScoredValue.getValue());
               scoredEntries.remove(oldScoredValue);
               scoredEntries.add(newScoredValue);
               entries.put(newScoredValue.wrappedValue(), newScoredValue.score());
               addResult.updated++;
            }
         } else if (updateLessScoresOnly) {
            // Adds or updates if the new score is less than the current score
            Double actualScore = entries.get(newScoredValue.wrappedValue());
            if (actualScore == null) {
               scoredEntries.add(newScoredValue);
               entries.put(newScoredValue.wrappedValue(), newScoredValue.score());
            } else if (newScoredValue.score() < actualScore) {
               ScoredValue<V> oldScoredValue = ScoredValue.of(actualScore, newScoredValue.getValue());
               scoredEntries.remove(oldScoredValue);
               scoredEntries.add(newScoredValue);
               entries.put(newScoredValue.wrappedValue(), newScoredValue.score());
               addResult.updated++;
            }
         } else {
            Double actualScore = entries.get(newScoredValue.wrappedValue());
            if (actualScore == null) {
               // add new entry
               scoredEntries.add(newScoredValue);
               entries.put(newScoredValue.wrappedValue(), newScoredValue.score());
            } else if (actualScore != newScoredValue.score()) {
               // entry exists, check score
               ScoredValue<V> oldScoredValue = ScoredValue.of(actualScore, newScoredValue.getValue());
               scoredEntries.remove(oldScoredValue);
               scoredEntries.add(newScoredValue);
               entries.put(newScoredValue.wrappedValue(), newScoredValue.score());
               addResult.updated++;
            }
         }
      }
      addResult.created = entries.size() - startSize;
      return addResult;
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
         this.value = new MultimapObjectWrapper<V>(value);
      }

      public static <T> ScoredValue<T> of(double score, T value) {
         return new ScoredValue<T>(score, value);
      }

      @ProtoFactory
      ScoredValue(double score, MultimapObjectWrapper<V> wrappedValue) {
         this(score, wrappedValue.get());
      }

      @ProtoField(value = 1, required = true)
      public double score() {
         return score;
      }

      @ProtoField(value = 2)
      public MultimapObjectWrapper<V> wrappedValue() {
         return value;
      }

      public V getValue() {
         return value.get();
      }

      @Override
      public int hashCode() {
         // Only include the marshalled value
         return this.value.hashCode();
      }

      @Override
      public boolean equals(Object entry) {
         // Only include the marshalled value
         if (this == entry) return true;
         if (entry == null || getClass() != entry.getClass()) return false;

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
            return value.compareTo(other.wrappedValue());
         }
         return compare;
      }
   }
}
