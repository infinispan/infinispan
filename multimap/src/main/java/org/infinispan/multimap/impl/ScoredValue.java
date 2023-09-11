package org.infinispan.multimap.impl;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.multimap.impl.internal.MultimapObjectWrapper;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

import java.util.Objects;

@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_SCORED_VALUE)
public class ScoredValue<V> implements Comparable<ScoredValue<V>> {
   private static final Object NO_VALUE = new Object();
   private final Double score;
   private final MultimapObjectWrapper<V> value;

   public static <V> ScoredValue<V> of(double score, V value) {
      return new ScoredValue<>(score, new MultimapObjectWrapper<>(value));
   }

   public static <V> ScoredValue<V> of(double score) {
      return new ScoredValue<>(score, new MultimapObjectWrapper(NO_VALUE));
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
