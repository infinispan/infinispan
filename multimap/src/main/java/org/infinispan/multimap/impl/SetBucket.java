package org.infinispan.multimap.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Util;
import org.infinispan.multimap.impl.internal.MultimapObjectWrapper;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Bucket used to store Set data type.
 *
 * @author Vittorio Rigamonti
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_SET_BUCKET)
public class SetBucket<V> implements SortableBucket<V>, BaseSetBucket<V> {
   final Set<MultimapObjectWrapper<V>> values;

   public SetBucket() {
      this.values = new HashSet<>();
   }

   public SetBucket(V value) {
      Set<MultimapObjectWrapper<V>> set = new HashSet<>(1);
      set.add(new MultimapObjectWrapper<>(value));
      this.values = set;
   }

   public SetBucket(Set<V> values) {
      this.values = values.stream().map(MultimapObjectWrapper::new).collect(Collectors.toSet());
   }

   public static <V> SetBucket<V> create(V value) {
      return new SetBucket<>(value);
   }

   @ProtoFactory
   SetBucket(Collection<MultimapObjectWrapper<V>> wrappedValues) {
      this.values = new HashSet<>(wrappedValues);
   }

   @ProtoField(number = 1, collectionImplementation = ArrayList.class)
   Collection<MultimapObjectWrapper<V>> getWrappedValues() {
      return new ArrayList<>(values);
   }

   public boolean contains(V value) {
      for (MultimapObjectWrapper<V> v : values) {
         if (Objects.deepEquals(v.get(), value)) {
            return Boolean.TRUE;
         }
      }
      return Boolean.FALSE;
   }

   @Override
   public Set<ScoredValue<V>> getAsSet() {
      return values.stream()
            .map(v -> new ScoredValue<>(1d, v))
            .collect(Collectors.toSet());
   }

   @Override
   public List<ScoredValue<V>> getAsList() {
      return values.stream()
            .map(v -> new ScoredValue<>(1d, v))
            .toList();
   }

   @Override
   public Double getScore(MultimapObjectWrapper<V> key) {
      return 1d;
   }

   public boolean isEmpty() {
      return values.isEmpty();
   }

   public int size() {
      return values.size();
   }

   /**
    * @return a defensive copy of the {@link #values} collection.
    */
   public Set<V> toSet() {
      return values.stream().map(MultimapObjectWrapper::get).collect(Collectors.toSet());
   }

   public List<V> toList() {
      return values.stream().map(mow -> mow.get()).collect(Collectors.toList());
   }


   @Override
   public String toString() {
      return "SetBucket{values=" + Util.toStr(values) + '}';
   }

   public boolean add(V value) {
      return values.add(new MultimapObjectWrapper<>(value));
   }

   public boolean remove(V value) {
      return values.remove(new MultimapObjectWrapper<>(value));
   }

   public boolean addAll(Collection<V> values) {
      return this.values.addAll(values.stream().map(MultimapObjectWrapper::new).collect(Collectors.toSet()));
   }

   public boolean removeAll(Collection<V> values) {
      return this.values.removeAll(values.stream().map(MultimapObjectWrapper::new).collect(Collectors.toSet()));
   }

   @Override
   public Stream<MultimapObjectWrapper<V>> stream() {
      return values.stream();
   }

   @Override
   public List<ScoredValue<V>> sort(SortOptions sortOptions) {
      Stream<ScoredValue<V>> scoredValueStream;
      if (sortOptions.alpha) {
         scoredValueStream = values.stream()
               .map(v -> new ScoredValue<>(1d, v));
      } else {
         scoredValueStream = values.stream()
               .map(v -> new ScoredValue<>(v.asDouble(), v));
      }
      return sort(scoredValueStream, sortOptions);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SetBucket<?> setBucket = (SetBucket<?>) o;
      return Objects.equals(values, setBucket.values);
   }

   @Override
   public int hashCode() {
      return Objects.hash(values);
   }
}
