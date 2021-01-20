package org.infinispan.multimap.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableUserObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Bucket used to store MultiMap values, required as HashSet cannot be directly marshalled via ProtoStream.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_BUCKET)
public class Bucket<V> {

   final Collection<V> values;

   public Bucket() {
      this.values = Collections.emptyList();
   }

   public Bucket(V value) {
      this.values = Collections.singletonList(value);
   }

   private Bucket(List<V> values) {
      this.values = values;
   }

   @ProtoFactory
   Bucket(HashSet<MarshallableUserObject<V>> wrappedValues) {
      this(wrappedValues.stream().map(MarshallableUserObject::get).collect(Collectors.toList()));
   }

   @ProtoField(number = 1, collectionImplementation = HashSet.class)
   Set<MarshallableUserObject<V>> getWrappedValues() {
      return this.values.stream().map(MarshallableUserObject::new).collect(Collectors.toSet());
   }

   public boolean contains(V value) {
      for (V v : values) {
         if (Objects.deepEquals(v, value)) {
            return Boolean.TRUE;
         }
      }
      return Boolean.FALSE;
   }

   /**
    * @return {@code null} if the element exists in this {@link Bucket}, otherwise, it returns a new {@link Bucket}
    * instance.
    */
   public Bucket<V> add(V value) {
      List<V> newBucket = new ArrayList<>(values.size() + 1);
      for (V v : values) {
         if (Objects.deepEquals(v, value)) {
            return null;
         }
         newBucket.add(v);
      }
      newBucket.add(value);
      return new Bucket<>(newBucket);
   }

   public Bucket<V> remove(V value) {
      List<V> newBucket = new ArrayList<>(values.size());
      boolean removed = false;
      Iterator<V> it = values.iterator();
      while (it.hasNext()) {
         V v = it.next();
         if (Objects.deepEquals(v, value)) {
            removed = true;
            break;
         }
         newBucket.add(v);
      }
      //add remaining values
      while (it.hasNext()) {
         newBucket.add(it.next());
      }
      return removed ? new Bucket<>(newBucket) : null;
   }

   public Bucket<V> removeIf(Predicate<? super V> p) {
      List<V> newBucket = new ArrayList<>(values.size());
      for (V v : values) {
         if (!p.test(v)) {
            newBucket.add(v);
         }
      }
      return newBucket.size() == values.size() ? null : new Bucket<>(newBucket);
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
      return new HashSet<>(values);
   }
}
