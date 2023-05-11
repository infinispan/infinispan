package org.infinispan.multimap.impl;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.protostream.impl.MarshallableUserObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Bucket used to store ListMultimap values.
 *
 * @author Katia Aresti
 * @since 15.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_LIST_BUCKET)
public class ListBucket<V> {

   final Deque<V> values;

   public ListBucket() {
      this.values = new ArrayDeque<>(0);
   }

   public ListBucket(V value) {
      Deque<V> deque = new ArrayDeque<>(1);
      deque.add(value);
      this.values = deque;
   }

   private ListBucket(Deque<V> values) {
      this.values = values;
   }

   public static <V> ListBucket<V> create(V value) {
      return new ListBucket<>(value);
   }

   @ProtoFactory
   ListBucket(Collection<MarshallableUserObject<V>> wrappedValues) {
      this((Deque<V>) wrappedValues.stream().map(MarshallableUserObject::get)
            .collect(Collectors.toCollection(ArrayDeque::new)));
   }

   @ProtoField(number = 1, collectionImplementation = ArrayList.class)
   Collection<MarshallableUserObject<V>> getWrappedValues() {
      return this.values.stream().map(MarshallableUserObject::new).collect(Collectors.toCollection(ArrayDeque::new));
   }

   public boolean contains(V value) {
      for (V v : values) {
         if (Objects.deepEquals(v, value)) {
            return Boolean.TRUE;
         }
      }
      return Boolean.FALSE;
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
   public Deque<V> toDeque() {
      return new ArrayDeque<>(values);
   }


   @Override
   public String toString() {
      return "ListBucket{values=" + Util.toStr(values) + '}';
   }

   public ListBucket<V> offer(V value, boolean first) {
      Deque<V> newBucket = new ArrayDeque<>(values);
      if (first) {
         newBucket.offerFirst(value);
      } else {
         newBucket.offerLast(value);
      }

      return new ListBucket<>(newBucket);
   }
}
