package org.infinispan.multimap.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.protostream.impl.MarshallableUserObject;
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
public class SetBucket<V> {

   final Set<V> values;

   public SetBucket() {
      this.values = new HashSet<>(null);
   }

   public SetBucket(V value) {
      var set = new HashSet<V>(1);
      set.add(value);
      this.values = set;
   }

   private SetBucket(Set<V> values) {
      this.values = values;
   }

   public static <V> SetBucket<V> create(V value) {
      return new SetBucket<>(value);
   }

   @ProtoFactory
   SetBucket(Collection<MarshallableUserObject<V>> wrappedValues) {
      this((Set<V>) wrappedValues.stream().map(MarshallableUserObject::get)
            .collect(Collectors.toCollection(HashSet::new)));
   }

   @ProtoField(number = 1, collectionImplementation = HashSet.class)
   Collection<MarshallableUserObject<V>> getWrappedValues() {
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

   @Override
   public String toString() {
      return "SetBucket{values=" + Util.toStr(values) + '}';
   }

   public boolean add(V value) {
      return values.add(value);
   }
}
