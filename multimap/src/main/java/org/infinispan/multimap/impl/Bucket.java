package org.infinispan.multimap.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
      this.values = ConcurrentHashMap.newKeySet();
   }

   @ProtoFactory
   Bucket(HashSet<MarshallableUserObject<V>> wrappedValues) {
      this.values = wrappedValues.stream().map(MarshallableUserObject::get).collect(Collectors.toSet());
   }

   @ProtoField(number = 1, collectionImplementation = HashSet.class)
   Set<MarshallableUserObject<V>> getWrappedValues() {
      return this.values.stream().map(MarshallableUserObject::new).collect(Collectors.toSet());
   }

   public boolean contains(V value) {
      return this.values.contains(value);
   }

   public boolean add(V value) {
      return this.values.add(value);
   }

   public boolean addAll(Bucket<V> bucket) {
      return this.values.addAll(bucket.values);
   }

   public boolean remove(V value) {
      return this.values.remove(value);
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
