package org.infinispan.multimap.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Bucket used to store MultiMap values, required as HashSet cannot be directly marshalled via ProtoStream.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
@ProtoTypeId(ProtoStreamTypeIds.MULTIMAP_LOWER_BOUND)
public class Bucket<V> {

   final Collection<V> values;

   public Bucket() {
      this.values = new HashSet<>();
   }

   @ProtoFactory
   Bucket(HashSet<WrappedMessage> wrappedMessages) {
      this.values = wrappedMessages == null ? new HashSet<>() : (Collection<V>) wrappedMessages.stream().map(WrappedMessage::getValue).collect(Collectors.toSet());
   }

   @ProtoField(number = 1, collectionImplementation = HashSet.class)
   Set<WrappedMessage> getWrappedMessages() {
      return this.values.stream().map(WrappedMessage::new).collect(Collectors.toSet());
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

   public HashSet<V> toSet() {
      return new HashSet<>(values);
   }
}
