package org.infinispan.util;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 *
 * Holds logically related key-value pairs or binary tuples.
 *
 * @author Mircea Markus
 * @since 6.0
 */
@ProtoTypeId(ProtoStreamTypeIds.KEY_VALUE_PAIR)
public class KeyValuePair<K,V> {

   public static <K, V> KeyValuePair<K, V> of(K key, V value) {
      return new KeyValuePair<>(key, value);
   }

   @ProtoField(number = 1)
   final MarshallableObject<K> key;

   @ProtoField(number = 2)
   final MarshallableObject<V> value;

   @ProtoFactory
   KeyValuePair(MarshallableObject<K> key, MarshallableObject<V> value) {
      this.key = key;
      this.value = value;
   }

   public KeyValuePair(K key, V value) {
      this(MarshallableObject.create(key), MarshallableObject.create(value));
   }

   public K getKey() {
      return MarshallableObject.unwrap(key);
   }

   public V getValue() {
      return MarshallableObject.unwrap(value);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof KeyValuePair)) return false;

      KeyValuePair that = (KeyValuePair) o;

      if (key != null ? !key.equals(that.key) : that.key != null) return false;
      if (value != null ? !value.equals(that.value) : that.value != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = key != null ? key.hashCode() : 0;
      result = 31 * result + (value != null ? value.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "KeyValuePair{key=" + key + ", value=" + value + '}';
   }
}
