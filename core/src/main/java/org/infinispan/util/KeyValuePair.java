package org.infinispan.util;

import java.util.Map;
import java.util.Objects;

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
public class KeyValuePair<K,V> implements Map.Entry<K,V> {
   private final K key;
   private final V value;

   public static <K, V> KeyValuePair<K, V> of(K key, V value) {
      return new KeyValuePair<>(key, value);
   }

   public KeyValuePair(K key, V value) {
      this.key = key;
      this.value = value;
   }

   @ProtoFactory
   KeyValuePair(MarshallableObject<K> wrappedKey, MarshallableObject<V> wrappedValue) {
      this(MarshallableObject.unwrap(wrappedKey), MarshallableObject.unwrap(wrappedValue));
   }

   @ProtoField(number = 1, name = "key")
   MarshallableObject<K> getWrappedKey() {
      return MarshallableObject.create(key);
   }

   @ProtoField(number = 2, name = "value")
   MarshallableObject<V> getWrappedValue() {
      return MarshallableObject.create(value);
   }

   public K getKey() {
      return key;
   }

   public V getValue() {
      return value;
   }

   @Override
   public V setValue(V value) {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean equals(Object o) {
      if (o == null || getClass() != o.getClass()) return false;
      KeyValuePair<?, ?> that = (KeyValuePair<?, ?>) o;
      return Objects.equals(key, that.key) && Objects.equals(value, that.value);
   }

   @Override
   public int hashCode() {
      return Objects.hash(key, value);
   }

   @Override
   public String toString() {
      return "KeyValuePair{key=" + key + ", value=" + value + '}';
   }
}
