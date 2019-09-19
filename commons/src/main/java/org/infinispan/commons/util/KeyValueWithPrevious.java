package org.infinispan.commons.util;

import java.io.Serializable;

import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;

public class KeyValueWithPrevious<K, V> implements Serializable {
   /**
    * The serialVersionUID
    */
   private static final long serialVersionUID = -7875910676423622104L;

   private final K key;
   private final V value;
   private final V prev;

   public KeyValueWithPrevious(K key, V value, V prev) {
      this.key = key;
      this.value = value;
      this.prev = prev;
   }

   @ProtoFactory
   KeyValueWithPrevious(WrappedMessage wrappedKey, WrappedMessage wrappedValue, WrappedMessage wrappedPrev) {
      this.key = (K) wrappedKey.getValue();
      this.value = (V) wrappedValue.getValue();
      this.prev = (V) wrappedPrev.getValue();
   }

   public K getKey() {
      return key;
   }

   public V getValue() {
      return value;
   }

   public V getPrev() {
      return prev;
   }

   @ProtoField(number = 1)
   WrappedMessage getWrappedKey() {
      return new WrappedMessage(key);
   }

   @ProtoField(number = 2)
   WrappedMessage getWrappedValue() {
      return new WrappedMessage(key);
   }

   @ProtoField(number = 3)
   WrappedMessage getWrappedPrev() {
      return new WrappedMessage(key);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      KeyValueWithPrevious keyValueWithPrevious = (KeyValueWithPrevious) o;

      if (key != null ? !key.equals(keyValueWithPrevious.key) : keyValueWithPrevious.key != null) return false;
      if (prev != null ? !prev.equals(keyValueWithPrevious.prev) : keyValueWithPrevious.prev != null) return false;
      if (value != null ? !value.equals(keyValueWithPrevious.value) : keyValueWithPrevious.value != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = key != null ? key.hashCode() : 0;
      result = 31 * result + (value != null ? value.hashCode() : 0);
      result = 31 * result + (prev != null ? prev.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "KeyValueWithPrevious{" +
            "key=" + key +
            ", value=" + value +
            ", prev=" + prev +
            '}';
   }
}
