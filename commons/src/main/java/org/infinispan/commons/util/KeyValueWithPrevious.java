package org.infinispan.commons.util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.SerializeWith;

@SerializeWith(KeyValueWithPrevious.KeyValueWithPreviousExternalizer.class)
public class KeyValueWithPrevious<K, V> {
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

   public K getKey() {
      return key;
   }

   public V getValue() {
      return value;
   }

   public V getPrev() {
      return prev;
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

   @SuppressWarnings("unchecked")
   public static class KeyValueWithPreviousExternalizer implements Externalizer<KeyValueWithPrevious> {

      @Override
      public void writeObject(ObjectOutput output, KeyValueWithPrevious kvPair) throws IOException {
         output.writeObject(kvPair.getKey());
         output.writeObject(kvPair.getValue());
         output.writeObject(kvPair.getPrev());
      }

      @Override
      public KeyValueWithPrevious readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new KeyValueWithPrevious(input.readObject(), input.readObject(), input.readObject());
      }

   }

}
