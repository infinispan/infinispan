package org.infinispan.util;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.marshall.core.Ids;

/**
 *
 * Holds logically related key-value pairs or binary tuples.
 *
 * @author Mircea Markus
 * @since 6.0
 */
public class KeyValuePair<K,V> {
   private final K key;
   private final V value;

   public KeyValuePair(K key, V value) {
      this.key = key;
      this.value = value;
   }

   public K getKey() {
      return key;
   }

   public V getValue() {
      return value;
   }


   public static class Externalizer extends AbstractExternalizer<KeyValuePair> {

      private static final long serialVersionUID = -5291318076267612501L;

      @Override
      public void writeObject(UserObjectOutput output, KeyValuePair kvPair) throws IOException {
         output.writeUserObjects(kvPair.getKey(), kvPair.getValue());
      }

      @Override
      public KeyValuePair readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
         return new KeyValuePair(input.readUserObject(), input.readUserObject());
      }

      @Override
      public Integer getId() {
         return Ids.KEY_VALUE_PAIR_ID;
      }

      @Override
      public Set<Class<? extends KeyValuePair>> getTypeClasses() {
         return Collections.singleton(KeyValuePair.class);
      }
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
