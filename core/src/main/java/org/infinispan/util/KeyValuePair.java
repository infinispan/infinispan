package org.infinispan.util;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

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

   public KeyValuePair() {
      this(null, null); //required by azul vm
   }

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
      public void writeObject(ObjectOutput output, KeyValuePair kvPair) throws IOException {
         output.writeObject(kvPair.getKey());
         output.writeObject(kvPair.getValue());
      }

      @Override
      public KeyValuePair readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new KeyValuePair(input.readObject(), input.readObject());
      }

      @Override
      public Integer getId() {
         return Ids.KEY_VALUE_PAIR_ID;
      }

      @Override
      @SuppressWarnings("unchecked")
      public Set<Class<? extends KeyValuePair>> getTypeClasses() {
         return Util.<Class<? extends KeyValuePair>>asSet(KeyValuePair.class);
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
}
