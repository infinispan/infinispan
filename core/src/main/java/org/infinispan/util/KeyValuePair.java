package org.infinispan.util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.marshall.core.Ids;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.protostream.MessageMarshaller;

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

   public static class Marshaller implements MessageMarshaller<KeyValuePair> {

      private final PersistenceMarshaller marshaller;

      public Marshaller(PersistenceMarshaller marshaller) {
         this.marshaller = marshaller;
      }

      @Override
      public KeyValuePair readFrom(ProtoStreamReader reader) throws IOException {
         byte[] keyBytes = reader.readBytes("key");
         byte[] valueBytes = reader.readBytes("value");
         try {
            Object key = keyBytes == null ? null : marshaller.objectFromByteBuffer(keyBytes);
            Object value = valueBytes == null ? null : marshaller.objectFromByteBuffer(valueBytes);
            return new KeyValuePair<>(key, value);
         } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
         }
      }

      @Override
      public void writeTo(ProtoStreamWriter writer, KeyValuePair keyValuePair) throws IOException {
         try {
            byte[] keyBytes = keyValuePair.key == null ? null : marshaller.objectToByteBuffer(keyValuePair.key);
            byte[] valueBytes = keyValuePair.value == null ? null : marshaller.objectToByteBuffer(keyValuePair.value);
            writer.writeBytes("key", keyBytes);
            writer.writeBytes("value", valueBytes);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }
      }

      @Override
      public Class<KeyValuePair> getJavaClass() {
         return KeyValuePair.class;
      }

      @Override
      public String getTypeName() {
         return "persistence.KeyValuePair";
      }
   }
}
