package org.infinispan.atomic.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

/**
 * An atomic put operation.
 * <p/>
 *
 * @author (various)
 * @param <K>
 * @param <V>
 * @since 4.0
 */
public class PutOperation<K, V> extends Operation<K, V> {
   private K key;
   private V oldValue;
   private V newValue;

   public PutOperation() {
   }

   PutOperation(K key, V oldValue, V newValue) {
      this.key = key;
      this.oldValue = oldValue;
      this.newValue = newValue;
   }

   @Override
   public void rollback(Map<K, V> delegate) {
      if (oldValue == null)
         delegate.remove(key);
      else
         delegate.put(key, oldValue);
   }

   @Override
   public void replay(Map<K, V> delegate) {
      delegate.put(key, newValue);
   }

   @Override
   public K keyAffected() {
      return key;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof PutOperation)) return false;

      PutOperation that = (PutOperation) o;

      if (key != null ? !key.equals(that.key) : that.key != null) return false;
      if (newValue != null ? !newValue.equals(that.newValue) : that.newValue != null) return false;
      if (oldValue != null ? !oldValue.equals(that.oldValue) : that.oldValue != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = key != null ? key.hashCode() : 0;
      result = 31 * result + (oldValue != null ? oldValue.hashCode() : 0);
      result = 31 * result + (newValue != null ? newValue.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "PutOperation{" +
            "key=" + key +
            ", oldValue=" + oldValue +
            ", newValue=" + newValue +
            '}';
   }

   public static class Externalizer extends AbstractExternalizer<PutOperation> {
      @Override
      public void writeObject(ObjectOutput output, PutOperation put) throws IOException {
         output.writeObject(put.key);
         output.writeObject(put.newValue);
      }

      @Override
      public PutOperation readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         PutOperation<Object, Object> put = new PutOperation<Object, Object>();
         put.key = input.readObject();
         put.newValue = input.readObject();
         return put;
      }

      @Override
      public Integer getId() {
         return Ids.ATOMIC_PUT_OPERATION;
      }

      @Override
      public Set<Class<? extends PutOperation>> getTypeClasses() {
         return Util.<Class<? extends PutOperation>>asSet(PutOperation.class);
      }
   }
}