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
 * An atomic remove operation.
 * <p/>
 *
 * @author (various)
 * @param <K>
 * @param <V>
 * @since 4.0
 */
public class RemoveOperation<K, V> extends Operation<K, V> {
   private K key;
   private V oldValue;

   public RemoveOperation() {
   }

   RemoveOperation(K key, V oldValue) {
      this.key = key;
      this.oldValue = oldValue;
   }

   @Override
   public void rollback(Map<K, V> delegate) {
      if (oldValue != null) delegate.put(key, oldValue);
   }

   @Override
   public void replay(Map<K, V> delegate) {
      delegate.remove(key);
   }

   @Override
   public K keyAffected() {
      return key;
   }

   @Override
   public String toString() {
      return "RemoveOperation{" +
            "key=" + key +
            ", oldValue=" + oldValue +
            '}';
   }

   public static class Externalizer extends AbstractExternalizer<RemoveOperation> {
      @Override
      public void writeObject(ObjectOutput output, RemoveOperation remove) throws IOException {
         output.writeObject(remove.key);
      }

      @Override
      public RemoveOperation readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         RemoveOperation<Object, Object> remove = new RemoveOperation<Object, Object>();
         remove.key = input.readObject();
         return remove;
      }

      @Override
      public Integer getId() {
         return Ids.ATOMIC_REMOVE_OPERATION;
      }

      @Override
      public Set<Class<? extends RemoveOperation>> getTypeClasses() {
         return Util.<Class<? extends RemoveOperation>>asSet(RemoveOperation.class);
      }
   }
}