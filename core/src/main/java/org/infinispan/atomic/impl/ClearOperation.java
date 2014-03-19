package org.infinispan.atomic.impl;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.FastCopyHashMap;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Set;

/**
 * An atomic clear operation.
 * <p/>
 *
 * @author (various)
 * @param <K>
 * @param <V>
 * @since 4.0
 */
public class ClearOperation<K, V> extends Operation<K, V> {
   FastCopyHashMap<K, V> originalEntries;

   ClearOperation() {
   }

   ClearOperation(FastCopyHashMap<K, V> originalEntries) {
      this.originalEntries = originalEntries;
   }

   @Override
   public void rollback(Map<K, V> delegate) {
      if (!originalEntries.isEmpty()) delegate.putAll(originalEntries);
   }

   @Override
   public void replay(Map<K, V> delegate) {
      delegate.clear();
   }

   @Override
   public K keyAffected() {
      //null means all keys are affected
      return null;
   }

   @Override
   public String toString() {
      return "ClearOperation";
   }

   public static class Externalizer extends AbstractExternalizer<ClearOperation> {
      @Override
      public void writeObject(ObjectOutput output, ClearOperation object) throws IOException {
         // no-op
      }

      @Override
      public ClearOperation readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         return new ClearOperation();
      }

      @Override
      public Integer getId() {
         return Ids.ATOMIC_CLEAR_OPERATION;
      }

      @Override
      public Set<Class<? extends ClearOperation>> getTypeClasses() {
         return Util.<Class<? extends ClearOperation>>asSet(ClearOperation.class);
      }
   }
}