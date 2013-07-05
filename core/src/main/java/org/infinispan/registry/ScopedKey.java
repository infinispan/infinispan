package org.infinispan.registry;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
* Used to support scoping for the cluster registry's entries.
*
* @author Mircea Markus
* @since 6.0
*/
public final class ScopedKey<S,K> {
   public final S scope;
   public final K key;

   public ScopedKey(S scope, K key) {
      if (scope == null) throw new IllegalArgumentException("Null scope not allowed");
      if (key == null) throw new IllegalArgumentException("Null key not allowed");
      this.scope = scope;
      this.key = key;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof ScopedKey)) return false;

      ScopedKey scopedKey = (ScopedKey) o;

      if (!key.equals(scopedKey.key)) return false;
      if (!scope.equals(scopedKey.scope)) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = scope.hashCode();
      result = 31 * result + key.hashCode();
      return result;
   }

   public K getKey() {
      return key;
   }

   public S getScope() {
      return scope;
   }

   public boolean hasScope(S scope) {
      return this.scope.equals(scope);
   }

   public static class Externalizer extends AbstractExternalizer<ScopedKey> {
      @Override
      public Set<Class<? extends ScopedKey>> getTypeClasses() {
         return Util.<Class<? extends ScopedKey>>asSet(ScopedKey.class);
      }

      @Override
      public void writeObject(ObjectOutput output, ScopedKey object) throws IOException {
         output.writeObject(object.getScope());
         output.writeObject(object.getKey());
      }

      @Override
      public ScopedKey readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object scope = input.readObject();
         Object key = input.readObject();
         return new ScopedKey(scope, key);
      }

      @Override
      public Integer getId() {
         return Ids.SCOPED_KEY;
      }
   }
}
