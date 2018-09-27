package org.infinispan.query.test;

import org.infinispan.query.Transformable;

/**
 * Non serializable key for testing DefaultTransformer.
 *
 * @author Anna Manukyan
 */
@Transformable
public class NonSerializableKey {

   private String key;

   public NonSerializableKey(String key) {
      this.key = key;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      NonSerializableKey other = (NonSerializableKey) o;
      return key != null ? key.equals(other.key) : other.key == null;
   }

   @Override
   public int hashCode() {
      return key != null ? key.hashCode() : 0;
   }
}
