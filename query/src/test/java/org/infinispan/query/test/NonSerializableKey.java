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

   public NonSerializableKey(final String key) {
      this.key = key;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      NonSerializableKey that = (NonSerializableKey) o;

      if (key != null ? !key.equals(that.key) : that.key != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return key != null ? key.hashCode() : 0;
   }
}
