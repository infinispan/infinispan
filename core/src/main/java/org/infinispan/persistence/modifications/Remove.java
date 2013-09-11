package org.infinispan.persistence.modifications;

/**
 * Represents a {@link org.infinispan.persistence.spi.CacheWriter#delete(Object)} (Object)} modification
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class Remove implements Modification {

   final Object key;

   public Remove(Object key) {
      this.key = key;
   }

   @Override
   public Type getType() {
      return Type.REMOVE;
   }

   public Object getKey() {
      return key;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Remove remove = (Remove) o;

      if (key != null ? !key.equals(remove.key) : remove.key != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return key != null ? key.hashCode() : 0;
   }

   @Override
   public String toString() {
      return "Remove{" +
            "key=" + key +
            '}';
   }
}
