package org.infinispan.loaders.modifications;

/**
 * Represents a {@link org.infinispan.loaders.CacheStore#clear()} modification
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class Clear implements Modification {
   @Override
   public Type getType() {
      return Type.CLEAR;
   }

   @Override
   public String toString() {
      return "Clear";
   }
}
