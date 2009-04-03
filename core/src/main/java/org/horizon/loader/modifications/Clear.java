package org.horizon.loader.modifications;

/**
 * Represents a {@link org.horizon.loader.CacheStore#clear()} modification
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class Clear implements Modification {
   public Type getType() {
      return Type.CLEAR;
   }

   @Override
   public String toString() {
      return "Clear";
   }
}
