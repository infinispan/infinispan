package org.horizon.loader.modifications;

/**
 * An interface that defines a {@link org.horizon.loader.CacheStore} modification
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface Modification {
   public static enum Type {
      STORE, REMOVE, CLEAR, PURGE_EXPIRED;
   }

   Type getType();
}
