package org.infinispan.loaders.modifications;

/**
 * An interface that defines a {@link org.infinispan.loaders.CacheStore} modification
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface Modification {
   static enum Type {
      STORE, REMOVE, CLEAR, PURGE_EXPIRED, LIST
   }

   Type getType();
}
