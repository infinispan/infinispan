package org.infinispan.persistence.modifications;

/**
 * An interface that defines a {@link org.infinispan.loaders.spi.CacheStore} modification
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface Modification {
   static enum Type {
      STORE, REMOVE, CLEAR, LIST
   }

   Type getType();
}
