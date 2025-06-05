package org.infinispan.persistence.modifications;

/**
 * An interface that defines a {@link org.infinispan.persistence.spi.NonBlockingStore} modification
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface Modification {
   enum Type {
      STORE, REMOVE, CLEAR, LIST
   }

   Type getType();
}
