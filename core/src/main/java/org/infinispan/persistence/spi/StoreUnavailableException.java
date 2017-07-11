package org.infinispan.persistence.spi;

/**
 * An exception thrown by the {@link org.infinispan.persistence.manager.PersistenceManager} when one or more
 * stores are unavailable when a cache operation is attempted.
 *
 * @author Ryan Emerson
 * @since 9.3
 */
public class StoreUnavailableException extends PersistenceException {

   public StoreUnavailableException() {
   }

   public StoreUnavailableException(String message) {
      super(message);
   }

   public StoreUnavailableException(String message, Throwable cause) {
      super(message, cause);
   }

   public StoreUnavailableException(Throwable cause) {
      super(cause);
   }
}
