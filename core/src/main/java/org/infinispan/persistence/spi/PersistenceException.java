package org.infinispan.persistence.spi;

import org.infinispan.commons.CacheException;

/**
 * An exception thrown by a {@link org.infinispan.persistence.spi.NonBlockingStore} implementation if there are problems
 * reading from a loader.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class PersistenceException extends CacheException {

   private static final long serialVersionUID = -7640401612614646818L;

   public PersistenceException() {
   }

   public PersistenceException(String message) {
      super(message);
   }

   public PersistenceException(String message, Throwable cause) {
      super(message, cause);
   }

   public PersistenceException(Throwable cause) {
      super(cause);
   }
}
