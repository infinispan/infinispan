package org.horizon.manager;

/**
 * Thrown when a named cache cannot be found.
 *
 * @author (various)
 * @since 1.0
 */
public class NamedCacheNotFoundException extends Exception {
   public NamedCacheNotFoundException() {
   }

   public NamedCacheNotFoundException(String message) {
      super(message);
   }

   public NamedCacheNotFoundException(String message, Throwable cause) {
      super(message, cause);
   }

   public NamedCacheNotFoundException(Throwable cause) {
      super(cause);
   }
}
