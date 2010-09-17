package org.infinispan.manager;

/**
 * Thrown when a named cache cannot be found.
 *
 * @author (various)
 * @since 4.0
 */
public class NamedCacheNotFoundException extends Exception {

   private static final long serialVersionUID = 5937213470732655993L;

   public NamedCacheNotFoundException(String cacheName) {
      super("Cache: " + cacheName);
   }

   public NamedCacheNotFoundException(String cacheName, String message) {
      super(message + " Cache: " + cacheName);
   }

   public NamedCacheNotFoundException(String cacheName, String message, Throwable cause) {
      super(message + " Cache: " + cacheName, cause);
   }

   public NamedCacheNotFoundException(String cacheName, Throwable cause) {
      super("Cache: " + cacheName, cause);
   }
}
