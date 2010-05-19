package org.infinispan;

/**
 * An exception thrown when a command refers to a named cache that does not exist.
 *
 * @author Manik Surtani
 * @version 4.1
 */
public class NamedCacheNotDefinedException extends CacheException {
   public NamedCacheNotDefinedException(String cacheName) {
      super("Cache name: " + cacheName);
   }

   public NamedCacheNotDefinedException(String cacheName, Throwable cause) {
      super("Cache name: " + cacheName, cause);
   }

   public NamedCacheNotDefinedException(String cacheName, String msg) {
      super(msg + " Cache name: " + cacheName);
   }

   public NamedCacheNotDefinedException(String cacheName, String msg, Throwable cause) {
      super(msg + " Cache name: " + cacheName, cause);
   }
}
