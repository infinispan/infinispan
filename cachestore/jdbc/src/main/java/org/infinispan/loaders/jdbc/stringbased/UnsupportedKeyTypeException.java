package org.infinispan.loaders.jdbc.stringbased;

import org.infinispan.loaders.CacheLoaderException;

/**
 * Exception thrown by {@link org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore} when one tries to
 * persist a StoredEntry with an unsupported key type.
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.loaders.jdbc.stringbased.JdbcStringBasedCacheStore
 */
public class UnsupportedKeyTypeException extends CacheLoaderException {

   public UnsupportedKeyTypeException(Object key) {
      this("Unsupported key type: '" + key.getClass().getName() + "' on key: " + key);
   }

   public UnsupportedKeyTypeException(String message) {
      super(message);
   }

   public UnsupportedKeyTypeException(String message, Throwable cause) {
      super(message, cause);
   }

   public UnsupportedKeyTypeException(Throwable cause) {
      super(cause);
   }
}
