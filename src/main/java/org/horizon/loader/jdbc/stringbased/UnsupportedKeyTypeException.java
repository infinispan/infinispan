package org.horizon.loader.jdbc.stringbased;

import org.horizon.loader.CacheLoaderException;

/**
 * Exception thrown by {@link org.horizon.loader.jdbc.stringbased.JdbcStringBasedCacheStore} when one tries to persist
 * a StoredEntry with an unsupported key type.
 *
 * @see org.horizon.loader.jdbc.stringbased.JdbcStringBasedCacheStore
 * @author Mircea.Markus@jboss.com
 */
public class UnsupportedKeyTypeException extends CacheLoaderException {

   public UnsupportedKeyTypeException(Object key) {
      this("Unsupported key type: '" + key.getClass().getName() + "' on key: " + key );
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
