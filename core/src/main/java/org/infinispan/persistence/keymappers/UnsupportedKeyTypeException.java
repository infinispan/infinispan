package org.infinispan.persistence.keymappers;

import org.infinispan.persistence.CacheLoaderException;

/**
 * Exception thrown by certain cache stores when one tries to persist an entry with an unsupported key type.
 *
 * @author Mircea.Markus@jboss.com
 */
public class UnsupportedKeyTypeException extends CacheLoaderException {

   /** The serialVersionUID */
   private static final long serialVersionUID = 1442739860198872706L;

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
