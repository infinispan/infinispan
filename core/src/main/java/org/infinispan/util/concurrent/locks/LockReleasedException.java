package org.infinispan.util.concurrent.locks;

import org.infinispan.commons.CacheException;

/**
 * The exception is thrown if a locks is released while waiting for it to be acquired.
 *
 * @author Pedro Ruivo
 * @since 13.0
 */
public class LockReleasedException extends CacheException {

   public LockReleasedException() {
   }

   public LockReleasedException(Throwable cause) {
      super(cause);
   }

   public LockReleasedException(String msg) {
      super(msg);
   }

   public LockReleasedException(String msg, Throwable cause) {
      super(msg, cause);
   }

   public LockReleasedException(String message, Throwable cause, boolean enableSuppression,
         boolean writableStackTrace) {
      super(message, cause, enableSuppression, writableStackTrace);
   }
}
