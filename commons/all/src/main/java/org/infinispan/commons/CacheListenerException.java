package org.infinispan.commons;

/**
 * Wraps exceptions produced by listener implementations.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
public class CacheListenerException extends CacheException {

   public CacheListenerException() {
   }

   public CacheListenerException(Throwable cause) {
      super(cause);
   }

   public CacheListenerException(String msg) {
      super(msg);
   }

   public CacheListenerException(String msg, Throwable cause) {
      super(msg, cause);
   }

}
