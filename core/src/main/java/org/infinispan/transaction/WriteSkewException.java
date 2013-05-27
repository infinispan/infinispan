package org.infinispan.transaction;

import org.infinispan.commons.CacheException;

/**
 * Thrown when a write skew is detected
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class WriteSkewException extends CacheException {

   private final Object key;

   public WriteSkewException() {
      this.key = null;
   }

   public WriteSkewException(Throwable cause, Object key) {
      super(cause);
      this.key = key;
   }

   public WriteSkewException(String msg, Object key) {
      super(msg);
      this.key = key;
   }

   public WriteSkewException(String msg, Throwable cause, Object key) {
      super(msg, cause);
      this.key = key;
   }

   public final Object getKey() {
      return key;
   }
}
