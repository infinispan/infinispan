package org.infinispan.transaction;

import org.infinispan.commons.CacheException;

/**
 * Thrown when a write skew is detected
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class WriteSkewException extends CacheException {
   public WriteSkewException() {
   }

   public WriteSkewException(Throwable cause) {
      super(cause);
   }

   public WriteSkewException(String msg) {
      super(msg);
   }

   public WriteSkewException(String msg, Throwable cause) {
      super(msg, cause);
   }
}
