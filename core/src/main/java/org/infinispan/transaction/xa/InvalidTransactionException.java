package org.infinispan.transaction.xa;

import org.infinispan.commons.CacheException;

/**
 * Thrown if an operation is to be performed on an invalid transaction context.
 *
 * @author Manik Surtani
 * @since 4.2
 */
public class InvalidTransactionException extends CacheException {
   public InvalidTransactionException() {
   }

   public InvalidTransactionException(Throwable cause) {
      super(cause);
   }

   public InvalidTransactionException(String msg) {
      super(msg);
   }

   public InvalidTransactionException(String msg, Throwable cause) {
      super(msg, cause);
   }
}
