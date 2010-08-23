package org.infinispan.statetransfer;

/**
 * An exception to denote problems in transferring state between cache instances in a cluster
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class StateTransferException extends Exception {

   private static final long serialVersionUID = -7679740750970789100L;

   public StateTransferException() {
   }

   public StateTransferException(String message) {
      super(message);
   }

   public StateTransferException(String message, Throwable cause) {
      super(message, cause);
   }

   public StateTransferException(Throwable cause) {
      super(cause);
   }
}
