package org.horizon.statetransfer;

/**
 * An exception to denote problems in transferring state between cache instances in a cluster
 *
 * @author Manik Surtani
 * @since 1.0
 */
public class StateTransferException extends Exception {
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
