package org.infinispan.transaction;

/**
 * Enumerate with the possible commits protocols.
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public enum TransactionProtocol {
   /**
    * uses the 2PC protocol
    */
   TWO_PHASE_COMMIT,
   /**
    * uses the total order protocol
    */
   TOTAL_ORDER;

   public boolean isTotalOrder() {
      return this == TOTAL_ORDER;
   }
}
