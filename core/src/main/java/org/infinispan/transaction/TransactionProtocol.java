package org.infinispan.transaction;

/**
 * Enumerate with the possible commits protocols.
 *
 * @author Pedro Ruivo
 * @since 5.3
 * @deprecated since 10.0. Total Order will be removed.
 */
@Deprecated
public enum TransactionProtocol {
   /**
    * uses the 2PC protocol
    */
   DEFAULT,
   /**
    * uses the total order protocol
    */
   TOTAL_ORDER;

   public boolean isTotalOrder() {
      return this == TOTAL_ORDER;
   }
}
