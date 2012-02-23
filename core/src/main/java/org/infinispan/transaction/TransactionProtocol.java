package org.infinispan.transaction;

/**
 * Enumerate with the possible commits protocols.
 *
 * Date: 1/13/12
 * Time: 4:05 PM
 *
 * @author pruivo
 */
public enum TransactionProtocol {
   /**
    * uses the 2PC protocol
    */
   NORMAL,
   /**
    * uses the total order protocol
    */
   TOTAL_ORDER;

   public boolean isTotalOrder() {
      return this == TOTAL_ORDER;
   }
}
