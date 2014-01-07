package org.infinispan.configuration.cache;

/**
 * Transaction mode
 *
 * @author Galder Zamarreño
 * @version 7.0
 */
public enum TransactionMode {

   NONE, NON_XA, NON_DURABLE_XA, FULL_XA;

   public org.infinispan.transaction.TransactionMode getMode() {
      return this == NONE
            ? org.infinispan.transaction.TransactionMode.NON_TRANSACTIONAL
            : org.infinispan.transaction.TransactionMode.TRANSACTIONAL;
   }

   public boolean isXAEnabled() {
      return this == FULL_XA || this == NON_DURABLE_XA;
   }

   public boolean isRecoveryEnabled() {
      return this == FULL_XA;
   }

}
