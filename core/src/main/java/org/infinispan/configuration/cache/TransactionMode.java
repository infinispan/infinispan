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
      switch (this) {
         case NON_XA:
         case NON_DURABLE_XA:
         case FULL_XA:
            return org.infinispan.transaction.TransactionMode.TRANSACTIONAL;
         default:
            return org.infinispan.transaction.TransactionMode.NON_TRANSACTIONAL;
      }
   }

   public boolean isXAEnabled() {
      switch (this) {
         case NON_DURABLE_XA:
         case FULL_XA:
            return true;
         default:
            return false;
      }
   }

   public boolean isRecoveryEnabled() {
      switch (this) {
         case FULL_XA:
            return true;
         default:
            return false;
      }
   }

}
