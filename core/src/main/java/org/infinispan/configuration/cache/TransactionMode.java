package org.infinispan.configuration.cache;

/**
 * Transaction mode
 *
 * @author Galder Zamarreño
 * @version 7.0
 */
public enum TransactionMode {
   NONE(false, false, false, false),
   BATCH(true, false, false, true),
   NON_XA(true, false, false, false),
   NON_DURABLE_XA(true, true, false, false),
   FULL_XA(true, true, true, false),
   ;
   private final boolean enabled;
   private final boolean xaEnabled;
   private final boolean recoveryEnabled;
   private final boolean batchingEnabled;

   TransactionMode(boolean enabled, boolean xaEnabled, boolean recoveryEnabled, boolean batchingEnabled) {
      this.enabled = enabled;
      this.xaEnabled = xaEnabled;
      this.recoveryEnabled = recoveryEnabled;
      this.batchingEnabled = batchingEnabled;
   }

   public static TransactionMode fromConfiguration(TransactionConfiguration transactionConfiguration) {
      return transactionConfiguration.mode();
   }

   /**
    * @deprecated Use {@link #isTransactional()}
    */
   @Deprecated(forRemoval = true, since = "16.3")
   public org.infinispan.transaction.TransactionMode getMode() {
      return this.enabled ? org.infinispan.transaction.TransactionMode.TRANSACTIONAL : org.infinispan.transaction.TransactionMode.NON_TRANSACTIONAL;
   }

   public boolean isTransactional() {
      return this.enabled;
   }

   public boolean isXAEnabled() {
      return this.xaEnabled;
   }

   public boolean isRecoveryEnabled() {
      return this.recoveryEnabled;
   }

   public boolean isBatchingEnabled() {
      return batchingEnabled;
   }
}
