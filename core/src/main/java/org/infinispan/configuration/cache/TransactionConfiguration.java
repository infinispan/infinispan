package org.infinispan.configuration.cache;

import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.infinispan.transaction.lookup.TransactionSynchronizationRegistryLookup;

public class TransactionConfiguration {

   private final boolean autoCommit;
   private final int cacheStopTimeout;
   private final boolean eagerLockingSingleNode;
   private final LockingMode lockingMode;
   private final boolean syncCommitPhase;
   private final boolean syncRollbackPhase;
   private final TransactionManagerLookup transactionManagerLookup;
   private final TransactionSynchronizationRegistryLookup transactionSynchronizationRegistryLookup;
   private final TransactionMode transactionMode;
   private final boolean useEagerLocking;
   private final boolean useSynchronization;
   private final RecoveryConfiguration recovery;
   
   TransactionConfiguration(boolean autoCommit, int cacheStopTimeout, boolean eagerLockingSingleNode, LockingMode lockingMode,
         boolean syncCommitPhase, boolean syncRollbackPhase, TransactionManagerLookup transactionManagerLookup, TransactionSynchronizationRegistryLookup transactionSynchronizationRegistryLookup,
         TransactionMode transactionMode, boolean useEagerLocking, boolean useSynchronization, RecoveryConfiguration recovery) {
      this.autoCommit = autoCommit;
      this.cacheStopTimeout = cacheStopTimeout;
      this.eagerLockingSingleNode = eagerLockingSingleNode;
      this.lockingMode = lockingMode;
      this.syncCommitPhase = syncCommitPhase;
      this.syncRollbackPhase = syncRollbackPhase;
      this.transactionManagerLookup = transactionManagerLookup;
      this.transactionSynchronizationRegistryLookup = transactionSynchronizationRegistryLookup;
      this.transactionMode = transactionMode;
      this.useEagerLocking = useEagerLocking;
      this.useSynchronization = useSynchronization;
      this.recovery = recovery;
   }

   public boolean isAutoCommit() {
      return autoCommit;
   }

   public int getCacheStopTimeout() {
      return cacheStopTimeout;
   }

   public boolean isEagerLockingSingleNode() {
      return eagerLockingSingleNode;
   }

   public LockingMode getLockingMode() {
      return lockingMode;
   }

   public boolean isSyncCommitPhase() {
      return syncCommitPhase;
   }

   public boolean isSyncRollbackPhase() {
      return syncRollbackPhase;
   }

   public TransactionManagerLookup getTransactionManagerLookup() {
      return transactionManagerLookup;
   }
   
   public TransactionSynchronizationRegistryLookup getTransactionSynchronizationRegistryLookup() {
      return transactionSynchronizationRegistryLookup;
   }

   public TransactionMode getTransactionMode() {
      return transactionMode;
   }

   public boolean isUseEagerLocking() {
      return useEagerLocking;
   }

   public boolean isUseSynchronization() {
      return useSynchronization;
   }
   
   public RecoveryConfiguration getRecovery() {
      return recovery;
   }
   
   /**
    * Returns true if the cache is configured to run in transactional mode, false otherwise. Starting with Infinispan
    * version 5.1 a cache doesn't support mixed access: i.e.won't support transactional and non-transactional
    * operations.
    * A cache is transactional if one the following:
    * <pre>
    * - a transactionManagerLookup is configured for the cache
    * - batching is enabled
    * - it is explicitly marked as transactional: config.fluent().transaction().transactionMode(TransactionMode.TRANSACTIONAL).
    *   In this last case a transactionManagerLookup needs to be explicitly set
    * </pre>
    * By default a cache is not transactional.
    *
    * @see #isTransactionAutoCommit()
    */
   public  boolean isTransactionalCache() {
      return transactionMode.equals(TransactionMode.TRANSACTIONAL);
   }
   
}
