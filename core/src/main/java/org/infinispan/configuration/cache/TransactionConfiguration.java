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

   public boolean autoCommit() {
      return autoCommit;
   }

   public int cacheStopTimeout() {
      return cacheStopTimeout;
   }

   public boolean eagerLockingSingleNode() {
      return eagerLockingSingleNode;
   }

   public LockingMode lockingMode() {
      return lockingMode;
   }

   public boolean syncCommitPhase() {
      return syncCommitPhase;
   }

   public boolean syncRollbackPhase() {
      return syncRollbackPhase;
   }

   public TransactionManagerLookup transactionManagerLookup() {
      return transactionManagerLookup;
   }
   
   public TransactionSynchronizationRegistryLookup transactionSynchronizationRegistryLookup() {
      return transactionSynchronizationRegistryLookup;
   }

   public TransactionMode transactionMode() {
      return transactionMode;
   }

   public boolean useEagerLocking() {
      return useEagerLocking;
   }

   public boolean useSynchronization() {
      return useSynchronization;
   }
   
   public RecoveryConfiguration recovery() {
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
   public  boolean transactionalCache() {
      return transactionMode.equals(TransactionMode.TRANSACTIONAL);
   }
   
}
