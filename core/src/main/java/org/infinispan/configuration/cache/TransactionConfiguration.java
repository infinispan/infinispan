package org.infinispan.configuration.cache;

import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.infinispan.transaction.lookup.TransactionSynchronizationRegistryLookup;

/**
 * Defines transactional (JTA) characteristics of the cache.
 * 
 * @author pmuir
 * @author Pedro Ruivo
 * 
 */
public class TransactionConfiguration {

   private final boolean autoCommit;
   private long cacheStopTimeout;
   private final boolean eagerLockingSingleNode;
   private LockingMode lockingMode;
   private boolean syncCommitPhase;
   private boolean syncRollbackPhase;
   private TransactionManagerLookup transactionManagerLookup;
   private final TransactionSynchronizationRegistryLookup transactionSynchronizationRegistryLookup;
   private final TransactionMode transactionMode;
   private boolean useEagerLocking;
   private final boolean useSynchronization;
   private final RecoveryConfiguration recovery;
   private final boolean use1PcForAutoCommitTransactions;
   private final long reaperWakeUpInterval;
   private final long completedTxTimeout;
   private final TransactionProtocol transactionProtocol; //2PC or Total order protocol


   TransactionConfiguration(boolean autoCommit, long cacheStopTimeout, boolean eagerLockingSingleNode, LockingMode lockingMode,
                            boolean syncCommitPhase, boolean syncRollbackPhase, TransactionManagerLookup transactionManagerLookup,
                            TransactionSynchronizationRegistryLookup transactionSynchronizationRegistryLookup, TransactionMode transactionMode,
                            boolean useEagerLocking, boolean useSynchronization, boolean use1PcForAutoCommitTransactions,
                            long reaperWakeUpInterval, long completedTxTimeout, RecoveryConfiguration recovery, TransactionProtocol transactionProtocol) {
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
      this.use1PcForAutoCommitTransactions = use1PcForAutoCommitTransactions;
      this.reaperWakeUpInterval = reaperWakeUpInterval;
      this.completedTxTimeout = completedTxTimeout;
      this.transactionProtocol = transactionProtocol;
   }

   /**
    * If the cache is transactional (i.e. {@link #transactionMode()} == TransactionMode.TRANSACTIONAL)
    * and transactionAutoCommit is enabled then for single operation transactions
    * the user doesn't need to manually start a transaction, but a transactions
    * is injected by the system. Defaults to true.
    */
   public boolean autoCommit() {
      return autoCommit;
   }

   /**
    * If there are any ongoing transactions when a cache is stopped, Infinispan waits for ongoing
    * remote and local transactions to finish. The amount of time to wait for is defined by the
    * cache stop timeout. It is recommended that this value does not exceed the transaction timeout
    * because even if a new transaction was started just before the cache was stopped, this could
    * only last as long as the transaction timeout allows it.
    */
   public TransactionConfiguration cacheStopTimeout(long l) {
      this.cacheStopTimeout = l;
      return this;
   }

   /**
    * If there are any ongoing transactions when a cache is stopped, Infinispan waits for ongoing
    * remote and local transactions to finish. The amount of time to wait for is defined by the
    * cache stop timeout. It is recommended that this value does not exceed the transaction timeout
    * because even if a new transaction was started just before the cache was stopped, this could
    * only last as long as the transaction timeout allows it.
    */
   public long cacheStopTimeout() {
      return cacheStopTimeout;
   }

   /**
    * Only has effect for DIST mode and when useEagerLocking is set to true. When this is enabled,
    * then only one node is locked in the cluster, disregarding numOwners config. On the opposite,
    * if this is false, then on all cache.lock() calls numOwners RPCs are being performed. The node
    * that gets locked is the main data owner, i.e. the node where data would reside if
    * numOwners==1. If the node where the lock resides crashes, then the transaction is marked for
    * rollback - data is in a consistent state, no fault tolerance.
    *
    * @deprecated starting with Infinispan 5.1 single node locking is used by default
    */
   @Deprecated
   public boolean eagerLockingSingleNode() {
      return eagerLockingSingleNode;
   }

   /**
    * Configures whether the cache uses optimistic or pessimistic locking.
    * If the cache is not transactional then the locking mode is ignored.
    * 
    * @see TransactionConfiguration#transactionMode()
    */
   public LockingMode lockingMode() {
      return lockingMode;
   }

   /**
    * Configures whether the cache uses optimistic or pessimistic locking.
    * If the cache is not transactional then the locking mode is ignored.
    *
    * @see TransactionConfiguration#transactionMode()
    */
    public TransactionConfiguration lockingMode(LockingMode lockingMode) {
      this.lockingMode = lockingMode;
      return this;
   }

   /**
    * If true, the cluster-wide commit phase in two-phase commit (2PC) transactions will be
    * synchronous, so Infinispan will wait for responses from all nodes to which the commit was
    * sent. Otherwise, the commit phase will be asynchronous. Keeping it as false improves
    * performance of 2PC transactions, since any remote failures are trapped during the prepare
    * phase anyway and appropriate rollbacks are issued.
    */
   public boolean syncCommitPhase() {
      return syncCommitPhase;
   }

   /**
    * If true, the cluster-wide commit phase in two-phase commit (2PC) transactions will be
    * synchronous, so Infinispan will wait for responses from all nodes to which the commit was
    * sent. Otherwise, the commit phase will be asynchronous. Keeping it as false improves
    * performance of 2PC transactions, since any remote failures are trapped during the prepare
    * phase anyway and appropriate rollbacks are issued.
    */
   public TransactionConfiguration syncCommitPhase(boolean b) {
      this.syncCommitPhase = b;
      return this;
   }

   /**
    * If true, the cluster-wide rollback phase in two-phase commit (2PC) transactions will be
    * synchronous, so Infinispan will wait for responses from all nodes to which the rollback was
    * sent. Otherwise, the rollback phase will be asynchronous. Keeping it as false improves
    * performance of 2PC transactions.
    * 
    * @return
    */
   public boolean syncRollbackPhase() {
      return syncRollbackPhase;
   }

   /**
    * If true, the cluster-wide rollback phase in two-phase commit (2PC) transactions will be
    * synchronous, so Infinispan will wait for responses from all nodes to which the rollback was
    * sent. Otherwise, the rollback phase will be asynchronous. Keeping it as false improves
    * performance of 2PC transactions.
    *
    * @param b
    * @return
    */
   public TransactionConfiguration syncRollbackPhase(boolean b) {
      this.syncRollbackPhase = b;
      return this;
   }

   /**
    * Configure Transaction manager lookup directly using an instance of TransactionManagerLookup.
    * Calling this method marks the cache as transactional.
    */
   public TransactionManagerLookup transactionManagerLookup() {
      return transactionManagerLookup;
   }

   public TransactionConfiguration transactionManagerLookup(TransactionManagerLookup transactionManagerLookup) {
      this.transactionManagerLookup = transactionManagerLookup;
      return this;
   }

   /**
    * Configure Transaction Synchronization Registry lookup directly using an instance of
    * TransactionManagerLookup. Calling this method marks the cache as transactional.
    */
   public TransactionSynchronizationRegistryLookup transactionSynchronizationRegistryLookup() {
      return transactionSynchronizationRegistryLookup;
   }

   public TransactionMode transactionMode() {
      return transactionMode;
   }

   /**
    * Only has effect for DIST mode and when useEagerLocking is set to true. When this is enabled,
    * then only one node is locked in the cluster, disregarding numOwners config. On the opposite,
    * if this is false, then on all cache.lock() calls numOwners RPCs are being performed. The node
    * that gets locked is the main data owner, i.e. the node where data would reside if
    * numOwners==1. If the node where the lock resides crashes, then the transaction is marked for
    * rollback - data is in a consistent state, no fault tolerance.
    * <p/>
    * Note: Starting with infinispan 5.1 eager locking is replaced with pessimistic locking and can
    * be enforced by setting transaction's locking mode to PESSIMISTIC.
    */
   @Deprecated
   public boolean useEagerLocking() {
      return useEagerLocking;
   }

   @Deprecated
   public TransactionConfiguration useEagerLocking(boolean b) {
      this.useEagerLocking = b;
      return this;
   }

   public boolean useSynchronization() {
      return useSynchronization;
   }

   /**
    * This method allows configuration of the transaction recovery cache. When this method is
    * called, it automatically enables recovery. So, if you want it to be disabled, make sure you
    * call {@link RecoveryConfigurationBuilder#enabled(boolean)} with false as parameter
    */
   public RecoveryConfiguration recovery() {
      return recovery;
   }

   /**
    * @see TransactionConfigurationBuilder#reaperWakeUpInterval(long)
    */
   public long reaperWakeUpInterval() {
      return reaperWakeUpInterval;
   }

   /**
    * @see TransactionConfigurationBuilder#completedTxTimeout(long)
    */
   public long completedTxTimeout()  {
      return completedTxTimeout;
   }

   /**
    * Before Infinispan 5.1 you could access the cache both transactionally and
    * non-transactionally. Naturally the non-transactional access is faster and
    * offers less consistency guarantees. From Infinispan 5.1 onwards, mixed
    * access is no longer supported, so if you wanna speed up transactional
    * caches and you're ready to trade some consistency guarantees, you can
    * enable use1PcForAutoCommitTransactions. <p/>
    *
    * What this configuration option does is force an induced transaction,
    * that has been started by Infinispan as a result of enabling autoCommit,
    * to commit in a single phase. So only 1 RPC instead of 2RPCs as in the
    * case of a full 2 Phase Commit (2PC).
    * <p/>
    * <b>N.B.</b> this option should NOT be used when modifying the
    * same key from multiple transactions as 1PC does not offer any consistency
    * guarantees under concurrent access.
    */
   public boolean use1PcForAutoCommitTransactions() {
      return use1PcForAutoCommitTransactions;
   }

   @Override
   public String toString() {
      return "TransactionConfiguration{" +
            "autoCommit=" + autoCommit +
            ", cacheStopTimeout=" + cacheStopTimeout +
            ", eagerLockingSingleNode=" + eagerLockingSingleNode +
            ", lockingMode=" + lockingMode +
            ", syncCommitPhase=" + syncCommitPhase +
            ", syncRollbackPhase=" + syncRollbackPhase +
            ", transactionManagerLookup=" + transactionManagerLookup +
            ", transactionSynchronizationRegistryLookup=" + transactionSynchronizationRegistryLookup +
            ", transactionMode=" + transactionMode +
            ", useEagerLocking=" + useEagerLocking +
            ", useSynchronization=" + useSynchronization +
            ", recovery=" + recovery +
            ", reaperWakeUpInterval=" + reaperWakeUpInterval +
            ", completedTxTimeout=" + completedTxTimeout +
            ", use1PcForAutoCommitTransactions=" + use1PcForAutoCommitTransactions +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TransactionConfiguration that = (TransactionConfiguration) o;

      if (autoCommit != that.autoCommit) return false;
      if (cacheStopTimeout != that.cacheStopTimeout) return false;
      if (eagerLockingSingleNode != that.eagerLockingSingleNode) return false;
      if (syncCommitPhase != that.syncCommitPhase) return false;
      if (syncRollbackPhase != that.syncRollbackPhase) return false;
      if (use1PcForAutoCommitTransactions != that.use1PcForAutoCommitTransactions)
         return false;
      if (useEagerLocking != that.useEagerLocking) return false;
      if (useSynchronization != that.useSynchronization) return false;
      if (lockingMode != that.lockingMode) return false;
      if (recovery != null ? !recovery.equals(that.recovery) : that.recovery != null)
         return false;
      if (transactionManagerLookup != null ? !transactionManagerLookup.equals(that.transactionManagerLookup) : that.transactionManagerLookup != null)
         return false;
      if (transactionMode != that.transactionMode) return false;
      if (transactionSynchronizationRegistryLookup != null ? !transactionSynchronizationRegistryLookup.equals(that.transactionSynchronizationRegistryLookup) : that.transactionSynchronizationRegistryLookup != null)
         return false;
      if (transactionProtocol != that.transactionProtocol) {
         return false;
      }

      return true;
   }

   @Override
   public int hashCode() {
      int result = (autoCommit ? 1 : 0);
      result = 31 * result + (int) (cacheStopTimeout ^ (cacheStopTimeout >>> 32));
      result = 31 * result + (eagerLockingSingleNode ? 1 : 0);
      result = 31 * result + (lockingMode != null ? lockingMode.hashCode() : 0);
      result = 31 * result + (syncCommitPhase ? 1 : 0);
      result = 31 * result + (syncRollbackPhase ? 1 : 0);
      result = 31 * result + (transactionManagerLookup != null ? transactionManagerLookup.hashCode() : 0);
      result = 31 * result + (transactionSynchronizationRegistryLookup != null ? transactionSynchronizationRegistryLookup.hashCode() : 0);
      result = 31 * result + (transactionMode != null ? transactionMode.hashCode() : 0);
      result = 31 * result + (useEagerLocking ? 1 : 0);
      result = 31 * result + (useSynchronization ? 1 : 0);
      result = 31 * result + (recovery != null ? recovery.hashCode() : 0);
      result = 31 * result + (use1PcForAutoCommitTransactions ? 1 : 0);
      result = 31 * result + (transactionProtocol != null ? transactionProtocol.hashCode() : 0);
      return result;
   }

   /**
    * @return the transaction protocol in use (2PC or Total Order)
    */
   public TransactionProtocol transactionProtocol() {
      return transactionProtocol;
   }
}
