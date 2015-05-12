package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.IdentityAttributeCopier;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.transaction.lookup.GenericTransactionManagerLookup;
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
   public static final AttributeDefinition<Boolean> AUTO_COMMIT = AttributeDefinition.builder("autoCommit", true).immutable().build();
   public static final AttributeDefinition<Long> CACHE_STOP_TIMEOUT = AttributeDefinition.builder("cacheStopTimeout", TimeUnit.SECONDS.toMillis(30)).build();
   public static final AttributeDefinition<Boolean> EAGER_LOCKING_SINGLE_NODE = AttributeDefinition.builder("eagerLockingSingleNode", false).immutable().build();
   public static final AttributeDefinition<LockingMode> LOCKING_MODE = AttributeDefinition.builder("lockingMode", LockingMode.OPTIMISTIC).build();
   public static final AttributeDefinition<Boolean> SYNC_COMMIT_PHASE = AttributeDefinition.builder("syncCommitPhase", true).immutable().build();
   public static final AttributeDefinition<Boolean> SYNC_ROLLBACK_PHASE = AttributeDefinition.builder("syncRollbackPhase", true).immutable().build();
   public static final AttributeDefinition<TransactionManagerLookup> TRANSACTION_MANAGER_LOOKUP = AttributeDefinition.<TransactionManagerLookup>builder("transactionManagerLookup", GenericTransactionManagerLookup.INSTANCE).copier(IdentityAttributeCopier.INSTANCE).build();
   public static final AttributeDefinition<TransactionSynchronizationRegistryLookup> TRANSACTION_SYNCHRONIZATION_REGISTRY_LOOKUP = AttributeDefinition.builder("transactionSynchronizationRegistryLookup", null, TransactionSynchronizationRegistryLookup.class).copier(IdentityAttributeCopier.INSTANCE).build();
   public static final AttributeDefinition<TransactionMode> TRANSACTION_MODE = AttributeDefinition.builder("transactionMode", TransactionMode.NON_TRANSACTIONAL).immutable().build();
   public static final AttributeDefinition<Boolean> USE_EAGER_LOCKING = AttributeDefinition.builder("useEagerLocking", false).build();
   public static final AttributeDefinition<Boolean> USE_SYNCHRONIZATION = AttributeDefinition.builder("useSynchronization", false).immutable().build();
   public static final AttributeDefinition<Boolean> USE_1_PC_FOR_AUTO_COMMIT_TRANSACTIONS = AttributeDefinition.builder("use1PcForAutoCommitTransactions", false).build();
   public static final AttributeDefinition<Long> REAPER_WAKE_UP_INTERVAL = AttributeDefinition.builder("reaperWakeUpInterval", 30000l).immutable().build();
   public static final AttributeDefinition<Long> COMPLETED_TX_TIMEOUT = AttributeDefinition.builder("completedTxTimeout", 60000l).immutable().build();
   public static final AttributeDefinition<TransactionProtocol> TRANSACTION_PROTOCOL = AttributeDefinition.builder("transactionProtocol", TransactionProtocol.DEFAULT).immutable().build();
   public static final AttributeDefinition<Boolean> NOTIFICATIONS = AttributeDefinition.builder("notifications", true).immutable().build();
   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(TransactionConfiguration.class, AUTO_COMMIT, CACHE_STOP_TIMEOUT, EAGER_LOCKING_SINGLE_NODE, LOCKING_MODE, SYNC_COMMIT_PHASE, SYNC_ROLLBACK_PHASE,
            TRANSACTION_MANAGER_LOOKUP, TRANSACTION_SYNCHRONIZATION_REGISTRY_LOOKUP, TRANSACTION_MODE, USE_EAGER_LOCKING, USE_SYNCHRONIZATION, USE_1_PC_FOR_AUTO_COMMIT_TRANSACTIONS,
            REAPER_WAKE_UP_INTERVAL, COMPLETED_TX_TIMEOUT, TRANSACTION_PROTOCOL, NOTIFICATIONS);
   }

   private final Attribute<Boolean> autoCommit;
   private final Attribute<Long> cacheStopTimeout;
   private final Attribute<Boolean> eagerLockingSingleNode;
   private final Attribute<LockingMode> lockingMode;
   private final Attribute<Boolean> syncCommitPhase;
   private final Attribute<Boolean> syncRollbackPhase;
   private final Attribute<TransactionManagerLookup> transactionManagerLookup;
   private final Attribute<TransactionSynchronizationRegistryLookup> transactionSynchronizationRegistryLookup;
   private final Attribute<TransactionMode> transactionMode;
   private final Attribute<Boolean> useEagerLocking;
   private final Attribute<Boolean> useSynchronization;
   private final Attribute<Boolean> use1PcForAutoCommitTransactions;
   private final Attribute<Long> reaperWakeUpInterval;
   private final Attribute<Long> completedTxTimeout;
   private final Attribute<TransactionProtocol> transactionProtocol;
   private final Attribute<Boolean> notifications;
   private final AttributeSet attributes;
   private final RecoveryConfiguration recovery;

   TransactionConfiguration(AttributeSet attributes, RecoveryConfiguration recovery) {
      this.attributes = attributes.checkProtection();
      autoCommit = attributes.attribute(AUTO_COMMIT);
      cacheStopTimeout = attributes.attribute(CACHE_STOP_TIMEOUT);
      eagerLockingSingleNode = attributes.attribute(EAGER_LOCKING_SINGLE_NODE);
      lockingMode = attributes.attribute(LOCKING_MODE);
      syncCommitPhase = attributes.attribute(SYNC_COMMIT_PHASE);
      syncRollbackPhase = attributes.attribute(SYNC_ROLLBACK_PHASE);
      transactionManagerLookup = attributes.attribute(TRANSACTION_MANAGER_LOOKUP);
      transactionSynchronizationRegistryLookup = attributes.attribute(TRANSACTION_SYNCHRONIZATION_REGISTRY_LOOKUP);
      transactionMode = attributes.attribute(TRANSACTION_MODE);
      useEagerLocking = attributes.attribute(USE_EAGER_LOCKING);
      useSynchronization = attributes.attribute(USE_SYNCHRONIZATION);
      use1PcForAutoCommitTransactions = attributes.attribute(USE_1_PC_FOR_AUTO_COMMIT_TRANSACTIONS);
      reaperWakeUpInterval = attributes.attribute(REAPER_WAKE_UP_INTERVAL);
      completedTxTimeout = attributes.attribute(COMPLETED_TX_TIMEOUT);
      transactionProtocol = attributes.attribute(TRANSACTION_PROTOCOL);
      notifications = attributes.attribute(NOTIFICATIONS);
      this.recovery = recovery;
   }

   /**
    * If the cache is transactional (i.e. {@link #transactionMode()} == TransactionMode.TRANSACTIONAL)
    * and transactionAutoCommit is enabled then for single operation transactions
    * the user doesn't need to manually start a transaction, but a transactions
    * is injected by the system. Defaults to true.
    */
   public boolean autoCommit() {
      return autoCommit.get();
   }

   /**
    * If there are any ongoing transactions when a cache is stopped, Infinispan waits for ongoing
    * remote and local transactions to finish. The amount of time to wait for is defined by the
    * cache stop timeout. It is recommended that this value does not exceed the transaction timeout
    * because even if a new transaction was started just before the cache was stopped, this could
    * only last as long as the transaction timeout allows it.
    */
   public TransactionConfiguration cacheStopTimeout(long l) {
      cacheStopTimeout.set(l);
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
      return cacheStopTimeout.get();
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
      return eagerLockingSingleNode.get();
   }

   /**
    * Configures whether the cache uses optimistic or pessimistic locking.
    * If the cache is not transactional then the locking mode is ignored.
    *
    * @see TransactionConfiguration#transactionMode()
    */
   public LockingMode lockingMode() {
      return lockingMode.get();
   }

   /**
    * Configures whether the cache uses optimistic or pessimistic locking.
    * If the cache is not transactional then the locking mode is ignored.
    *
    * @see TransactionConfiguration#transactionMode()
    */
    public TransactionConfiguration lockingMode(LockingMode lockingMode) {
      this.lockingMode.set(lockingMode);
      return this;
   }

   /**
    * If true, the cluster-wide commit phase in two-phase commit (2PC) transactions will be
    * synchronous, so Infinispan will wait for responses from all nodes to which the commit was
    * sent. Otherwise, the commit phase will be asynchronous. Keeping it as false improves
    * performance of 2PC transactions, but it can lead to inconsistencies when the primary owner releases
    * the lock before the backup commits the change.
    */
   public boolean syncCommitPhase() {
      return syncCommitPhase.get();
   }

   /**
    * If true, the cluster-wide commit phase in two-phase commit (2PC) transactions will be
    * synchronous, so Infinispan will wait for responses from all nodes to which the commit was
    * sent. Otherwise, the commit phase will be asynchronous. Keeping it as false improves
    * performance of 2PC transactions, but it can lead to inconsistencies when the primary owner releases
    * the lock before the backup commits the change.
    *
    * @deprecated The syncRollbackPhase setting can no longer be modified at runtime. It must be the same on all nodes.
    */
   @Deprecated
   public TransactionConfiguration syncCommitPhase(boolean b) {
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
      return syncRollbackPhase.get();
   }

   /**
    * If true, the cluster-wide rollback phase in two-phase commit (2PC) transactions will be
    * synchronous, so Infinispan will wait for responses from all nodes to which the rollback was
    * sent.
    *
    * @deprecated The syncRollbackPhase setting can no longer be modified at runtime. It must be the same on all nodes.
    */
   @Deprecated
   public TransactionConfiguration syncRollbackPhase(boolean b) {
      return this;
   }

   /**
    * Configure Transaction manager lookup directly using an instance of TransactionManagerLookup.
    * Calling this method marks the cache as transactional.
    */
   public TransactionManagerLookup transactionManagerLookup() {
      return transactionManagerLookup.get();
   }

   public TransactionConfiguration transactionManagerLookup(TransactionManagerLookup transactionManagerLookup) {
      this.transactionManagerLookup.set(transactionManagerLookup);
      return this;
   }

   /**
    * Configure Transaction Synchronization Registry lookup directly using an instance of
    * TransactionManagerLookup. Calling this method marks the cache as transactional.
    */
   public TransactionSynchronizationRegistryLookup transactionSynchronizationRegistryLookup() {
      return transactionSynchronizationRegistryLookup.get();
   }

   public TransactionMode transactionMode() {
      return transactionMode.get();
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
      return useEagerLocking.get();
   }

   @Deprecated
   public TransactionConfiguration useEagerLocking(boolean b) {
      useEagerLocking.set(b);
      return this;
   }

   public boolean useSynchronization() {
      return useSynchronization.get();
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
      return reaperWakeUpInterval.get();
   }

   /**
    * @see TransactionConfigurationBuilder#completedTxTimeout(long)
    */
   public long completedTxTimeout()  {
      return completedTxTimeout.get();
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
      return use1PcForAutoCommitTransactions.get();
   }

   /**
    * @return the transaction protocol in use (2PC or Total Order)
    */
   public TransactionProtocol transactionProtocol() {
      return transactionProtocol.get();
   }

   /**
    * @return are transactional notifications (
    *    {@link org.infinispan.notifications.cachelistener.annotation.TransactionRegistered} and
    *    {@link org.infinispan.notifications.cachelistener.annotation.TransactionCompleted}) triggered?
    */
   public boolean notifications() {
      return notifications.get();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "TransactionConfiguration [attributes=" + attributes + ", recovery=" + recovery + "]";
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      TransactionConfiguration other = (TransactionConfiguration) obj;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      if (recovery == null) {
         if (other.recovery != null)
            return false;
      } else if (!recovery.equals(other.recovery))
         return false;
      return true;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      result = prime * result + ((recovery == null) ? 0 : recovery.hashCode());
      return result;
   }

}
