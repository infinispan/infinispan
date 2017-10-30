package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.IdentityAttributeCopier;
import org.infinispan.commons.configuration.attributes.Matchable;
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
public class TransactionConfiguration implements Matchable<TransactionConfiguration> {
   public static final AttributeDefinition<Boolean> AUTO_COMMIT = AttributeDefinition.builder("auto-commit", true).immutable().build();
   public static final AttributeDefinition<Long> CACHE_STOP_TIMEOUT = AttributeDefinition.builder("stop-timeout", TimeUnit.SECONDS.toMillis(30)).build();
   public static final AttributeDefinition<LockingMode> LOCKING_MODE = AttributeDefinition.builder("locking", LockingMode.OPTIMISTIC).build();
   public static final AttributeDefinition<TransactionManagerLookup> TRANSACTION_MANAGER_LOOKUP = AttributeDefinition.<TransactionManagerLookup>builder("transaction-manager-lookup", GenericTransactionManagerLookup.INSTANCE).copier(IdentityAttributeCopier.INSTANCE).autoPersist(false).build();
   public static final AttributeDefinition<TransactionSynchronizationRegistryLookup> TRANSACTION_SYNCHRONIZATION_REGISTRY_LOOKUP = AttributeDefinition.builder("transaction-synchronization-registry-lookup", null, TransactionSynchronizationRegistryLookup.class).copier(IdentityAttributeCopier.INSTANCE).autoPersist(false).build();
   public static final AttributeDefinition<TransactionMode> TRANSACTION_MODE = AttributeDefinition.builder("mode", TransactionMode.NON_TRANSACTIONAL).immutable().autoPersist(false).build();
   public static final AttributeDefinition<Boolean> USE_SYNCHRONIZATION = AttributeDefinition.builder("synchronization", false).immutable().autoPersist(false).build();
   public static final AttributeDefinition<Boolean> USE_1_PC_FOR_AUTO_COMMIT_TRANSACTIONS = AttributeDefinition.builder("single-phase-auto-commit", false).build();
   public static final AttributeDefinition<Long> REAPER_WAKE_UP_INTERVAL = AttributeDefinition.builder("reaper-wake-up-interval", 30000L).immutable().xmlName("reaper-interval").build();
   public static final AttributeDefinition<Long> COMPLETED_TX_TIMEOUT = AttributeDefinition.builder("complete-timeout", 60000L).immutable().build();
   public static final AttributeDefinition<TransactionProtocol> TRANSACTION_PROTOCOL = AttributeDefinition.builder("transaction-protocol", TransactionProtocol.DEFAULT).immutable().xmlName("protocol").build();
   public static final AttributeDefinition<Boolean> NOTIFICATIONS = AttributeDefinition.builder("notifications", true).immutable().build();
   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(TransactionConfiguration.class, AUTO_COMMIT, CACHE_STOP_TIMEOUT, LOCKING_MODE,
            TRANSACTION_MANAGER_LOOKUP, TRANSACTION_SYNCHRONIZATION_REGISTRY_LOOKUP, TRANSACTION_MODE, USE_SYNCHRONIZATION, USE_1_PC_FOR_AUTO_COMMIT_TRANSACTIONS,
            REAPER_WAKE_UP_INTERVAL, COMPLETED_TX_TIMEOUT, TRANSACTION_PROTOCOL, NOTIFICATIONS);
   }

   private final Attribute<Boolean> autoCommit;
   private final Attribute<Long> cacheStopTimeout;
   private final Attribute<LockingMode> lockingMode;
   private final Attribute<TransactionManagerLookup> transactionManagerLookup;
   private final Attribute<TransactionSynchronizationRegistryLookup> transactionSynchronizationRegistryLookup;
   private final Attribute<TransactionMode> transactionMode;
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
      lockingMode = attributes.attribute(LOCKING_MODE);
      transactionManagerLookup = attributes.attribute(TRANSACTION_MANAGER_LOOKUP);
      transactionSynchronizationRegistryLookup = attributes.attribute(TRANSACTION_SYNCHRONIZATION_REGISTRY_LOOKUP);
      transactionMode = attributes.attribute(TRANSACTION_MODE);
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
    *
    * @deprecated since 9.0. no longer supported
    */
   @Deprecated
   public boolean syncCommitPhase() {
      return true;
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
    * @deprecated since 9.0. no longer supported
    */
   @Deprecated
   public boolean syncRollbackPhase() {
      return true;
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

   /**
    * @deprecated since 9.0. The transaction manager is only looked up once, during cache startup.
    */
   @Deprecated
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
