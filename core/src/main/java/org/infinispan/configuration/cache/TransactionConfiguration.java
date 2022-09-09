package org.infinispan.configuration.cache;

import static org.infinispan.commons.configuration.attributes.IdentityAttributeCopier.identityCopier;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.tx.lookup.TransactionManagerLookup;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.GenericTransactionManagerLookup;
import org.infinispan.transaction.lookup.TransactionSynchronizationRegistryLookup;

/**
 * Defines transactional (JTA) characteristics of the cache.
 *
 * @author pmuir
 * @author Pedro Ruivo
 *
 */
public class TransactionConfiguration extends ConfigurationElement<TransactionConfiguration> {
   public static final AttributeDefinition<Boolean> AUTO_COMMIT = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.AUTO_COMMIT, true).immutable().build();
   public static final AttributeDefinition<Long> CACHE_STOP_TIMEOUT = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.STOP_TIMEOUT, TimeUnit.SECONDS.toMillis(30)).build();
   public static final AttributeDefinition<LockingMode> LOCKING_MODE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.LOCKING, LockingMode.OPTIMISTIC)
         .immutable().build();
   public static final AttributeDefinition<TransactionManagerLookup> TRANSACTION_MANAGER_LOOKUP = AttributeDefinition.<TransactionManagerLookup>builder(org.infinispan.configuration.parsing.Attribute.TRANSACTION_MANAGER_LOOKUP_CLASS, GenericTransactionManagerLookup.INSTANCE)
         .serializer(AttributeSerializer.INSTANCE_CLASS_NAME)
         .autoPersist(false).global(false).immutable().build();

   public static final AttributeDefinition<TransactionSynchronizationRegistryLookup> TRANSACTION_SYNCHRONIZATION_REGISTRY_LOOKUP = AttributeDefinition.builder("transaction-synchronization-registry-lookup", null, TransactionSynchronizationRegistryLookup.class)
         .copier(identityCopier()).autoPersist(false).immutable().build();
   public static final AttributeDefinition<TransactionMode> TRANSACTION_MODE = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.MODE, TransactionMode.NON_TRANSACTIONAL).immutable()
         .autoPersist(false).build();
   public static final AttributeDefinition<Boolean> USE_SYNCHRONIZATION = AttributeDefinition.builder("synchronization", false).immutable().autoPersist(false).build();
   public static final AttributeDefinition<Boolean> USE_1_PC_FOR_AUTO_COMMIT_TRANSACTIONS = AttributeDefinition.builder("single-phase-auto-commit", false).build();
   public static final AttributeDefinition<Long> REAPER_WAKE_UP_INTERVAL = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.REAPER_WAKE_UP_INTERVAL, 30000L).immutable().build();
   public static final AttributeDefinition<Long> COMPLETED_TX_TIMEOUT = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.COMPLETED_TX_TIMEOUT, 60000L).immutable().build();
   public static final AttributeDefinition<Boolean> NOTIFICATIONS = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.NOTIFICATIONS, true).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(TransactionConfiguration.class, AUTO_COMMIT, CACHE_STOP_TIMEOUT, LOCKING_MODE,
            TRANSACTION_MANAGER_LOOKUP, TRANSACTION_SYNCHRONIZATION_REGISTRY_LOOKUP, TRANSACTION_MODE, USE_SYNCHRONIZATION, USE_1_PC_FOR_AUTO_COMMIT_TRANSACTIONS,
            REAPER_WAKE_UP_INTERVAL, COMPLETED_TX_TIMEOUT, NOTIFICATIONS);
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
   private final Attribute<Boolean> notifications;
   private final RecoveryConfiguration recovery;
   private final boolean invocationBatching;

   TransactionConfiguration(AttributeSet attributes, RecoveryConfiguration recovery, boolean invocationBatching) {
      super(Element.TRANSACTION, attributes, recovery);
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
      notifications = attributes.attribute(NOTIFICATIONS);
      this.recovery = recovery;
      this.invocationBatching = invocationBatching;
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
    * Configure Transaction manager lookup directly using an instance of TransactionManagerLookup.
    * Calling this method marks the cache as transactional.
    */
   public TransactionManagerLookup transactionManagerLookup() {
      return transactionManagerLookup.get();
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
    * @return are transactional notifications (
    *    {@link org.infinispan.notifications.cachelistener.annotation.TransactionRegistered} and
    *    {@link org.infinispan.notifications.cachelistener.annotation.TransactionCompleted}) triggered?
    */
   public boolean notifications() {
      return notifications.get();
   }
}
