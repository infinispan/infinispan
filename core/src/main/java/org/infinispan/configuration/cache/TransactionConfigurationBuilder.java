package org.infinispan.configuration.cache;

import static org.infinispan.configuration.cache.TransactionConfiguration.AUTO_COMMIT;
import static org.infinispan.configuration.cache.TransactionConfiguration.CACHE_STOP_TIMEOUT;
import static org.infinispan.configuration.cache.TransactionConfiguration.COMPLETED_TX_TIMEOUT;
import static org.infinispan.configuration.cache.TransactionConfiguration.LOCKING_MODE;
import static org.infinispan.configuration.cache.TransactionConfiguration.NOTIFICATIONS;
import static org.infinispan.configuration.cache.TransactionConfiguration.REAPER_WAKE_UP_INTERVAL;
import static org.infinispan.configuration.cache.TransactionConfiguration.TRANSACTION_MANAGER_LOOKUP;
import static org.infinispan.configuration.cache.TransactionConfiguration.TRANSACTION_MODE;
import static org.infinispan.configuration.cache.TransactionConfiguration.TRANSACTION_SYNCHRONIZATION_REGISTRY_LOOKUP;
import static org.infinispan.configuration.cache.TransactionConfiguration.USE_1_PC_FOR_AUTO_COMMIT_TRANSACTIONS;
import static org.infinispan.configuration.cache.TransactionConfiguration.USE_SYNCHRONIZATION;
import static org.infinispan.util.logging.Log.CONFIG;

import java.util.concurrent.TimeUnit;

import jakarta.transaction.Synchronization;
import jakarta.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.tx.lookup.TransactionManagerLookup;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.TransactionSynchronizationRegistryLookup;

/**
 * Defines transactional (JTA) characteristics of the cache.
 *
 * @author pmuir
 * @author Pedro Ruivo
 */
public class TransactionConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<TransactionConfiguration> {
   final AttributeSet attributes;
   private final RecoveryConfigurationBuilder recovery;

   TransactionConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      attributes = TransactionConfiguration.attributeDefinitionSet();
      this.recovery = new RecoveryConfigurationBuilder(this);
   }

   /**
    * If the cache is transactional (i.e. {@link #transactionMode(org.infinispan.transaction.TransactionMode)} == TransactionMode.TRANSACTIONAL)
    * and transactionAutoCommit is enabled then for single operation transactions
    * the user doesn't need to manually start a transaction, but a transactions
    * is injected by the system. Defaults to true.
    */
   public TransactionConfigurationBuilder autoCommit(boolean b) {
      attributes.attribute(AUTO_COMMIT).set(b);
      return this;
   }

   /**
    * If there are any ongoing transactions when a cache is stopped, Infinispan waits for ongoing
    * remote and local transactions to finish. The amount of time to wait for is defined by the
    * cache stop timeout. It is recommended that this value does not exceed the transaction timeout
    * because even if a new transaction was started just before the cache was stopped, this could
    * only last as long as the transaction timeout allows it.
    * <p/>
    * This configuration property may be adjusted at runtime
    */
   public TransactionConfigurationBuilder cacheStopTimeout(long l) {
      attributes.attribute(CACHE_STOP_TIMEOUT).set(l);
      return this;
   }

   /**
    * If there are any ongoing transactions when a cache is stopped, Infinispan waits for ongoing
    * remote and local transactions to finish. The amount of time to wait for is defined by the
    * cache stop timeout. It is recommended that this value does not exceed the transaction timeout
    * because even if a new transaction was started just before the cache was stopped, this could
    * only last as long as the transaction timeout allows it.
    * <p/>
    * This configuration property may be adjusted at runtime
    */
   public TransactionConfigurationBuilder cacheStopTimeout(long l, TimeUnit unit) {
      return cacheStopTimeout(unit.toMillis(l));
   }

   /**
    * Configures whether the cache uses optimistic or pessimistic locking. If the cache is not
    * transactional then the locking mode is ignored.
    *
    * @see org.infinispan.configuration.cache.TransactionConfiguration#transactionMode()
    */
   public TransactionConfigurationBuilder lockingMode(LockingMode lockingMode) {
      attributes.attribute(LOCKING_MODE).set(lockingMode);
      return this;
   }

   LockingMode lockingMode() {
      return attributes.attribute(LOCKING_MODE).get();
   }

   /**
    * Configure Transaction manager lookup directly using an instance of TransactionManagerLookup.
    * Calling this method marks the cache as transactional.
    */
   public TransactionConfigurationBuilder transactionManagerLookup(TransactionManagerLookup tml) {
      attributes.attribute(TRANSACTION_MANAGER_LOOKUP).set(tml);
      if (tml != null) {
         this.transactionMode(TransactionMode.TRANSACTIONAL);
      }
      return this;
   }

   public TransactionManagerLookup transactionManagerLookup() {
      return attributes.attribute(TRANSACTION_MANAGER_LOOKUP).get();
   }

   /**
    * Configure Transaction Synchronization Registry lookup directly using an instance of
    * TransactionManagerLookup. Calling this method marks the cache as transactional.
    */
   public TransactionConfigurationBuilder transactionSynchronizationRegistryLookup(TransactionSynchronizationRegistryLookup lookup) {
      attributes.attribute(TRANSACTION_SYNCHRONIZATION_REGISTRY_LOOKUP).set(lookup);
      return this;
   }

   public TransactionConfigurationBuilder transactionMode(TransactionMode transactionMode) {
      attributes.attribute(TRANSACTION_MODE).set(transactionMode);
      return this;
   }

   public TransactionMode transactionMode() {
      return attributes.attribute(TRANSACTION_MODE).get();
   }

   /**
    * Configures whether the cache registers a synchronization with the transaction manager, or registers itself as an
    * XA resource. It is often unnecessary to register as a full XA resource unless you intend to make use of recovery
    * as well, and registering a synchronization is significantly more efficient.
    * @param b if true, {@link Synchronization}s are used rather than {@link XAResource}s when communicating with a {@link TransactionManager}.
    */
   public TransactionConfigurationBuilder useSynchronization(boolean b) {
      attributes.attribute(USE_SYNCHRONIZATION).set(b);
      return this;
   }

   /**
    * See {@link #useSynchronization(boolean)}
    *
    * @return {@code true} if synchronization enlistment is enabled
    */
   boolean useSynchronization() {
      return attributes.attribute(USE_SYNCHRONIZATION).get();
   }

   /**
    * This method allows configuration of the transaction recovery cache. When this method is
    * called, it automatically enables recovery. So, if you want it to be disabled, make sure you
    * call {@link RecoveryConfigurationBuilder#disable()} )}
    */
   public RecoveryConfigurationBuilder recovery() {
      return recovery;
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
    */
   public TransactionConfigurationBuilder use1PcForAutoCommitTransactions(boolean b) {
      attributes.attribute(USE_1_PC_FOR_AUTO_COMMIT_TRANSACTIONS).set(b);
      return this;
   }

   public boolean use1PcForAutoCommitTransactions() {
      return attributes.attribute(USE_1_PC_FOR_AUTO_COMMIT_TRANSACTIONS).get();
   }

   /**
    *The time interval (millis) at which the thread that cleans up transaction completion information kicks in. Defaults to 30000.
    */
   public TransactionConfigurationBuilder reaperWakeUpInterval(long interval) {
      attributes.attribute(REAPER_WAKE_UP_INTERVAL).set(interval);
      return this;
   }

   /**
    * The duration (millis) in which to keep information about the completion of a transaction. Defaults to 60000.
    */
   public TransactionConfigurationBuilder completedTxTimeout(long timeout) {
      attributes.attribute(COMPLETED_TX_TIMEOUT).set(timeout);
      return this;
   }

   /**
    * @return are transactional notifications (
    *    {@link org.infinispan.notifications.cachelistener.annotation.TransactionRegistered} and
    *    {@link org.infinispan.notifications.cachelistener.annotation.TransactionCompleted}) triggered?
    */
   public TransactionConfigurationBuilder notifications(boolean enabled) {
      attributes.attribute(NOTIFICATIONS).set(enabled);
      return this;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public void validate() {
      Attribute<Long> reaperWakeUpInterval = attributes.attribute(REAPER_WAKE_UP_INTERVAL);
      Attribute<Long> completedTxTimeout = attributes.attribute(COMPLETED_TX_TIMEOUT);
      if (reaperWakeUpInterval.get()< 0)
         throw CONFIG.invalidReaperWakeUpInterval(reaperWakeUpInterval.get());
      if (completedTxTimeout.get() < 0)
         throw CONFIG.invalidCompletedTxTimeout(completedTxTimeout.get());
      CacheMode cacheMode = clustering().cacheMode();
      if (!attributes.attribute(NOTIFICATIONS).get() && !getBuilder().template()) {
         CONFIG.transactionNotificationsDisabled();
      }
      if (attributes.attribute(TRANSACTION_MODE).get().isTransactional() && !cacheMode.isSynchronous()) {
         throw CONFIG.unsupportedAsyncCacheMode(cacheMode);
      }
      recovery.validate();
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
      recovery.validate(globalConfig);
   }

   @Override
   public TransactionConfiguration create() {
      boolean invocationBatching = builder.invocationBatching().isEnabled();
      return new TransactionConfiguration(attributes.protect(), recovery.create(), invocationBatching);
   }

   @Override
   public TransactionConfigurationBuilder read(TransactionConfiguration template) {
      this.attributes.read(template.attributes());
      this.recovery.read(template.recovery());

      return this;
   }

   @Override
   public String toString() {
      return "TransactionConfigurationBuilder [attributes=" + attributes + ", recovery=" + recovery + "]";
   }

}
