/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
package org.infinispan.configuration.cache;

import org.infinispan.CacheConfigurationException;
import org.infinispan.configuration.Builder;
import org.infinispan.config.ConfigurationException;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.transaction.lookup.GenericTransactionManagerLookup;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.infinispan.transaction.lookup.TransactionSynchronizationRegistryLookup;
import javax.transaction.Synchronization;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;
import java.util.concurrent.TimeUnit;

/**
 * Defines transactional (JTA) characteristics of the cache.
 *
 * @author pmuir
 * @author Pedro Ruivo
 */
public class TransactionConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<TransactionConfiguration> {

   private boolean autoCommit = true;
   private long cacheStopTimeout = TimeUnit.SECONDS.toMillis(30);
   private boolean eagerLockingSingleNode = false;
   LockingMode lockingMode = LockingMode.OPTIMISTIC;
   private boolean syncCommitPhase = true;
   private boolean syncRollbackPhase = false;
   private TransactionManagerLookup transactionManagerLookup = new GenericTransactionManagerLookup();
   private TransactionSynchronizationRegistryLookup transactionSynchronizationRegistryLookup;
   TransactionMode transactionMode = null;
   private boolean useEagerLocking = false;
   private boolean useSynchronization = true;
   private final RecoveryConfigurationBuilder recovery;
   private boolean use1PcForAutoCommitTransactions = false;
   private long reaperWakeUpInterval = 1000;
   private long completedTxTimeout = 15000;
   private TransactionProtocol transactionProtocol = TransactionProtocol.DEFAULT;


   TransactionConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      this.recovery = new RecoveryConfigurationBuilder(this);
   }

   /**
    * If the cache is transactional (i.e. {@link #transactionMode(org.infinispan.transaction.TransactionMode)} == TransactionMode.TRANSACTIONAL)
    * and transactionAutoCommit is enabled then for single operation transactions
    * the user doesn't need to manually start a transaction, but a transactions
    * is injected by the system. Defaults to true.
    */
   public TransactionConfigurationBuilder autoCommit(boolean b) {
      this.autoCommit = b;
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
      this.cacheStopTimeout = l;
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
   public TransactionConfigurationBuilder eagerLockingSingleNode(boolean b) {
      this.eagerLockingSingleNode = b;
      return this;
   }

   /**
    * Configures whether the cache uses optimistic or pessimistic locking. If the cache is not
    * transactional then the locking mode is ignored.
    *
    * @see org.infinispan.config.Configuration#isTransactionalCache()
    */
   public TransactionConfigurationBuilder lockingMode(LockingMode lockingMode) {
      this.lockingMode = lockingMode;
      return this;
   }

   /**
    * If true, the cluster-wide commit phase in two-phase commit (2PC) transactions will be
    * synchronous, so Infinispan will wait for responses from all nodes to which the commit was
    * sent. Otherwise, the commit phase will be asynchronous. Keeping it as false improves
    * performance of 2PC transactions, since any remote failures are trapped during the prepare
    * phase anyway and appropriate rollbacks are issued.
    * <p/>
    * This configuration property may be adjusted at runtime
    */
   public TransactionConfigurationBuilder syncCommitPhase(boolean b) {
      this.syncCommitPhase = b;
      return this;
   }

   /**
    * See {@link #syncCommitPhase(boolean)}
    *
    * @return {@code true} if sync commit phase is enabled
    */
   public boolean syncCommitPhase() {
      return syncCommitPhase;
   }

   /**
    * If true, the cluster-wide rollback phase in two-phase commit (2PC) transactions will be
    * synchronous, so Infinispan will wait for responses from all nodes to which the rollback was
    * sent. Otherwise, the rollback phase will be asynchronous. Keeping it as false improves
    * performance of 2PC transactions.
    * <p />
    *
    * This configuration property may be adjusted at runtime.
    */
   public TransactionConfigurationBuilder syncRollbackPhase(boolean b) {
      this.syncRollbackPhase = b;
      return this;
   }

   /**
    * Configure Transaction manager lookup directly using an instance of TransactionManagerLookup.
    * Calling this method marks the cache as transactional.
    */
   public TransactionConfigurationBuilder transactionManagerLookup(TransactionManagerLookup tml) {
      this.transactionManagerLookup = tml;
      return this;
   }

   /**
    * Configure Transaction Synchronization Registry lookup directly using an instance of
    * TransactionManagerLookup. Calling this method marks the cache as transactional.
    */
   public TransactionConfigurationBuilder transactionSynchronizationRegistryLookup(
         TransactionSynchronizationRegistryLookup lookup) {
      this.transactionSynchronizationRegistryLookup = lookup;
      return this;
   }

   public TransactionConfigurationBuilder transactionMode(TransactionMode transactionMode) {
      this.transactionMode = transactionMode;
      return this;
   }

   /**
    * Only has effect for DIST mode and when useEagerLocking is set to true. When this is enabled,
    * then only one node is locked in the cluster, disregarding numOwners config. On the opposite,
    * if this is false, then on all cache.lock() calls numOwners RPCs are being performed. The node
    * that gets locked is the main data owner, i.e. the node where data would reside if
    * numOwners==1. If the node where the lock resides crashes, then the transaction is marked for
    * rollback - data is in a consistent state, no fault tolerance.
    * <p />
    * Note: Starting with infinispan 5.1 eager locking is replaced with pessimistic locking and can
    * be enforced by setting transaction's locking mode to PESSIMISTIC.
    */
   @Deprecated
   public TransactionConfigurationBuilder useEagerLocking(boolean b) {
      this.useEagerLocking = b;
      return this;
   }

   /**
    * Configures whether the cache registers a synchronization with the transaction manager, or registers itself as an
    * XA resource. It is often unnecessary to register as a full XA resource unless you intend to make use of recovery
    * as well, and registering a synchronization is significantly more efficient.
    * @param b if true, {@link Synchronization}s are used rather than {@link XAResource}s when communicating with a {@link TransactionManager}.
    */
   public TransactionConfigurationBuilder useSynchronization(boolean b) {
      this.useSynchronization = b;
      return this;
   }

   /**
    * See {@link #useSynchronization(boolean)}
    *
    * @return {@code true} if synchronization enlistment is enabled
    */
   public boolean useSynchronization() {
      return useSynchronization;
   }

   /**
    * This method allows configuration of the transaction recovery cache. When this method is
    * called, it automatically enables recovery. So, if you want it to be disabled, make sure you
    * call {@link org.infinispan.config.FluentConfiguration.RecoveryConfig#disable()}
    */
   public RecoveryConfigurationBuilder recovery() {
      recovery.enable();
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
      this.use1PcForAutoCommitTransactions = b;
      return this;
   }

   /**
    *The time interval (millis) at which the thread that cleans up transaction completion information kicks in. Defaults to 1000.
    */
   public TransactionConfigurationBuilder reaperWakeUpInterval(long interval) {
      this.reaperWakeUpInterval = interval;
      return this;
   }

   /**
    * The duration (millis) in which to keep information about the completion of a transaction. Defaults to 3000.
    */
   public TransactionConfigurationBuilder completedTxTimeout(long timeout) {
      this.completedTxTimeout = timeout;
      return this;
   }

   @Override
   public void validate() {
      if (reaperWakeUpInterval < 0)
         throw new CacheConfigurationException("reaperWakeUpInterval must be > 0, we got " + reaperWakeUpInterval);
      if (completedTxTimeout < 0)
         throw new CacheConfigurationException("completedTxTimeout must be > 0, we got " + reaperWakeUpInterval);
      if(transactionProtocol == TransactionProtocol.TOTAL_ORDER) {
         //total order only supports transactional caches
         if(transactionMode != TransactionMode.TRANSACTIONAL) {
            throw new ConfigurationException("Total Order based protocol not available in " + transactionMode +" cache");
         }

         //total order only supports replicated and distributed mode
         if(!clustering().cacheMode().isReplicated() && !clustering().cacheMode().isDistributed()) {
            throw new ConfigurationException(clustering().cacheMode().friendlyCacheModeString() + " is not supported by Total Order based protocol");
         }

         if (recovery.create().enabled()) {
            throw new ConfigurationException("Total Order based protocol not available with recovery");
         }

         if (lockingMode != LockingMode.OPTIMISTIC) {
            throw new ConfigurationException("Total Order based protocol not available with " + lockingMode);
         }
      }
      recovery.validate();
   }

   @Override
   public TransactionConfiguration create() {
      if (useEagerLocking) {
         lockingMode = LockingMode.PESSIMISTIC;
      }
      if (transactionMode == null && getBuilder().invocationBatching().enabled)
         transactionMode = TransactionMode.TRANSACTIONAL;
      else if (transactionMode == null)
         transactionMode = TransactionMode.NON_TRANSACTIONAL;
      return new TransactionConfiguration(autoCommit, cacheStopTimeout, eagerLockingSingleNode, lockingMode, syncCommitPhase,
            syncRollbackPhase, transactionManagerLookup, transactionSynchronizationRegistryLookup, transactionMode,
            useEagerLocking, useSynchronization, use1PcForAutoCommitTransactions, reaperWakeUpInterval, completedTxTimeout, recovery.create(), transactionProtocol);
   }

   @Override
   public TransactionConfigurationBuilder read(TransactionConfiguration template) {
      this.autoCommit = template.autoCommit();
      this.cacheStopTimeout = template.cacheStopTimeout();
      this.eagerLockingSingleNode = template.eagerLockingSingleNode();
      this.lockingMode = template.lockingMode();
      this.syncCommitPhase = template.syncCommitPhase();
      this.syncRollbackPhase = template.syncRollbackPhase();
      this.transactionManagerLookup = template.transactionManagerLookup();
      this.transactionMode = template.transactionMode();
      this.transactionSynchronizationRegistryLookup = template.transactionSynchronizationRegistryLookup();
      this.useEagerLocking = template.useEagerLocking();
      this.useSynchronization = template.useSynchronization();
      this.use1PcForAutoCommitTransactions = template.use1PcForAutoCommitTransactions();
      this.recovery.read(template.recovery());
      this.reaperWakeUpInterval = template.reaperWakeUpInterval();
      this.completedTxTimeout = template.completedTxTimeout();
      this.transactionProtocol = template.transactionProtocol();

      return this;
   }

   @Override
   public String toString() {
      return "TransactionConfigurationBuilder{" +
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
            ", use1PcForAutoCommitTransactions=" + use1PcForAutoCommitTransactions +
            ", completedTxTimeout=" + completedTxTimeout +
            ", reaperWakeUpInterval=" + reaperWakeUpInterval +
            '}';
   }

   public TransactionConfigurationBuilder transactionProtocol(TransactionProtocol transactionProtocol) {
      this.transactionProtocol = transactionProtocol;
      return this;
   }
}
