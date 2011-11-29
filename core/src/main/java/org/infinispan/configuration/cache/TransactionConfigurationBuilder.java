package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.infinispan.transaction.lookup.TransactionSynchronizationRegistryLookup;

public class TransactionConfigurationBuilder extends AbstractConfigurationChildBuilder<TransactionConfiguration> {

   private boolean autoCommit = true;
   private long cacheStopTimeout = TimeUnit.SECONDS.toMillis(30);
   private boolean eagerLockingSingleNode = false;
   private LockingMode lockingMode = LockingMode.OPTIMISTIC;
   private boolean syncCommitPhase = true;
   private boolean syncRollbackPhase = false;
   private TransactionManagerLookup transactionManagerLookup ;
   private TransactionSynchronizationRegistryLookup transactionSynchronizationRegistryLookup;
   private TransactionMode transactionMode = TransactionMode.NON_TRANSACTIONAL;
   private boolean useEagerLocking = false;
   private boolean useSynchronization = false;
   private final RecoveryConfigurationBuilder recovery;
   private boolean use1PcForAutoCommitTransactions = false;

   TransactionConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      this.recovery = new RecoveryConfigurationBuilder(this);
   }
   
   public TransactionConfigurationBuilder autoCommit(boolean b) {
      this.autoCommit = b;
      return this;
   }

   @Deprecated
   public TransactionConfigurationBuilder cacheStopTimeout(int i) {
      this.cacheStopTimeout = i;
      return this;
   }
   
   public TransactionConfigurationBuilder cacheStopTimeout(long l) {
      this.cacheStopTimeout = l;
      return this;
   }

   public TransactionConfigurationBuilder eagerLockingSingleNode(boolean b) {
      this.eagerLockingSingleNode = b;
      return this;
   }

   public TransactionConfigurationBuilder lockingMode(LockingMode lockingMode) {
      this.lockingMode = lockingMode;
      return this;
   }

   public TransactionConfigurationBuilder syncCommitPhase(boolean b) {
      this.syncCommitPhase = b;
      return this;
   }

   public TransactionConfigurationBuilder syncRollbackPhase(boolean b) {
      this.syncRollbackPhase = b;
      return this;
   }

   public TransactionConfigurationBuilder transactionManagerLookup(TransactionManagerLookup tml) {
      this.transactionManagerLookup = tml;
      return this;
   }
   
   public TransactionConfigurationBuilder transactionSyncrontizationRegisteryLookup(TransactionSynchronizationRegistryLookup lookup) {
      this.transactionSynchronizationRegistryLookup = lookup;
      return this;
   }

   public TransactionConfigurationBuilder transactionMode(TransactionMode transactionMode) {
      this.transactionMode = transactionMode;
      return this;
   }

   public TransactionConfigurationBuilder useEagerLocking(boolean b) {
      this.useEagerLocking = b;
      return this;
   }

   public TransactionConfigurationBuilder useSynchronization(boolean b) {
      this.useSynchronization = b;
      return this;
   }
   
   public RecoveryConfigurationBuilder recovery() {
      return recovery;
   }

   public TransactionConfigurationBuilder use1PcForAutoCommitTransactions(boolean b) {
      this.use1PcForAutoCommitTransactions = b;
      return this;
   }

   @Override
   void validate() {
      // TODO Auto-generated method stub
      
   }

   @Override
   TransactionConfiguration create() {
      if (useEagerLocking) {
         lockingMode = LockingMode.PESSIMISTIC;
      }
      return new TransactionConfiguration(autoCommit, cacheStopTimeout, eagerLockingSingleNode, lockingMode, syncCommitPhase, syncRollbackPhase, transactionManagerLookup, transactionSynchronizationRegistryLookup, transactionMode, useEagerLocking, useSynchronization, recovery.create());
   }
}
