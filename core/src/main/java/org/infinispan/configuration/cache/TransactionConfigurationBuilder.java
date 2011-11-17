package org.infinispan.configuration.cache;

import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.infinispan.transaction.lookup.TransactionSynchronizationRegistryLookup;

public class TransactionConfigurationBuilder extends AbstractConfigurationChildBuilder<TransactionConfiguration> {

   private boolean autoCommit;
   private int cacheStopTimeout;
   private boolean eagerLockingSingleNode;
   private LockingMode lockingMode;
   private boolean syncCommitPhase;
   private boolean syncRollbackPhase;
   private TransactionManagerLookup transactionManagerLookup;
   private TransactionSynchronizationRegistryLookup transactionSynchronizationRegistryLookup;
   private TransactionMode transactionMode;
   private boolean useEagerLocking;
   private boolean useSynchronization;
   private final RecoveryConfigurationBuilder recovery;
   
   TransactionConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      this.recovery = new RecoveryConfigurationBuilder(this);
   }
   
   public TransactionConfigurationBuilder autoCommit(boolean b) {
      this.autoCommit = b;
      return this;
   }

   public TransactionConfigurationBuilder cacheStopTimeout(int i) {
      this.cacheStopTimeout = i;
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

   @Override
   void validate() {
      // TODO Auto-generated method stub
      
   }

   @Override
   TransactionConfiguration create() {
      if (useEagerLocking = true) {
         lockingMode = LockingMode.PESSIMISTIC;
      }
      return new TransactionConfiguration(autoCommit, cacheStopTimeout, eagerLockingSingleNode, lockingMode, syncCommitPhase, syncRollbackPhase, transactionManagerLookup, transactionSynchronizationRegistryLookup, transactionMode, useEagerLocking, useSynchronization, recovery.create());
   }
   
   
   
}
