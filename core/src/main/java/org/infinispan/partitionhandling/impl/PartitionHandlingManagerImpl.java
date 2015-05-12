package org.infinispan.partitionhandling.impl;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.*;

public class PartitionHandlingManagerImpl implements PartitionHandlingManager {
   private static final Log log = LogFactory.getLog(PartitionHandlingManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();
   private final Map<GlobalTransaction, TransactionInfo> partialTransactions;
   private volatile AvailabilityMode availabilityMode = AvailabilityMode.AVAILABLE;

   private DistributionManager distributionManager;
   private LocalTopologyManager localTopologyManager;
   private StateTransferManager stateTransferManager;
   private String cacheName;
   private CacheNotifier notifier;
   private CommandsFactory commandsFactory;
   private Configuration configuration;
   private RpcManager rpcManager;
   private LockManager lockManager;

   private boolean isVersioned;

   public PartitionHandlingManagerImpl() {
      partialTransactions = CollectionFactory.makeConcurrentMap();
   }

   @Inject
   public void init(DistributionManager distributionManager, LocalTopologyManager localTopologyManager,
                    StateTransferManager stateTransferManager, Cache cache, CacheNotifier notifier, CommandsFactory commandsFactory,
                    Configuration configuration, RpcManager rpcManager, LockManager lockManager) {
      this.distributionManager = distributionManager;
      this.localTopologyManager = localTopologyManager;
      this.stateTransferManager = stateTransferManager;
      this.cacheName = cache.getName();
      this.notifier = notifier;
      this.commandsFactory = commandsFactory;
      this.configuration = configuration;
      this.rpcManager = rpcManager;
      this.lockManager = lockManager;
   }

   @Start
   public void start() {
      isVersioned = Configurations.isVersioningEnabled(configuration);
   }

   @Override
   public AvailabilityMode getAvailabilityMode() {
      return availabilityMode;
   }

   @Override
   public void setAvailabilityMode(AvailabilityMode availabilityMode) {
      if (availabilityMode != this.availabilityMode) {
         log.debugf("Updating availability for cache %s: %s -> %s", cacheName, this.availabilityMode, availabilityMode);
         notifier.notifyPartitionStatusChanged(availabilityMode, true);
         this.availabilityMode = availabilityMode;
         notifier.notifyPartitionStatusChanged(availabilityMode, false);
      }
   }

   @Override
   public void checkWrite(Object key) {
      doCheck(key);
   }

   @Override
   public void checkRead(Object key) {
      doCheck(key);
   }

   @Override
   public void checkClear() {
      if (availabilityMode != AvailabilityMode.AVAILABLE) {
         throw log.clearDisallowedWhilePartitioned();
      }
   }

   @Override
   public void checkBulkRead() {
      if (availabilityMode != AvailabilityMode.AVAILABLE) {
         throw log.partitionDegraded();
      }
   }

   @Override
   public CacheTopology getLastStableTopology() {
      return localTopologyManager.getStableCacheTopology(cacheName);
   }

   @Override
   public boolean addPartialRollbackTransaction(GlobalTransaction globalTransaction, Collection<Address> affectedNodes, Collection<Object> lockedKeys) {
      if (trace) {
         log.tracef("Added partially rollback transaction %s", globalTransaction);
      }
      partialTransactions.put(globalTransaction, new RollbackTransactionInfo(globalTransaction, affectedNodes, lockedKeys));
      return true;
   }

   @Override
   public boolean addPartialCommit2PCTransaction(GlobalTransaction globalTransaction, Collection<Address> affectedNodes,
                                                 Collection<Object> lockedKeys, EntryVersionsMap newVersions) {
      if (trace) {
         log.tracef("Added partially committed (2PC) transaction %s", globalTransaction);
      }
      partialTransactions.put(globalTransaction, new Commit2PCTransactionInfo(globalTransaction, affectedNodes,
                                                                              lockedKeys, newVersions));
      return true;
   }

   @Override
   public boolean addPartialCommit1PCTransaction(GlobalTransaction globalTransaction, Collection<Address> affectedNodes,
                                                 Collection<Object> lockedKeys, List<WriteCommand> modifications) {
      if (trace) {
         log.tracef("Added partially committed (1PC) transaction %s", globalTransaction);
      }
      partialTransactions.put(globalTransaction, new Commit1PCTransactionInfo(globalTransaction, affectedNodes,
                                                                              lockedKeys, modifications));
      return true;
   }

   @Override
   public boolean isTransactionPartiallyCommitted(GlobalTransaction globalTransaction) {
      TransactionInfo transactionInfo = partialTransactions.get(globalTransaction);
      if (trace) {
         log.tracef("Can release resources for transaction %s. Transaction info=%s", globalTransaction, transactionInfo);
      }
      return transactionInfo != null && !transactionInfo.isRolledBack(); //if we are going to commit, we can't release the resources yet
   }

   @Override
   public Collection<GlobalTransaction> getPartialTransactions() {
      return Collections.unmodifiableCollection(partialTransactions.keySet());
   }

   @Override
   public boolean canRollbackTransactionAfterOriginatorLeave(GlobalTransaction globalTransaction) {
      boolean canRollback = availabilityMode == AvailabilityMode.AVAILABLE &&
            !getLastStableTopology().getActualMembers().contains(globalTransaction.getAddress());
      if (trace) {
         log.tracef("Can rollback transaction? %s", canRollback);
      }
      return canRollback;
   }

   @Override
   public void onTopologyUpdate(CacheTopology cacheTopology) {
      boolean isStable = isTopologyStable(cacheTopology);
      if (trace) {
         log.tracef("On stable topology update. Has pending tx? %b. Is stable? %b. topology=%s",
                    !partialTransactions.isEmpty(), isStable, cacheTopology);
      }
      if (isStable) {
         if (!partialTransactions.isEmpty()) {
            for (TransactionInfo transactionInfo : partialTransactions.values()) {
               completeTransaction(transactionInfo, cacheTopology);
            }
         }
      }
   }

   private void completeTransaction(final TransactionInfo transactionInfo, CacheTopology cacheTopology) {
      rpcManager.invokeRemotelyAsync(transactionInfo.getCommitNodes(cacheTopology),
              transactionInfo.buildCommand(commandsFactory, isVersioned),
              rpcManager.getDefaultRpcOptions(true))
              .whenComplete((responseMap, throwable) -> {
                 final GlobalTransaction globalTransaction = transactionInfo.getGlobalTransaction();
                 if (trace) {
                    log.tracef("Future done for transaction %s", globalTransaction);
                 }

                 if (throwable != null) {
                    if (trace) {
                       log.tracef(throwable, "Exception for transaction %s. Retry later.", globalTransaction);
                    }
                    return;
                 }

                 if (trace) {
                    log.tracef("Future done for transaction %s. Response are %s", globalTransaction, responseMap);
                 }

                 for (Response response : responseMap.values()) {
                    if (response == UnsureResponse.INSTANCE || response == CacheNotFoundResponse.INSTANCE) {
                       if (trace) {
                          log.tracef("Another partition or topology changed for transaction %s. Retry later.", globalTransaction);
                       }
                       return;
                    }
                 }
                 if (trace) {
                    log.tracef("Performing cleanup for transaction %s", globalTransaction);
                 }
                 lockManager.unlock(transactionInfo.getLockedKeys(), globalTransaction);
                 partialTransactions.remove(globalTransaction);
                 TxCompletionNotificationCommand command = commandsFactory.buildTxCompletionNotificationCommand(null, globalTransaction);
                 //a little bit overkill, but the state transfer can happen during a merge and some nodes can receive the
                 // transaction that aren't in the original affected nodes.
                 //no side effects.
                 rpcManager.invokeRemotely(null, command, rpcManager.getDefaultRpcOptions(false, DeliverOrder.NONE));
              });
   }

   private boolean isTopologyStable(CacheTopology cacheTopology) {
      CacheTopology stableTopology = localTopologyManager.getStableCacheTopology(cacheName);
      if (trace) {
         log.tracef("Check if topology %s is stable. Last stable topology is %s", cacheTopology, stableTopology);
      }
      return stableTopology != null && cacheTopology.getActualMembers().containsAll(stableTopology.getActualMembers());
   }

   private void doCheck(Object key) {
      if (trace) log.tracef("Checking availability for key=%s, status=%s", key, availabilityMode);
      if (availabilityMode == AvailabilityMode.AVAILABLE)
         return;

      List<Address> owners = distributionManager.locate(key);
      List<Address> actualMembers = stateTransferManager.getCacheTopology().getActualMembers();
      if (!actualMembers.containsAll(owners)) {
         if (trace) log.tracef("Partition is in %s mode, access is not allowed for key %s", availabilityMode, key);
         throw log.degradedModeKeyUnavailable(key);
      } else {
         if (trace) log.tracef("Key %s is available.", key);
      }
   }

   private interface TransactionInfo {
      boolean isRolledBack();

      List<Address> getCommitNodes(CacheTopology stableTopology);

      ReplicableCommand buildCommand(CommandsFactory commandsFactory, boolean isVersioned);

      GlobalTransaction getGlobalTransaction();

      Collection<Object> getLockedKeys();
   }

   private static class RollbackTransactionInfo extends BaseTransactionInfo {

      protected RollbackTransactionInfo(GlobalTransaction globalTransaction, Collection<Address> affectedNodes, Collection<Object> lockedKeys) {
         super(globalTransaction, affectedNodes, lockedKeys);
      }

      @Override
      public boolean isRolledBack() {
         return true;
      }

      @Override
      public ReplicableCommand buildCommand(CommandsFactory commandsFactory, boolean isVersioned) {
         return commandsFactory.buildRollbackCommand(getGlobalTransaction());
      }

   }

   private static class Commit2PCTransactionInfo extends BaseTransactionInfo {

      private final EntryVersionsMap newVersions;

      public Commit2PCTransactionInfo(GlobalTransaction globalTransaction, Collection<Address> affectedNodes, Collection<Object> lockedKeys, EntryVersionsMap newVersions) {
         super(globalTransaction, affectedNodes, lockedKeys);
         this.newVersions = newVersions;
      }

      @Override
      public boolean isRolledBack() {
         return false;
      }

      @Override
      public ReplicableCommand buildCommand(CommandsFactory commandsFactory, boolean isVersioned) {
         if (isVersioned) {
            VersionedCommitCommand commitCommand = commandsFactory.buildVersionedCommitCommand(getGlobalTransaction());
            commitCommand.setUpdatedVersions(newVersions);
            return commitCommand;
         } else {
            return commandsFactory.buildCommitCommand(getGlobalTransaction());
         }
      }
   }

   private static class Commit1PCTransactionInfo extends BaseTransactionInfo {

      private final List<WriteCommand> modifications;

      public Commit1PCTransactionInfo(GlobalTransaction globalTransaction, Collection<Address> affectedNodes, Collection<Object> lockedKeys, List<WriteCommand> modifications) {
         super(globalTransaction, affectedNodes, lockedKeys);
         this.modifications = modifications;
      }

      @Override
      public boolean isRolledBack() {
         return false;
      }

      @Override
      public ReplicableCommand buildCommand(CommandsFactory commandsFactory, boolean isVersioned) {
         if (isVersioned) {
            throw new IllegalArgumentException("Cannot build a versioned one-phase-commit prepare command.");
         }
         return commandsFactory.buildPrepareCommand(getGlobalTransaction(), modifications, true);
      }
   }

   private static abstract class BaseTransactionInfo implements TransactionInfo {
      private final GlobalTransaction globalTransaction;
      private final List<Address> affectedNodes;
      private final Collection<Object> lockedKeys;

      protected BaseTransactionInfo(GlobalTransaction globalTransaction, Collection<Address> affectedNodes, Collection<Object> lockedKeys) {
         this.globalTransaction = globalTransaction;
         this.lockedKeys = lockedKeys;
         this.affectedNodes = new ArrayList<>(affectedNodes);
      }

      @Override
      public final List<Address> getCommitNodes(CacheTopology stableTopology) {
         List<Address> commitNodes = new ArrayList<>(affectedNodes);
         commitNodes.retainAll(stableTopology.getActualMembers());
         return commitNodes;
      }

      @Override
      public final GlobalTransaction getGlobalTransaction() {
         return globalTransaction;
      }

      @Override
      public Collection<Object> getLockedKeys() {
         return lockedKeys;
      }

      @Override
      public String toString() {
         return "TransactionInfo{" +
               "globalTransaction=" + globalTransaction + ", " +
               "rollback=" + isRolledBack() + ", " +
               "affectedNodes=" + affectedNodes +
               '}';
      }
   }
}
