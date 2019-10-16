package org.infinispan.partitionhandling.impl;

import static org.infinispan.commons.util.EnumUtil.EMPTY_BIT_SET;
import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

@Scope(Scopes.NAMED_CACHE)
public class PartitionHandlingManagerImpl implements PartitionHandlingManager {
   private static final Log log = LogFactory.getLog(PartitionHandlingManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();
   private final Map<GlobalTransaction, TransactionInfo> partialTransactions;
   private volatile AvailabilityMode availabilityMode = AvailabilityMode.AVAILABLE;

   @ComponentName(KnownComponentNames.CACHE_NAME)
   @Inject String cacheName;
   @Inject protected DistributionManager distributionManager;
   @Inject LocalTopologyManager localTopologyManager;
   @Inject CacheNotifier<Object, Object> notifier;
   @Inject CommandsFactory commandsFactory;
   @Inject Configuration configuration;
   @Inject RpcManager rpcManager;
   @Inject LockManager lockManager;
   @Inject Transport transport;

   private boolean isVersioned;
   private PartitionHandling partitionHandling;

   public PartitionHandlingManagerImpl() {
      partialTransactions = new ConcurrentHashMap<>();
   }

   @Start
   public void start() {
      isVersioned = Configurations.isTxVersioned(configuration);
      partitionHandling = configuration.clustering().partitionHandling().whenSplit();
   }

   @Override
   public AvailabilityMode getAvailabilityMode() {
      return availabilityMode;
   }

   private CompletionStage<Void> updateAvailabilityMode(AvailabilityMode mode) {
      log.debugf("Updating availability for cache %s: %s -> %s", cacheName, this.availabilityMode, mode);
      this.availabilityMode = mode;
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> setAvailabilityMode(AvailabilityMode availabilityMode) {
      if (availabilityMode != this.availabilityMode) {
         return notifier.notifyPartitionStatusChanged(availabilityMode, true)
               .thenCompose(ignore -> {
                  updateAvailabilityMode(availabilityMode);
                  return notifier.notifyPartitionStatusChanged(availabilityMode, false);
               });
      } else {
         return CompletableFutures.completedNull();
      }
   }

   @Override
   public void checkWrite(Object key) {
      doCheck(key, true, EMPTY_BIT_SET);
   }

   @Override
   public void checkRead(Object key, long flagBitSet) {
      doCheck(key, false, flagBitSet);
   }

   @Override
   public void checkClear() {
      if (!isBulkOperationAllowed(true)) {
         throw CONTAINER.clearDisallowedWhilePartitioned();
      }
   }

   @Override
   public void checkBulkRead() {
      if (!isBulkOperationAllowed(false)) {
         throw CONTAINER.partitionDegraded();
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
      // If we are going to commit, we can't release the resources yet
      boolean partiallyCommitted = transactionInfo != null && !transactionInfo.isRolledBack();
      if (trace) {
         log.tracef("Can release resources for transaction %s? %s. Transaction info=%s", globalTransaction,
               !partiallyCommitted, transactionInfo);
      }
      return partiallyCommitted;
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
      if (isStable) {
         if (trace) {
            log.tracef("On stable topology update. Pending txs: %d", partialTransactions.size());
         }
         for (TransactionInfo transactionInfo : partialTransactions.values()) {
            completeTransaction(transactionInfo, cacheTopology);
         }
      }
   }

   private void completeTransaction(final TransactionInfo transactionInfo, CacheTopology cacheTopology) {
      List<Address> commitNodes = transactionInfo.getCommitNodes(cacheTopology);
      TransactionBoundaryCommand command = transactionInfo.buildCommand(commandsFactory, isVersioned);
      command.setTopologyId(cacheTopology.getTopologyId());
      CompletionStage<Map<Address, Response>> remoteInvocation = commitNodes != null ?
            rpcManager.invokeCommand(commitNodes, command, MapResponseCollector.validOnly(commitNodes.size()),
                                     rpcManager.getSyncRpcOptions()) :
            rpcManager.invokeCommandOnAll(command, MapResponseCollector.ignoreLeavers(),
                                          rpcManager.getSyncRpcOptions());
      remoteInvocation.whenComplete((responseMap, throwable) -> {
         final GlobalTransaction globalTransaction = transactionInfo.getGlobalTransaction();
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
                  log.tracef("Another partition or topology changed for transaction %s. Retry later.",
                             globalTransaction);
               }
               return;
            }
         }
         if (trace) {
            log.tracef("Performing cleanup for transaction %s", globalTransaction);
         }
         lockManager.unlock(transactionInfo.getLockedKeys(), globalTransaction);
         partialTransactions.remove(globalTransaction);
         TxCompletionNotificationCommand completionCommand =
               commandsFactory.buildTxCompletionNotificationCommand(null, globalTransaction);
         // A little bit overkill, but the state transfer can happen during a merge and some nodes can receive the
         // transaction that aren't in the original affected nodes.
         // no side effects.
         rpcManager.sendToAll(completionCommand, DeliverOrder.NONE);
      });
   }

   private boolean isTopologyStable(CacheTopology cacheTopology) {
      CacheTopology stableTopology = localTopologyManager.getStableCacheTopology(cacheName);
      if (trace) {
         log.tracef("Check if topology %s is stable. Last stable topology is %s", cacheTopology, stableTopology);
      }
      return stableTopology != null && cacheTopology.getActualMembers().containsAll(stableTopology.getActualMembers()) && cacheTopology.getPhase() != CacheTopology.Phase.CONFLICT_RESOLUTION;
   }

   protected void doCheck(Object key, boolean isWrite, long flagBitSet) {
      if (trace) log.tracef("Checking availability for key=%s, status=%s", key, availabilityMode);
      if (availabilityMode == AvailabilityMode.AVAILABLE)
         return;

      LocalizedCacheTopology cacheTopology = distributionManager.getCacheTopology();
      boolean operationAllowed = isKeyOperationAllowed(isWrite, flagBitSet, cacheTopology, key);
      if (!operationAllowed) {
         if (trace) log.tracef("Partition is in %s mode, PartitionHandling is set to to %s, access is not allowed for key %s", availabilityMode, partitionHandling, key);
         if (EnumUtil.containsAny(flagBitSet, FlagBitSets.FORCE_WRITE_LOCK)) {
            throw CONTAINER.degradedModeLockUnavailable(key);
         } else {
            throw CONTAINER.degradedModeKeyUnavailable(key);
         }
      } else {
         if (trace) log.tracef("Key %s is available.", key);
      }
   }

   /**
    * Check if a read/write operation is allowed with the actual members
    *
    * @param isWrite       {@code false} for reads, {@code true} for writes
    * @param flagBitSet    reads with the {@link org.infinispan.context.Flag#FORCE_WRITE_LOCK} are treated as writes
    * @param cacheTopology actual members, or {@code null} for bulk operations
    * @param key           key owners, or {@code null} for bulk operations
    * @return
    */
   protected boolean isKeyOperationAllowed(boolean isWrite, long flagBitSet,
                                           LocalizedCacheTopology cacheTopology, Object key) {
      if (availabilityMode == AvailabilityMode.AVAILABLE)
         return true;

      assert partitionHandling != PartitionHandling.ALLOW_READ_WRITES :
         "ALLOW_READ_WRITES caches should always be AVAILABLE";

      List<Address> actualMembers = cacheTopology.getActualMembers();
      switch (partitionHandling) {
         case ALLOW_READS:
            List<Address> owners = getOwners(cacheTopology, key, isWrite);
            if (isWrite || EnumUtil.containsAny(flagBitSet, FlagBitSets.FORCE_WRITE_LOCK)) {
               // Writes require all the owners to be in the local partition
               return actualMembers.containsAll(owners);
            } else {
               // Reads only require one owner in the local partition
               return InfinispanCollections.containsAny(actualMembers, owners);
            }
         case DENY_READ_WRITES:
            // Both reads and writes require all the owners to be in the local partition
            return actualMembers.containsAll(getOwners(cacheTopology, key, isWrite));
         default:
            throw new IllegalStateException("Unsupported partition handling type: " + partitionHandling);
      }
   }

   protected boolean isBulkOperationAllowed(boolean isWrite) {
      if (availabilityMode == AvailabilityMode.AVAILABLE)
         return true;

      assert partitionHandling != PartitionHandling.ALLOW_READ_WRITES :
         "ALLOW_READ_WRITES caches should always be AVAILABLE";

      // We reject bulk writes because some owners are always missing in degraded mode
      if (isWrite)
         return false;

      switch (partitionHandling) {
         case ALLOW_READS:
            // Bulk reads require only one owner of each segment in the local partition
            LocalizedCacheTopology cacheTopology = distributionManager.getCacheTopology();
            for (int i = 0; i < cacheTopology.getReadConsistentHash().getNumSegments(); i++) {
               List<Address> owners = cacheTopology.getSegmentDistribution(i).readOwners();
               if (!InfinispanCollections.containsAny(owners, cacheTopology.getActualMembers()))
                  return false;
            }
            return true;
         case DENY_READ_WRITES:
            return false;
         default:
            throw new IllegalStateException("Unsupported partition handling type: " + partitionHandling);
      }
   }

   protected PartitionHandling getPartitionHandling() {
      return partitionHandling;
   }

   private List<Address> getOwners(LocalizedCacheTopology cacheTopology, Object key, boolean isWrite) {
      DistributionInfo distribution = cacheTopology.getDistribution(key);
      return isWrite ? distribution.writeOwners() : distribution.readOwners();
   }

   private interface TransactionInfo {
      boolean isRolledBack();

      List<Address> getCommitNodes(CacheTopology stableTopology);

      TransactionBoundaryCommand buildCommand(CommandsFactory commandsFactory, boolean isVersioned);

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
      public TransactionBoundaryCommand buildCommand(CommandsFactory commandsFactory, boolean isVersioned) {
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
      public TransactionBoundaryCommand buildCommand(CommandsFactory commandsFactory, boolean isVersioned) {
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
      public TransactionBoundaryCommand buildCommand(CommandsFactory commandsFactory, boolean isVersioned) {
         if (isVersioned) {
            throw new IllegalArgumentException("Cannot build a versioned one-phase-commit prepare command.");
         }
         return commandsFactory.buildPrepareCommand(getGlobalTransaction(), modifications, true);
      }
   }

   private static abstract class BaseTransactionInfo implements TransactionInfo {
      private final GlobalTransaction globalTransaction;
      private final Collection<Address> affectedNodes;
      private final Collection<Object> lockedKeys;

      protected BaseTransactionInfo(GlobalTransaction globalTransaction, Collection<Address> affectedNodes, Collection<Object> lockedKeys) {
         this.globalTransaction = globalTransaction;
         this.lockedKeys = lockedKeys;
         this.affectedNodes = affectedNodes;
      }

      @Override
      public final List<Address> getCommitNodes(CacheTopology stableTopology) {
         if (affectedNodes == null) {
            return null;
         } else {
            List<Address> commitNodes = new ArrayList<>(affectedNodes);
            commitNodes.retainAll(stableTopology.getActualMembers());
            return commitNodes;
         }
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
