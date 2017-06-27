package org.infinispan.statetransfer;

import static org.infinispan.context.Flag.CACHE_MODE_LOCAL;
import static org.infinispan.context.Flag.IGNORE_RETURN_VALUES;
import static org.infinispan.context.Flag.PUT_FOR_STATE_TRANSFER;
import static org.infinispan.context.Flag.SKIP_LOCKING;
import static org.infinispan.context.Flag.SKIP_OWNERSHIP_CHECK;
import static org.infinispan.context.Flag.SKIP_REMOTE_LOOKUP;
import static org.infinispan.context.Flag.SKIP_SHARED_CACHE_STORE;
import static org.infinispan.context.Flag.SKIP_XSITE_BACKUP;
import static org.infinispan.factories.KnownComponentNames.STATE_TRANSFER_EXECUTOR;
import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.PRIVATE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.SmallIntSet;
import org.infinispan.commons.util.concurrent.ConcurrentHashSet;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.PartitionHandlingConfiguration;
import org.infinispan.conflict.impl.InternalConflictManager;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.TriangleOrderManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.executors.LimitedExecutor;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.filter.KeyFilter;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.totalorder.TotalOrderLatch;
import org.infinispan.transaction.totalorder.TotalOrderManager;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import net.jcip.annotations.GuardedBy;

/**
 * {@link StateConsumer} implementation.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class StateConsumerImpl implements StateConsumer {

   protected static final Log log = LogFactory.getLog(StateConsumerImpl.class);
   protected static final boolean trace = log.isTraceEnabled();
   protected static final int NO_STATE_TRANSFER_IN_PROGRESS = -1;
   protected static final long STATE_TRANSFER_FLAGS = EnumUtil.bitSetOf(PUT_FOR_STATE_TRANSFER, CACHE_MODE_LOCAL,
                                                                      IGNORE_RETURN_VALUES, SKIP_REMOTE_LOOKUP,
                                                                      SKIP_SHARED_CACHE_STORE, SKIP_OWNERSHIP_CHECK,
                                                                      SKIP_XSITE_BACKUP);

   protected Cache cache;
   protected StateTransferManager stateTransferManager;
   protected LocalTopologyManager localTopologyManager;
   protected String cacheName;
   protected Configuration configuration;
   protected RpcManager rpcManager;
   protected TransactionManager transactionManager;   // optional
   protected CommandsFactory commandsFactory;
   protected TransactionTable transactionTable;       // optional
   protected DataContainer<Object, Object> dataContainer;
   protected PersistenceManager persistenceManager;
   protected AsyncInterceptorChain interceptorChain;
   protected InvocationContextFactory icf;
   protected StateTransferLock stateTransferLock;
   protected CacheNotifier cacheNotifier;
   protected TotalOrderManager totalOrderManager;
   protected BlockingTaskAwareExecutorService remoteCommandsExecutor;
   protected long timeout;
   protected boolean isFetchEnabled;
   protected boolean isTransactional;
   protected boolean isInvalidationMode;
   protected boolean isTotalOrder;
   protected volatile KeyInvalidationListener keyInvalidationListener; //for test purpose only!
   protected CommitManager commitManager;
   protected ExecutorService stateTransferExecutor;
   protected CommandAckCollector commandAckCollector;
   protected TriangleOrderManager triangleOrderManager;
   protected DistributionManager distributionManager;
   protected KeyPartitioner keyPartitioner;
   private InternalConflictManager conflictManager;

   protected volatile CacheTopology cacheTopology;

   /**
    * Indicates if there is a state transfer in progress. It is set to the new topology id when onTopologyUpdate with
    * isRebalance==true is called.
    * It is changed back to NO_REBALANCE_IN_PROGRESS when a topology update with a null pending CH is received.
    */
   protected final AtomicInteger stateTransferTopologyId = new AtomicInteger(NO_STATE_TRANSFER_IN_PROGRESS);

   /**
    * Indicates if there is a rebalance in progress and there the local node has not yet received
    * all the new segments yet. It is set to true when rebalance starts and becomes when all inbound transfers have completed
    * (before stateTransferTopologyId is set back to NO_REBALANCE_IN_PROGRESS).
    */
   protected final AtomicBoolean waitingForState = new AtomicBoolean(false);

   protected final Object transferMapsLock = new Object();

   /**
    * A map that keeps track of current inbound state transfers by source address. There could be multiple transfers
    * flowing in from the same source (but for different segments) so the values are lists. This works in tandem with
    * transfersBySegment so they always need to be kept in sync and updates to both of them need to be atomic.
    */
   @GuardedBy("transferMapsLock")
   private final Map<Address, List<InboundTransferTask>> transfersBySource = new HashMap<>();

   /**
    * A map that keeps track of current inbound state transfers by segment id. There is at most one transfers per segment.
    * This works in tandem with transfersBySource so they always need to be kept in sync and updates to both of them
    * need to be atomic.
    */
   @GuardedBy("transferMapsLock")
   protected final Map<Integer, List<InboundTransferTask>> transfersBySegment = new HashMap<>();

   /**
    * Push RPCs on a background thread
    */
   protected LimitedExecutor stateRequestExecutor;

   private volatile boolean ownsData = false;

   protected RpcOptions synchronousRpcOptions;
   protected RpcOptions synchronousIgnoreLeaversRpcOptions;

   public StateConsumerImpl() {
   }

   /**
    * Stops applying incoming state. Also stops tracking updated keys. Should be called at the end of state transfer or
    * when a ClearCommand is committed during state transfer.
    * @param topologyId
    */
   @Override
   public void stopApplyingState(int topologyId) {
      if (trace) log.tracef("Stop keeping track of changed keys for state transfer in topology %d", topologyId);
      commitManager.stopTrack(PUT_FOR_STATE_TRANSFER);
   }

   @Inject
   public void init(Cache cache,
                    @ComponentName(STATE_TRANSFER_EXECUTOR) ExecutorService stateTransferExecutor,
                    StateTransferManager stateTransferManager,
                    LocalTopologyManager localTopologyManager,
                    AsyncInterceptorChain interceptorChain,
                    InvocationContextFactory icf,
                    Configuration configuration,
                    RpcManager rpcManager,
                    TransactionManager transactionManager,
                    CommandsFactory commandsFactory,
                    PersistenceManager persistenceManager,
                    DataContainer<Object, Object> dataContainer,
                    TransactionTable transactionTable,
                    StateTransferLock stateTransferLock,
                    CacheNotifier cacheNotifier,
                    TotalOrderManager totalOrderManager,
                    @ComponentName(
                          KnownComponentNames.REMOTE_COMMAND_EXECUTOR) BlockingTaskAwareExecutorService remoteCommandsExecutor,
                    CommitManager commitManager,
                    CommandAckCollector commandAckCollector,
                    TriangleOrderManager triangleOrderManager,
                    DistributionManager distributionManager, KeyPartitioner keyPartitioner,
                    InternalConflictManager conflictManager) {
      this.cache = cache;
      this.cacheName = cache.getName();
      this.stateTransferExecutor = stateTransferExecutor;
      this.stateTransferManager = stateTransferManager;
      this.localTopologyManager = localTopologyManager;
      this.interceptorChain = interceptorChain;
      this.icf = icf;
      this.configuration = configuration;
      this.rpcManager = rpcManager;
      this.transactionManager = transactionManager;
      this.commandsFactory = commandsFactory;
      this.persistenceManager = persistenceManager;
      this.dataContainer = dataContainer;
      this.transactionTable = transactionTable;
      this.stateTransferLock = stateTransferLock;
      this.cacheNotifier = cacheNotifier;
      this.totalOrderManager = totalOrderManager;
      this.remoteCommandsExecutor = remoteCommandsExecutor;
      this.commitManager = commitManager;
      this.commandAckCollector = commandAckCollector;
      this.triangleOrderManager = triangleOrderManager;
      this.distributionManager = distributionManager;
      this.keyPartitioner = keyPartitioner;
      this.conflictManager = conflictManager;

      isInvalidationMode = configuration.clustering().cacheMode().isInvalidation();

      isTransactional = configuration.transaction().transactionMode().isTransactional();
      isTotalOrder = configuration.transaction().transactionProtocol().isTotalOrder();

      timeout = configuration.clustering().stateTransfer().timeout();
   }

   public boolean hasActiveTransfers() {
      synchronized (transferMapsLock) {
         return !transfersBySource.isEmpty();
      }
   }

   @Override
   public boolean isStateTransferInProgress() {
      return stateTransferTopologyId.get() != NO_STATE_TRANSFER_IN_PROGRESS;
   }

   @Override
   public boolean isStateTransferInProgressForKey(Object key) {
      if (isInvalidationMode) {
         // In invalidation mode it is of not much relevance if the key is actually being transferred right now.
         // A false response to this will just mean the usual remote lookup before a write operation is not
         // performed and a null is assumed. But in invalidation mode the user must expect the data can disappear
         // from cache at any time so this null previous value should not cause any trouble.
         return false;
      }

      DistributionInfo distributionInfo = distributionManager.getCacheTopology().getDistribution(key);
      return distributionInfo.isWriteOwner() && !distributionInfo.isReadOwner();
   }

   @Override
   public boolean ownsData() {
      return ownsData;
   }

   @Override
   public void onTopologyUpdate(final CacheTopology cacheTopology, final boolean isRebalance) {
      final boolean isMember = cacheTopology.getMembers().contains(rpcManager.getAddress());
      final boolean startConflictResolution = !isRebalance && cacheTopology.getPhase() == CacheTopology.Phase.CONFLICT_RESOLUTION;
      if (trace) log.tracef("Received new topology for cache %s, isRebalance = %b, isMember = %b, topology = %s", cacheName, isRebalance, isMember, cacheTopology);

      if (!ownsData && isMember) {
         ownsData = true;
      } else if (ownsData && !isMember) {
         // This can happen after a merge, if the local node was in a minority partition.
         ownsData = false;
      }

      // If a member leaves/crashes immediately after a rebalance was started, the new CH_UPDATE
      // command may be executed before the REBALANCE_START command, so it has to start the rebalance.
      boolean startRebalance = isRebalance;
      if (!isRebalance && !startConflictResolution) {
         if (cacheTopology.getPendingCH() != null && this.cacheTopology.getPendingCH() == null) {
            if (trace) log.tracef("Forcing startRebalance = true");
            startRebalance = true;
         }
      }
      if (startRebalance) {
         // Only update the rebalance topology id when starting the rebalance, as we're going to ignore any state
         // response with a smaller topology id
         stateTransferTopologyId.compareAndSet(NO_STATE_TRANSFER_IN_PROGRESS, cacheTopology.getTopologyId());
         conflictManager.cancelVersionRequests();
         cacheNotifier.notifyDataRehashed(cacheTopology.getCurrentCH(), cacheTopology.getPendingCH(),
               cacheTopology.getUnionCH(), cacheTopology.getTopologyId(), true);
      }

      if (startConflictResolution) {
         // This stops state being applied from a prior rebalance and also prevents tracking from being stopped
         stateTransferTopologyId.set(NO_STATE_TRANSFER_IN_PROGRESS);
      }

      awaitTotalOrderTransactions(cacheTopology, startRebalance);

      // Make sure we don't send a REBALANCE_CONFIRM command before we've added all the transfer tasks
      // even if some of the tasks are removed and re-added
      waitingForState.set(false);

      final ConsistentHash newWriteCh = cacheTopology.getWriteConsistentHash();
      final CacheTopology previousCacheTopology = this.cacheTopology;
      final ConsistentHash previousReadCh =
            previousCacheTopology != null ? previousCacheTopology.getCurrentCH() : null;
      final ConsistentHash previousWriteCh =
            previousCacheTopology != null ? previousCacheTopology.getWriteConsistentHash() : null;
      // Ensures writes to the data container use the right consistent hash
      // No need for a try/finally block, since it's just an assignment
      stateTransferLock.acquireExclusiveTopologyLock();
      beforeTopologyInstalled(cacheTopology.getTopologyId(), startRebalance, previousWriteCh, newWriteCh);
      this.cacheTopology = cacheTopology;
      triangleOrderManager.updateCacheTopology(cacheTopology);
      if (distributionManager != null) {
         distributionManager.setCacheTopology(cacheTopology);
         conflictManager.onTopologyUpdate(distributionManager.getCacheTopology());
      }

      // We need to track changes so that user puts during conflict resolution are prioritised over MergePolicy updates
      // Tracking is stopped once the subsequent rebalance completes
      if (startRebalance || startConflictResolution) {
         if (trace) log.tracef("Start keeping track of keys for rebalance");
         commitManager.stopTrack(PUT_FOR_STATE_TRANSFER);
         commitManager.startTrack(PUT_FOR_STATE_TRANSFER);
      }
      stateTransferLock.releaseExclusiveTopologyLock();
      stateTransferLock.notifyTopologyInstalled(cacheTopology.getTopologyId());
      remoteCommandsExecutor.checkForReadyTasks();

      try {
         // fetch transactions and data segments from other owners if this is enabled
         if (!startConflictResolution && (isTransactional || isFetchEnabled)) {
            Set<Integer> addedSegments, removedSegments;
            if (previousWriteCh == null) {
               // If we have any segments assigned in the initial CH, it means we are the first member.
               // If we are not the first member, we can only add segments via rebalance.
               removedSegments = Collections.emptySet();
               addedSegments = Collections.emptySet();

               // TODO Perhaps we should only do this once we are a member, as listener installation should happen only on cache members?
               if (configuration.clustering().cacheMode().isDistributed() || configuration.clustering().cacheMode().isScattered()) {
                  Collection<DistributedCallable> callables = getClusterListeners(cacheTopology);
                  for (DistributedCallable callable : callables) {
                     callable.setEnvironment(cache, null);
                     try {
                        callable.call();
                     } catch (Exception e) {
                        log.clusterListenerInstallationFailure(e);
                     }
                  }
               }

               if (trace) {
                  log.tracef("On cache %s we have: added segments: %s", cacheName, addedSegments);
               }
            } else {
               Set<Integer> previousSegments = getOwnedSegments(previousWriteCh);
               Set<Integer> newSegments = getOwnedSegments(newWriteCh);

               if (newSegments.size() == newWriteCh.getNumSegments()) {
                  // Optimization for replicated caches
                  removedSegments = new SmallIntSet();
               } else {
                  removedSegments = new SmallIntSet(previousSegments);
                  removedSegments.removeAll(newSegments);
               }

               // This is a rebalance, we need to request the segments we own in the new CH.
               addedSegments = new SmallIntSet(newSegments);
               addedSegments.removeAll(previousSegments);

               if (trace) {
                  log.tracef("On cache %s we have: new segments: %s; old segments: %s", cacheName, newSegments, previousSegments);
                  log.tracef("On cache %s we have: added segments: %s; removed segments: %s", cacheName, addedSegments, removedSegments);
               }

               // remove inbound transfers for segments we no longer own
               cancelTransfers(removedSegments);

               // Scattered cache gets added segments on the first CH_UPDATE, and we want to keep these
               if (!startRebalance && !addedSegments.isEmpty() && !configuration.clustering().cacheMode().isScattered()) {
                  // If the last owner of a segment leaves the cluster, a new set of owners is assigned,
                  // but the new owners should not try to retrieve the segment from each other.
                  // If this happens during a rebalance, we might have already sent our rebalance
                  // confirmation, so the coordinator won't wait for us to retrieve those segments anyway.
                  log.debugf("Not requesting segments %s because the last owner left the cluster",
                        addedSegments);
                  addedSegments.clear();
               }

               // check if any of the existing transfers should be restarted from a different source because
               // the initial source is no longer a member
               restartBrokenTransfers(cacheTopology, addedSegments);
            }

            handleSegments(startRebalance, addedSegments, removedSegments);
         }

         int stateTransferTopologyId = this.stateTransferTopologyId.get();
         if (trace) log.tracef("Topology update processed, stateTransferTopologyId = %d, startRebalance = %s, pending CH = %s",
               (Object)stateTransferTopologyId, startRebalance, cacheTopology.getPendingCH());
         if (stateTransferTopologyId != NO_STATE_TRANSFER_IN_PROGRESS && !startRebalance && !cacheTopology.getPhase().isRebalance()) {
            // we have received a topology update without a pending CH, signalling the end of the rebalance
            boolean changed = this.stateTransferTopologyId.compareAndSet(stateTransferTopologyId, NO_STATE_TRANSFER_IN_PROGRESS);
            if (changed) {
               stopApplyingState(stateTransferTopologyId);

               // if the coordinator changed, we might get two concurrent topology updates,
               // but we only want to notify the @DataRehashed listeners once
               ConsistentHash nextConsistentHash = cacheTopology.getPendingCH();
               if (nextConsistentHash == null) {
                  nextConsistentHash = cacheTopology.getCurrentCH();
               }
               cacheNotifier.notifyDataRehashed(previousReadCh, nextConsistentHash, previousWriteCh,
                     cacheTopology.getTopologyId(), false);

               if (trace) {
                  log.tracef("Unlock State Transfer in Progress for topology ID %s", cacheTopology.getTopologyId());
               }
               if (isTotalOrder) {
                  totalOrderManager.notifyStateTransferEnd();
               }
            }
         }
      } finally {
         stateTransferLock.notifyTransactionDataReceived(cacheTopology.getTopologyId());
         remoteCommandsExecutor.checkForReadyTasks();

         // Only set the flag here, after all the transfers have been added to the transfersBySource map
         if (stateTransferTopologyId.get() != NO_STATE_TRANSFER_IN_PROGRESS && isMember) {
            waitingForState.set(true);
         }

         notifyEndOfStateTransferIfNeeded(cacheTopology.getTopologyId(), cacheTopology.getRebalanceId());

         // Remove the transactions whose originators have left the cache.
         // Need to do it now, after we have applied any transactions from other nodes,
         // and after notifyTransactionDataReceived - otherwise the RollbackCommands would block.
         try {
            if (transactionTable != null) {
               transactionTable.cleanupLeaverTransactions(rpcManager.getTransport().getMembers());
            }
         } catch (Exception e) {
            // Do not fail state transfer when the cleanup fails. See ISPN-7437 for details.
            log.transactionCleanupError(e);
         }

         commandAckCollector.onMembersChange(newWriteCh.getMembers());

         // The rebalance (READ_OLD_WRITE_ALL/TRANSITORY) is confirmed through notifyEndOfRebalanceIfNeeded
         // and STABLE does not have to be confirmed at all
         switch (cacheTopology.getPhase()) {
            case READ_ALL_WRITE_ALL:
            case READ_NEW_WRITE_ALL:
               localTopologyManager.confirmRebalancePhase(cacheName, cacheTopology.getTopologyId(), cacheTopology.getRebalanceId(), null);
         }

         PartitionHandlingConfiguration phConfig = configuration.clustering().partitionHandling();
         boolean wasMember = previousWriteCh != null && previousWriteCh.getMembers().contains(rpcManager.getAddress());
         boolean deletePastMemberVals = wasMember && phConfig.whenSplit() != PartitionHandling.ALLOW_READ_WRITES && phConfig.mergePolicy() == null;
         // Any data for segments we do not own should be removed from data container and cache store
         // We need to discard data from all segments we don't own, not just those we previously owned,
         // when we lose membership (e.g. because there was a merge, the local partition was in degraded mode
         // and the other partition was available) or when L1 is enabled.
         // The only exception, is if a merge policy has been enabled, in which case we must only perform the removal
         // when this node is a member of the new topology, otherwise entries updated during conflict resolution can be
         // removed, resulting in only a subset of the owners hosting the resolved entry.
         Set<Integer> removedSegments;
         if ((isMember || deletePastMemberVals) && cacheTopology.getPhase() == CacheTopology.Phase.NO_REBALANCE) {
            removedSegments = IntStream.range(0, newWriteCh.getNumSegments()).boxed().collect(Collectors.toSet());
            Set<Integer> newSegments = getOwnedSegments(newWriteCh);
            removedSegments.removeAll(newSegments);

            try {
               removeStaleData(removedSegments);
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
               throw new CacheException(e);
            }
            conflictManager.restartVersionRequests();
         }
      }
   }

   protected void beforeTopologyInstalled(int topologyId, boolean startRebalance, ConsistentHash previousWriteCh, ConsistentHash newWriteCh) {
   }

   protected void handleSegments(boolean startRebalance, Set<Integer> addedSegments, Set<Integer> removedSegments) {
      if (!addedSegments.isEmpty()) {
         // add transfers for new or restarted segments
         addTransfers(addedSegments);
      }
   }

   private void awaitTotalOrderTransactions(CacheTopology cacheTopology, boolean isRebalance) {
      //in total order, we should wait for remote transactions before proceeding
      if (isTotalOrder) {
         if (trace) {
            log.trace("State Transfer in Total Order cache. Waiting for remote transactions to finish");
         }
         try {
            for (TotalOrderLatch block : totalOrderManager.notifyStateTransferStart(cacheTopology.getTopologyId(), isRebalance)) {
               block.awaitUntilUnBlock();
            }
         } catch (InterruptedException e) {
            //interrupted...
            Thread.currentThread().interrupt();
            throw new CacheException(e);
         }
         if (trace) {
            log.trace(
                  "State Transfer in Total Order cache. All remote transactions are finished. Moving on...");
         }
      }

   }

   protected boolean notifyEndOfStateTransferIfNeeded(int topologyId, int rebalanceId) {
      if (waitingForState.get()) {
         if (hasActiveTransfers()) {
            return false;
         }
         if (waitingForState.compareAndSet(true, false)) {
            log.debugf("Finished receiving of segments for cache %s for topology %d.", cacheName, topologyId);
            stopApplyingState(stateTransferTopologyId.get());
            stateTransferManager.notifyEndOfStateTransfer(topologyId, rebalanceId);
            return true;
         }
         return false;
      }
      return true;
   }

   protected Set<Integer> getOwnedSegments(ConsistentHash consistentHash) {
      Address address = rpcManager.getAddress();
      return consistentHash.getMembers().contains(address) ? consistentHash.getSegmentsForOwner(address)
            : Collections.emptySet();
   }

   @Override
   public void applyState(final Address sender, int topologyId, boolean pushTransfer, Collection<StateChunk> stateChunks) {
      ConsistentHash wCh = cacheTopology.getWriteConsistentHash();
      // Ignore responses received after we are no longer a member
      if (!wCh.getMembers().contains(rpcManager.getAddress())) {
         if (trace) {
            log.tracef("Ignoring received state because we are no longer a member of cache %s", cacheName);
         }
         return;
      }

      // Ignore segments that we requested for a previous rebalance
      // Can happen when the coordinator leaves, and the new coordinator cancels the rebalance in progress
      int rebalanceTopologyId = stateTransferTopologyId.get();
      if (rebalanceTopologyId == NO_STATE_TRANSFER_IN_PROGRESS && !pushTransfer) {
         log.debugf("Discarding state response with topology id %d for cache %s, we don't have a state transfer in progress",
               topologyId, cacheName);
         return;
      }
      if (topologyId < rebalanceTopologyId) {
         log.debugf("Discarding state response with old topology id %d for cache %s, state transfer request topology was %b",
               topologyId, cacheName, waitingForState);
         return;
      }

      if (trace) {
         log.tracef("Before applying the received state the data container of cache %s has %d keys", cacheName,
                    dataContainer.sizeIncludingExpired());
      }
      final CountDownLatch countDownLatch = new CountDownLatch(stateChunks.size());
      if (pushTransfer) {
         // push-transfer is specific for scattered cache but this is the easiest way to integrate it
         for (StateChunk stateChunk : stateChunks) {
            if (stateChunk.getCacheEntries() != null) {
               stateTransferExecutor.submit(() -> {
                  doApplyState(sender, stateChunk.getSegmentId(), stateChunk.getCacheEntries());
                  countDownLatch.countDown();
               });
            }
         }
      } else {
         Set<Integer> mySegments = wCh.getSegmentsForOwner(rpcManager.getAddress());
         for (StateChunk stateChunk : stateChunks) {
            stateTransferExecutor.submit(() -> {
               try {
                  applyChunk(sender, mySegments, stateChunk);
               } catch (Throwable e) {
                  log.error("Failed applying state", e);
               }
               countDownLatch.countDown();
               log.tracef("Latch %d", countDownLatch.getCount());
            });
         }
      }
      try {
         boolean await = countDownLatch.await(timeout, TimeUnit.MILLISECONDS);
         if (!await) {
            throw new TimeoutException("Timed out applying state");
         }
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      }

      if (trace) {
         log.tracef("After applying the received state the data container of cache %s has %d keys", cacheName,
                    dataContainer.sizeIncludingExpired());
         synchronized (transferMapsLock) {
            log.tracef("Segments not received yet for cache %s: %s", cacheName, transfersBySource);
         }
      }
   }

   private void applyChunk(Address sender, Set<Integer> mySegments, StateChunk stateChunk) {
      if (!mySegments.contains(stateChunk.getSegmentId())) {
         log.warnf("Discarding received cache entries for segment %d of cache %s because they do not belong to this node.", stateChunk.getSegmentId(), cacheName);
         return;
      }

      // Notify the inbound task that a chunk of cache entries was received
      InboundTransferTask inboundTransfer = null;
      synchronized (transferMapsLock) {
         List<InboundTransferTask> inboundTransfers = transfersBySegment.get(stateChunk.getSegmentId());
         if (inboundTransfers != null) {
            inboundTransfer = inboundTransfers.stream().filter(task -> task.getSource().equals(sender)).findFirst().orElse(null);
         }
      }
      if (inboundTransfer != null) {
         if (stateChunk.getCacheEntries() != null) {
            doApplyState(sender, stateChunk.getSegmentId(), stateChunk.getCacheEntries());
         }

         inboundTransfer.onStateReceived(stateChunk.getSegmentId(), stateChunk.isLastChunk());
      } else {
         if (cache.getStatus().allowInvocations()) {
            log.ignoringUnsolicitedState(sender, stateChunk.getSegmentId(), cacheName);
         }
      }
   }

   private void doApplyState(Address sender, int segmentId, Collection<InternalCacheEntry> cacheEntries) {
      if (trace) log.tracef("Applying new state chunk for segment %d of cache %s from node %s: received %d cache entries",
            segmentId, cacheName, sender, cacheEntries.size());

      // CACHE_MODE_LOCAL avoids handling by StateTransferInterceptor and any potential locks in StateTransferLock
      boolean transactional = transactionManager != null;
      for (InternalCacheEntry e : cacheEntries) {
         try {
            InvocationContext ctx;
            if (transactional) {
               transactionManager.begin();
               ctx = icf.createInvocationContext(transactionManager.getTransaction(), true);
               ((TxInvocationContext) ctx).getCacheTransaction().setStateTransferFlag(PUT_FOR_STATE_TRANSFER);
            } else {
               // non-tx cache
               ctx = icf.createSingleKeyNonTxInvocationContext();
            }

            PutKeyValueCommand put = commandsFactory.buildPutKeyValueCommand(
                  e.getKey(), e.getValue(), e.getMetadata(), STATE_TRANSFER_FLAGS);
            ctx.setLockOwner(put.getKeyLockOwner());
            interceptorChain.invoke(ctx, put);

            if (transactionManager != null) {
               transactionManager.commit();
            }
         } catch (Exception ex) {
            if (!cache.getStatus().allowInvocations()) {
               log.debugf("Cache %s is shutting down, stopping state transfer", cacheName);
               break;
            } else {
               log.problemApplyingStateForKey(ex.getMessage(), e.getKey(), ex);
            }
         } finally {
            try {
               if (transactional && transactionManager.getTransaction() != null) {
                  transactionManager.rollback();
               }
            } catch (SystemException e1) {
               // Ignore
            }
         }
      }
      if (trace) log.tracef("Finished applying chunk of segment %d of cache %s", segmentId, cacheName);
   }

   private void applyTransactions(Address sender, Collection<TransactionInfo> transactions, int topologyId) {
      log.debugf("Applying %d transactions for cache %s transferred from node %s", transactions.size(), cacheName, sender);
      if (isTransactional) {
         for (TransactionInfo transactionInfo : transactions) {
            GlobalTransaction gtx = transactionInfo.getGlobalTransaction();
            if (rpcManager.getAddress().equals(gtx.getAddress())) {
               continue; // it is a transaction originated in this node. can happen with partition handling
            }
            // Mark the global transaction as remote. Only used for logging, hashCode/equals ignore it.
            gtx.setRemote(true);

            CacheTransaction tx = transactionTable.getLocalTransaction(gtx);
            if (tx == null) {
               tx = transactionTable.getRemoteTransaction(gtx);
               if (tx == null) {
                  try {
                     tx = transactionTable.getOrCreateRemoteTransaction(gtx, transactionInfo.getModifications());
                     // Force this node to replay the given transaction data by making it think it is 1 behind
                     ((RemoteTransaction) tx).setLookedUpEntriesTopology(topologyId - 1);
                  } catch (Throwable t) {
                     if (trace)
                        log.tracef(t, "Failed to create remote transaction %s", gtx);
                  }
               }
            }
            if (tx != null) {
               transactionInfo.getLockedKeys().forEach(tx::addBackupLockForKey);
            }
         }
      }
   }

   // Must run after the PersistenceManager
   @Start(priority = 20)
   public void start() {
      CacheMode mode = configuration.clustering().cacheMode();
      isFetchEnabled = mode.needsStateTransfer() &&
              (configuration.clustering().stateTransfer().fetchInMemoryState() || configuration.persistence().fetchPersistentState());
      //rpc options does not changes in runtime. we can use always the same instance.
      synchronousRpcOptions = rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS)
            .timeout(timeout, TimeUnit.MILLISECONDS).build();
      synchronousIgnoreLeaversRpcOptions = rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS)
            .timeout(timeout, TimeUnit.MILLISECONDS).build();

      stateRequestExecutor = new LimitedExecutor("StateRequest-" + cacheName, stateTransferExecutor, 1);
   }

   @Stop(priority = 0)
   @Override
   public void stop() {
      if (trace) {
         log.tracef("Shutting down StateConsumer of cache %s on node %s", cacheName, rpcManager.getAddress());
      }

      try {
         synchronized (transferMapsLock) {
            // cancel all inbound transfers
            stateRequestExecutor.cancelQueuedTasks();

            // make a copy, cancel might remove the transfers
            Collection<List<InboundTransferTask>> transfers = new ArrayList<>(transfersBySource.values());
            for (List<InboundTransferTask> inboundTransfers : transfers) {
               inboundTransfers.forEach(InboundTransferTask::cancel);
            }
            transfersBySource.clear();
            transfersBySegment.clear();
         }
      } catch (Throwable t) {
         log.errorf(t, "Failed to stop StateConsumer of cache %s on node %s", cacheName, rpcManager.getAddress());
      }
   }

   @Override
   public CacheTopology getCacheTopology() {
      return cacheTopology;
   }

   public void setKeyInvalidationListener(KeyInvalidationListener keyInvalidationListener) {
      this.keyInvalidationListener = keyInvalidationListener;
   }

   // not used in scattered cache
   private void addTransfers(Set<Integer> segments) {
      log.debugf("Adding inbound state transfer for segments %s", segments);

      // the set of nodes that reported errors when fetching data from them - these will not be retried in this topology
      Set<Address> excludedSources = new HashSet<>();

      // the sources and segments we are going to get from each source
      Map<Address, Set<Integer>> sources = new HashMap<>();

      if (isTransactional && !isTotalOrder) {
         requestTransactions(segments, sources, excludedSources);
      }

      if (isFetchEnabled) {
         requestSegments(segments, sources, excludedSources);
      }

      if (trace) log.tracef("Finished adding inbound state transfer for segments %s", segments,
                            cacheName);
   }

   private void findSources(Set<Integer> segments, Map<Address, Set<Integer>> sources, Set<Address> excludedSources) {
      if (cache.getStatus().isTerminated())
         return;

      SmallIntSet segmentsWithoutSource = new SmallIntSet(configuration.clustering().hash().numSegments());
      for (Integer segmentId : segments) {
         Address source = findSource(segmentId, excludedSources);
         // ignore all segments for which there are no other owners to pull data from.
         // these segments are considered empty (or lost) and do not require a state transfer
         if (source != null) {
            Set<Integer> segmentsFromSource = sources.computeIfAbsent(source, k -> new SmallIntSet());
            segmentsFromSource.add(segmentId);
         } else {
            segmentsWithoutSource.set(segmentId);
         }
      }
      if (!segmentsWithoutSource.isEmpty()) {
         log.noLiveOwnersFoundForSegments(segmentsWithoutSource, cacheName, excludedSources);
      }
   }

   private Address findSource(int segmentId, Set<Address> excludedSources) {
      List<Address> owners = cacheTopology.getReadConsistentHash().locateOwnersForSegment(segmentId);
      if (!owners.contains(rpcManager.getAddress())) {
         // We prefer that transactions are sourced from primary owners.
         // Needed in pessimistic mode, if the originator is the primary owner of the key than the lock
         // command is not replicated to the backup owners. See PessimisticDistributionInterceptor.acquireRemoteIfNeeded.
         for (Address o : owners) {
            if (!o.equals(rpcManager.getAddress()) && !excludedSources.contains(o)) {
               return o;
            }
         }
      }
      return null;
   }

   private void requestTransactions(Set<Integer> segments, Map<Address, Set<Integer>> sources, Set<Address> excludedSources) {
      findSources(segments, sources, excludedSources);

      boolean seenFailures = false;
      while (true) {
         SmallIntSet failedSegments = new SmallIntSet();
         int topologyId = cacheTopology.getTopologyId();
         for (Map.Entry<Address, Set<Integer>> sourceEntry : sources.entrySet()) {
            Address source = sourceEntry.getKey();
            Set<Integer> segmentsFromSource = sourceEntry.getValue();
            boolean failed = false;
            boolean exclude = false;
            try {
               Response response = getTransactions(source, segmentsFromSource, topologyId);
               if (response instanceof SuccessfulResponse) {
                  List<TransactionInfo> transactions = (List<TransactionInfo>) ((SuccessfulResponse) response).getResponseValue();
                  applyTransactions(source, transactions, topologyId);
               } else if (response instanceof CacheNotFoundResponse) {
                  log.debugf("Cache %s was stopped on node %s before sending transaction information", cacheName, source);
                  failed = true;
                  exclude = true;
               } else {
                  log.unsuccessfulResponseRetrievingTransactionsForSegments(source, response);
                  failed = true;
               }
            } catch (SuspectException e) {
               log.debugf("Node %s left the cluster before sending transaction information", source);
               failed = true;
               exclude = true;
            } catch (Exception e) {
               if (cache.getStatus().isTerminated()) {
                  log.debugf("Cache %s has stopped while requesting transactions", cacheName);
                  sources.clear();
                  return;
               } else {
                  log.failedToRetrieveTransactionsForSegments(cacheName, source, segments, e);
               }
               // The primary owner is still in the cluster, so we can't exclude it - see ISPN-4091
               failed = true;
            }

            // If requesting the transactions failed we need to retry
            if (failed) {
               failedSegments.addAll(segmentsFromSource);
            }
            // If the primary owner is no longer running, we can retry on a backup owner
            if (exclude) {
               excludedSources.add(source);
            }
         }

         if (failedSegments.isEmpty()) {
            break;
         }

         // look for other sources for all failed segments
         seenFailures = true;
         sources.clear();
         findSources(failedSegments, sources, excludedSources);
      }

      if (seenFailures) {
         // start fresh when next step starts (fetching segments)
         sources.clear();
      }
   }

   private Collection<DistributedCallable> getClusterListeners(CacheTopology topology) {
      for (Address source : topology.getMembers()) {
         // Don't send to ourselves
         if (!source.equals(rpcManager.getAddress())) {
            if (trace) {
               log.tracef("Requesting cluster listeners of cache %s from node %s", cacheName, source);
            }
            // get cluster listeners
            try {
               StateRequestCommand cmd = commandsFactory.buildStateRequestCommand(StateRequestCommand.Type.GET_CACHE_LISTENERS,
                                                                                  rpcManager.getAddress(), topology.getTopologyId(), null);
               Map<Address, Response> responses = rpcManager.invokeRemotely(Collections.singleton(source), cmd, synchronousIgnoreLeaversRpcOptions);
               Response response = responses.get(source);
               if (response instanceof SuccessfulResponse) {
                  return (Collection<DistributedCallable>) ((SuccessfulResponse) response).getResponseValue();
               } else {
                  log.unsuccessfulResponseForClusterListeners(source, response);
               }
            } catch (CacheException e) {
               log.exceptionDuringClusterListenerRetrieval(source, e);
            }
         }
      }
      if (trace) log.trace("Unable to acquire cluster listeners from other members, assuming none are present");
      return Collections.emptySet();
   }

   private Response getTransactions(Address source, Set<Integer> segments, int topologyId) {
      if (trace) {
         log.tracef("Requesting transactions from node %s for segments %s", source, segments);
      }
      // get transactions and locks
      StateRequestCommand cmd = commandsFactory.buildStateRequestCommand(StateRequestCommand.Type.GET_TRANSACTIONS, rpcManager.getAddress(), topologyId, segments);
      Map<Address, Response> responses = rpcManager.invokeRemotely(Collections.singleton(source), cmd, synchronousRpcOptions);
      return responses.get(source);
   }

   // not used in scattered cache
   private void requestSegments(Set<Integer> segments, Map<Address, Set<Integer>> sources, Set<Address> excludedSources) {
      if (sources.isEmpty()) {
         findSources(segments, sources, excludedSources);
      }

      for (Map.Entry<Address, Set<Integer>> e : sources.entrySet()) {
         addTransfer(e.getKey(), e.getValue());
      }
   }

   /**
    * Cancel transfers for segments we no longer own.
    *
    * @param removedSegments segments to be cancelled
    */
   private void cancelTransfers(Set<Integer> removedSegments) {
      synchronized (transferMapsLock) {
         List<Integer> segmentsToCancel = new ArrayList<>(removedSegments);
         while (!segmentsToCancel.isEmpty()) {
            int segmentId = segmentsToCancel.remove(0);
            List<InboundTransferTask> inboundTransfers = transfersBySegment.get(segmentId);
            if (inboundTransfers != null) { // we need to check the transfer was not already completed
               for (InboundTransferTask inboundTransfer : inboundTransfers) {
                  Set<Integer> cancelledSegments = new SmallIntSet(removedSegments);
                  cancelledSegments.retainAll(inboundTransfer.getSegments());
                  segmentsToCancel.removeAll(cancelledSegments);
                  transfersBySegment.keySet().removeAll(cancelledSegments);
                  //this will also remove it from transfersBySource if the entire task gets cancelled
                  inboundTransfer.cancelSegments(cancelledSegments);
                  if (inboundTransfer.isCancelled()) {
                     removeTransfer(inboundTransfer);
                  }
               }
            }
         }
      }
   }

   protected void removeStaleData(final Set<Integer> removedSegments) throws InterruptedException {
      log.debugf("Removing no longer owned entries for cache %s", cacheName);
      if (keyInvalidationListener != null) {
         keyInvalidationListener.beforeInvalidation(removedSegments, Collections.emptySet());
      }

      if (removedSegments.isEmpty())
         return;

      // Keys that we used to own, and need to be removed from the data container AND the cache stores
      final ConcurrentHashSet<Object> keysToRemove = new ConcurrentHashSet<>();

      dataContainer.executeTask(KeyFilter.ACCEPT_ALL_FILTER, (o, ice) -> {
         Object key = ice.getKey();
         int keySegment = getSegment(key);
         if (removedSegments.contains(keySegment)) {
            keysToRemove.add(key);
         }
      });

      // gather all keys from cache store that belong to the segments that are being removed/moved to L1
      if (!removedSegments.isEmpty()) {
         try {
            KeyFilter filter = key -> {
               if (dataContainer.containsKey(key))
                  return false;
               int keySegment = getSegment(key);
               return (removedSegments.contains(keySegment));
            };
            persistenceManager.processOnAllStores(filter,
                  (marshalledEntry, taskContext) -> keysToRemove.add(marshalledEntry.getKey()), false, false, PRIVATE);
         } catch (CacheException e) {
            log.failedLoadingKeysFromCacheStore(e);
         }
      }

      if (!keysToRemove.isEmpty()) {
         try {
            InvalidateCommand invalidateCmd = commandsFactory.buildInvalidateCommand(EnumUtil.bitSetOf(CACHE_MODE_LOCAL, SKIP_LOCKING), keysToRemove.toArray());
            InvocationContext ctx = icf.createNonTxInvocationContext();
            ctx.setLockOwner(invalidateCmd.getKeyLockOwner());
            interceptorChain.invoke(ctx, invalidateCmd);

            if (trace) log.tracef("Removed %d keys, data container now has %d keys", keysToRemove.size(), dataContainer.sizeIncludingExpired());
         } catch (CacheException e) {
            log.failedToInvalidateKeys(e);
         }
      }
   }

   /**
    * Check if any of the existing transfers should be restarted from a different source because the initial source is no longer a member.
    */
   private void restartBrokenTransfers(CacheTopology cacheTopology, Set<Integer> addedSegments) {
      Set<Address> members = new HashSet<>(cacheTopology.getReadConsistentHash().getMembers());
      synchronized (transferMapsLock) {
         for (Iterator<Map.Entry<Address, List<InboundTransferTask>>> it = transfersBySource.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Address, List<InboundTransferTask>> entry = it.next();
            Address source = entry.getKey();
            if (!members.contains(source)) {
               if (trace) {
                  log.tracef("Removing inbound transfers from source %s for cache %s", source, cacheName);
               }
               List<InboundTransferTask> inboundTransfers = entry.getValue();
               it.remove();
               for (InboundTransferTask inboundTransfer : inboundTransfers) {
                  // these segments will be restarted if they are still in new write CH
                  if (trace) {
                     log.tracef("Removing inbound transfers from node %s for segments %s", source, inboundTransfer.getSegments());
                  }
                  inboundTransfer.cancel();
                  transfersBySegment.keySet().removeAll(inboundTransfer.getSegments());
                  addedSegments.addAll(inboundTransfer.getUnfinishedSegments());
               }
            }
         }

         // exclude those that are already in progress from a valid source
         addedSegments.removeAll(transfersBySegment.keySet());
      }
   }

   private int getSegment(Object key) {
      // here we can use any CH version because the routing table is not involved in computing the segment
      return keyPartitioner.getSegment(key);
   }

   // not used in scattered cache
   private InboundTransferTask addTransfer(Address source, Set<Integer> segmentsFromSource) {
      final InboundTransferTask inboundTransfer;

      synchronized (transferMapsLock) {
         if (trace) {
            log.tracef("Adding transfer from %s for segments %s", source, segmentsFromSource);
         }
         segmentsFromSource.removeAll(transfersBySegment.keySet());  // already in progress segments are excluded
         if (segmentsFromSource.isEmpty()) {
            if (trace) {
               log.tracef("All segments are already in progress, skipping");
            }
            return null;
         }

         inboundTransfer = new InboundTransferTask(segmentsFromSource, source, cacheTopology.getTopologyId(),
               rpcManager, commandsFactory, timeout, cacheName, true);
         addTransfer(inboundTransfer, segmentsFromSource);
      }

      stateRequestExecutor.executeAsync(() -> {
         CompletableFuture<Void> transferStarted = inboundTransfer.requestSegments();

         if (trace)
            log.tracef("Waiting for inbound transfer to finish: %s", inboundTransfer);
         return transferStarted.whenComplete((aVoid, throwable) -> onTaskCompletion(inboundTransfer));
      });
      return inboundTransfer;
   }

   @GuardedBy("transferMapsLock")
   protected void addTransfer(InboundTransferTask inboundTransfer, Set<Integer> segments) {
      for (int segmentId : segments) {
         transfersBySegment.computeIfAbsent(segmentId, s -> new ArrayList<>()).add(inboundTransfer);
      }
      transfersBySource.computeIfAbsent(inboundTransfer.getSource(), s -> new ArrayList<>()).add(inboundTransfer);
   }

   protected boolean removeTransfer(InboundTransferTask inboundTransfer) {
      boolean found = false;
      synchronized (transferMapsLock) {
         if (trace) log.tracef("Removing inbound transfers from node %s for segments %s",
               inboundTransfer.getSegments(), inboundTransfer.getSource(), cacheName);
         List<InboundTransferTask> transfers = transfersBySource.get(inboundTransfer.getSource());
         if (transfers != null && (found = transfers.remove(inboundTransfer)) && transfers.isEmpty()) {
            transfersBySource.remove(inboundTransfer.getSource());
         }
         for (int segment : inboundTransfer.getSegments()) {
            transfers = transfersBySegment.get(segment);
            if (transfers != null && transfers.remove(inboundTransfer) && transfers.isEmpty()) {
               transfersBySegment.remove(segment);
            }
         }
      }
      return found;
   }

   protected void onTaskCompletion(final InboundTransferTask inboundTransfer) {
      if (trace) log.tracef("Inbound transfer finished: %s", inboundTransfer);
      if (inboundTransfer.isCompletedSuccessfully()) {
         removeTransfer(inboundTransfer);
         notifyEndOfStateTransferIfNeeded(cacheTopology.getTopologyId(), cacheTopology.getRebalanceId());
      }
   }

   public interface KeyInvalidationListener {
      void beforeInvalidation(Set<Integer> removedSegments, Set<Integer> staleL1Segments);
   }
}
