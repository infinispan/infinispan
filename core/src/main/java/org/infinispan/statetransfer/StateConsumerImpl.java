package org.infinispan.statetransfer;

import static org.infinispan.context.Flag.CACHE_MODE_LOCAL;
import static org.infinispan.context.Flag.IGNORE_RETURN_VALUES;
import static org.infinispan.context.Flag.IRAC_STATE;
import static org.infinispan.context.Flag.PUT_FOR_STATE_TRANSFER;
import static org.infinispan.context.Flag.SKIP_LOCKING;
import static org.infinispan.context.Flag.SKIP_OWNERSHIP_CHECK;
import static org.infinispan.context.Flag.SKIP_REMOTE_LOOKUP;
import static org.infinispan.context.Flag.SKIP_SHARED_CACHE_STORE;
import static org.infinispan.context.Flag.SKIP_XSITE_BACKUP;
import static org.infinispan.factories.KnownComponentNames.NON_BLOCKING_EXECUTOR;
import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.PRIVATE;
import static org.infinispan.commons.util.concurrent.CompletionStages.handleAndCompose;
import static org.infinispan.commons.util.concurrent.CompletionStages.ignoreValue;
import static org.infinispan.util.logging.Log.PERSISTENCE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.conflict.impl.InternalConflictManager;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.executors.LimitedExecutor;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.metadata.impl.InternalMetadataImpl;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.DataRehashed;
import org.infinispan.notifications.cachelistener.cluster.ClusterListenerReplicateCallable;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.reactive.publisher.impl.LocalPublisherManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.PassthroughSingleResponseCollector;
import org.infinispan.remoting.transport.impl.SingleResponseCollector;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.impl.FakeJTATransaction;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.commons.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.statetransfer.XSiteStateTransferManager;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;
import net.jcip.annotations.GuardedBy;

/**
 * {@link StateConsumer} implementation.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@Scope(Scopes.NAMED_CACHE)
public class StateConsumerImpl implements StateConsumer {
   private static final Log log = LogFactory.getLog(StateConsumerImpl.class);
   protected static final int NO_STATE_TRANSFER_IN_PROGRESS = -1;
   protected static final long STATE_TRANSFER_FLAGS = EnumUtil.bitSetOf(PUT_FOR_STATE_TRANSFER, CACHE_MODE_LOCAL,
                                                                        IGNORE_RETURN_VALUES, SKIP_REMOTE_LOOKUP,
                                                                        SKIP_SHARED_CACHE_STORE, SKIP_OWNERSHIP_CHECK,
                                                                        SKIP_XSITE_BACKUP, SKIP_LOCKING, IRAC_STATE);
   protected static final long INVALIDATE_FLAGS = STATE_TRANSFER_FLAGS & ~FlagBitSets.PUT_FOR_STATE_TRANSFER;
   public static final String NO_KEY = "N/A";

   @Inject protected ComponentRef<Cache<Object, Object>> cache;
   @Inject protected Configuration configuration;
   @Inject protected RpcManager rpcManager;
   @Inject protected TransactionManager transactionManager;   // optional
   @Inject protected CommandsFactory commandsFactory;
   @Inject protected TransactionTable transactionTable;       // optional
   @Inject protected InternalDataContainer<Object, Object> dataContainer;
   @Inject protected PersistenceManager persistenceManager;
   @Inject protected AsyncInterceptorChain interceptorChain;
   @Inject protected InvocationContextFactory icf;
   @Inject protected StateTransferLock stateTransferLock;
   @Inject protected CacheNotifier<?, ?> cacheNotifier;
   @Inject protected CommitManager commitManager;
   @Inject @ComponentName(NON_BLOCKING_EXECUTOR)
   protected Executor nonBlockingExecutor;
   @Inject protected CommandAckCollector commandAckCollector;
   @Inject protected DistributionManager distributionManager;
   @Inject protected KeyPartitioner keyPartitioner;
   @Inject protected InternalConflictManager<?, ?> conflictManager;
   @Inject protected LocalPublisherManager<Object, Object> localPublisherManager;
   @Inject PerCacheInboundInvocationHandler inboundInvocationHandler;
   @Inject XSiteStateTransferManager xSiteStateTransferManager;

   protected String cacheName;
   protected long timeout;
   protected boolean isFetchEnabled;
   protected boolean isTransactional;
   protected boolean isInvalidationMode;
   protected volatile KeyInvalidationListener keyInvalidationListener; //for test purpose only!

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
   protected CompletableFuture<Void> stateTransferFuture = CompletableFutures.completedNull();

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
    * A set identifying the transactional segments requested by the cache. This is a set so a segment is counted only
    * once.
    */
   private IntSet requestedTransactionalSegments;

   /**
    * Limit to one state request at a time.
    */
   protected LimitedExecutor stateRequestExecutor;

   private volatile boolean ownsData = false;

   // Use the state transfer timeout for RPCs instead of the regular remote timeout
   protected RpcOptions rpcOptions;
   private volatile boolean running;
   private int numSegments;

   public StateConsumerImpl() {
   }

   /**
    * Stops applying incoming state. Also stops tracking updated keys. Should be called at the end of state transfer or
    * when a ClearCommand is committed during state transfer.
    */
   @Override
   public void stopApplyingState(int topologyId) {
      if (log.isTraceEnabled()) log.tracef("Stop keeping track of changed keys for state transfer in topology %d", topologyId);
      commitManager.stopTrack(PUT_FOR_STATE_TRANSFER);
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
   public long inflightRequestCount() {
      synchronized(transferMapsLock) {
         return transfersBySegment.size();
      }
   }

   @Override
   public long inflightTransactionSegmentCount() {
      return requestedTransactionalSegments.size();
   }

   @Override
   public boolean ownsData() {
      return ownsData;
   }

   @Override
   public CompletionStage<CompletionStage<Void>> onTopologyUpdate(CacheTopology cacheTopology, boolean isRebalance) {
      final ConsistentHash newWriteCh = cacheTopology.getWriteConsistentHash();
      final CacheTopology previousCacheTopology = this.cacheTopology;
      final ConsistentHash previousWriteCh =
            previousCacheTopology != null ? previousCacheTopology.getWriteConsistentHash() : null;
      IntSet newWriteSegments = getOwnedSegments(newWriteCh);

      Address address = rpcManager.getAddress();
      final boolean isMember = cacheTopology.getMembers().contains(address);
      final boolean wasMember = previousWriteCh != null &&
                                previousWriteCh.getMembers().contains(address);

      if (log.isTraceEnabled())
         log.tracef("Received new topology for cache %s, isRebalance = %b, isMember = %b, topology = %s", cacheName,
                    isRebalance, isMember, cacheTopology);

      if (!ownsData && isMember) {
         ownsData = true;
      } else if (ownsData && !isMember) {
         // This can happen after a merge, if the local node was in a minority partition.
         ownsData = false;
      }

      // If a member leaves/crashes immediately after a rebalance was started, the new CH_UPDATE
      // command may be executed before the REBALANCE_START command, so it has to start the rebalance.
      boolean addedPendingCH = cacheTopology.getPendingCH() != null && wasMember &&
                               previousCacheTopology.getPendingCH() == null;
      boolean startConflictResolution =
            !isRebalance && cacheTopology.getPhase() == CacheTopology.Phase.CONFLICT_RESOLUTION;
      boolean startStateTransfer = isRebalance || (addedPendingCH && !startConflictResolution);
      if (startStateTransfer && !isRebalance) {
         if (log.isTraceEnabled()) log.tracef("Forcing startRebalance = true");
      }
      CompletionStage<Void> stage = CompletableFutures.completedNull();
      if (startStateTransfer) {
         // Only update the rebalance topology id when starting the rebalance, as we're going to ignore any state
         // response with a smaller topology id
         stateTransferTopologyId.compareAndSet(NO_STATE_TRANSFER_IN_PROGRESS, cacheTopology.getTopologyId());
         conflictManager.cancelVersionRequests();
         if (cacheNotifier.hasListener(DataRehashed.class)) {
            stage = cacheNotifier.notifyDataRehashed(cacheTopology.getCurrentCH(), cacheTopology.getPendingCH(),
                                                    cacheTopology.getUnionCH(), cacheTopology.getTopologyId(), true);
         }
      }
      stage = stage.thenCompose(ignored -> {
         if (startConflictResolution) {
            // This stops state being applied from a prior rebalance and also prevents tracking from being stopped
            stateTransferTopologyId.set(NO_STATE_TRANSFER_IN_PROGRESS);
         }

         // Make sure we don't send a REBALANCE_CONFIRM command before we've added all the transfer tasks
         // even if some of the tasks are removed and re-added
         waitingForState.set(false);
         stateTransferFuture = new CompletableFuture<>();

         if (!configuration.clustering().cacheMode().isInvalidation()) {
            // Owned segments
            dataContainer.addSegments(newWriteSegments);
            // TODO Should we throw an exception if addSegments() returns false?
            return ignoreValue(persistenceManager.addSegments(newWriteSegments));
         }
         return CompletableFutures.completedNull();
      });
      stage = stage.thenCompose(ignored -> {
         // We need to track changes so that user puts during conflict resolution are prioritised over
         // state transfer or conflict resolution updates
         // Tracking is stopped once the state transfer completes (i.e. all the entries have been inserted)
         if (startStateTransfer || startConflictResolution) {
            if (commitManager.isTracking(PUT_FOR_STATE_TRANSFER)) {
               log.debug("Starting state transfer but key tracking is already enabled");
            } else {
               if (log.isTraceEnabled()) log.tracef("Start keeping track of keys for state transfer");
               commitManager.startTrack(PUT_FOR_STATE_TRANSFER);
            }
         }

         // Ensures writes to the data container use the right consistent hash
         // Writers block on the state transfer shared lock, so we keep the exclusive lock as short as possible
         stateTransferLock.acquireExclusiveTopologyLock();
         try {
            this.cacheTopology = cacheTopology;
            distributionManager.setCacheTopology(cacheTopology);
         } finally {
            stateTransferLock.releaseExclusiveTopologyLock();
         }

         stateTransferLock.notifyTopologyInstalled(cacheTopology.getTopologyId());
         inboundInvocationHandler.checkForReadyTasks();
         xSiteStateTransferManager.onTopologyUpdated(cacheTopology, isStateTransferInProgress());

         if (!wasMember && isMember) {
            return fetchClusterListeners(cacheTopology);
         }

         return CompletableFutures.completedNull();
      });
      stage = stage.thenCompose(ignored -> {
         // fetch transactions and data segments from other owners if this is enabled
         if (startConflictResolution || (!isTransactional && !isFetchEnabled)) {
            return CompletableFutures.completedNull();
         }
         IntSet addedSegments, removedSegments;
         if (previousWriteCh == null) {
            // If we have any segments assigned in the initial CH, it means we are the first member.
            // If we are not the first member, we can only add segments via rebalance.
            removedSegments = IntSets.immutableEmptySet();
            addedSegments = IntSets.immutableEmptySet();

            if (log.isTraceEnabled()) {
               log.tracef("On cache %s we have: added segments: %s", cacheName, addedSegments);
            }
         } else {
            IntSet previousSegments = getOwnedSegments(previousWriteCh);

            if (newWriteSegments.size() == numSegments) {
               // Optimization for replicated caches
               removedSegments = IntSets.immutableEmptySet();
            } else {
               removedSegments = IntSets.mutableCopyFrom(previousSegments);
               removedSegments.removeAll(newWriteSegments);
            }

            // This is a rebalance, we need to request the segments we own in the new CH.
            addedSegments = IntSets.mutableCopyFrom(newWriteSegments);
            addedSegments.removeAll(previousSegments);

            if (log.isTraceEnabled()) {
               log.tracef("On cache %s we have: new segments: %s; old segments: %s", cacheName, newWriteSegments,
                          previousSegments);
               log.tracef("On cache %s we have: added segments: %s; removed segments: %s", cacheName,
                          addedSegments, removedSegments);
            }

            // remove inbound transfers for segments we no longer own
            cancelTransfers(removedSegments);

            if (!startStateTransfer && !addedSegments.isEmpty()) {
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

         IntSet transactionOnlySegments = computeTransactionOnlySegments(cacheTopology, address);
         return handleSegments(addedSegments, transactionOnlySegments);
      });
      stage = stage.thenCompose(ignored -> {
         int stateTransferTopologyId = this.stateTransferTopologyId.get();
         if (log.isTraceEnabled())
            log.tracef("Topology update processed, stateTransferTopologyId = %d, startRebalance = %s, pending CH = %s",
                       (Object) stateTransferTopologyId, startStateTransfer, cacheTopology.getPendingCH());
         if (stateTransferTopologyId != NO_STATE_TRANSFER_IN_PROGRESS && !startStateTransfer &&
             !cacheTopology.getPhase().isRebalance()) {
            // we have received a topology update without a pending CH, signalling the end of the rebalance
            boolean changed = this.stateTransferTopologyId.compareAndSet(stateTransferTopologyId,
                                                                         NO_STATE_TRANSFER_IN_PROGRESS);
            // if the coordinator changed, we might get two concurrent topology updates,
            // but we only want to notify the @DataRehashed listeners once
            if (changed) {
               stopApplyingState(stateTransferTopologyId);

               if (cacheNotifier.hasListener(DataRehashed.class)) {
                  return cacheNotifier.notifyDataRehashed(previousCacheTopology.getCurrentCH(),
                                                          previousCacheTopology.getPendingCH(),
                                                          previousCacheTopology.getUnionCH(),
                                                          cacheTopology.getTopologyId(), false);
               }
            }
         }
         return CompletableFutures.completedNull();
      });
      return handleAndCompose(stage, (ignored, throwable) -> {
         if (log.isTraceEnabled()) {
            log.tracef("Unlock State Transfer in Progress for topology ID %s", cacheTopology.getTopologyId());
         }
         stateTransferLock.notifyTransactionDataReceived(cacheTopology.getTopologyId());
         inboundInvocationHandler.checkForReadyTasks();

         // Only set the flag here, after all the transfers have been added to the transfersBySource map
         if (stateTransferTopologyId.get() != NO_STATE_TRANSFER_IN_PROGRESS && isMember) {
            waitingForState.set(true);
         }

         notifyEndOfStateTransferIfNeeded();

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

         // The rebalance (READ_OLD_WRITE_ALL/TRANSITORY) is completed through notifyEndOfStateTransferIfNeeded
         // and STABLE does not have to be confirmed at all
         switch (cacheTopology.getPhase()) {
            case READ_ALL_WRITE_ALL:
            case READ_NEW_WRITE_ALL:
               stateTransferFuture.complete(null);
         }

         // Any data for segments we do not own should be removed from data container and cache store
         // We need to discard data from all segments we don't own, not just those we previously owned,
         // when we lose membership (e.g. because there was a merge, the local partition was in degraded mode
         // and the other partition was available) or when L1 is enabled.
         if ((isMember || wasMember) && cacheTopology.getPhase() == CacheTopology.Phase.NO_REBALANCE) {
            int numSegments = newWriteCh.getNumSegments();
            IntSet removedSegments = IntSets.mutableEmptySet(numSegments);
            IntSet newSegments = getOwnedSegments(newWriteCh);
            for (int i = 0; i < numSegments; ++i) {
               if (!newSegments.contains(i)) {
                  removedSegments.set(i);
               }
            }

            return removeStaleData(removedSegments)
                  .thenApply(ignored1 -> {
                     conflictManager.restartVersionRequests();

                     // rethrow the original exception, if any
                     CompletableFutures.rethrowExceptionIfPresent(throwable);
                     return stateTransferFuture;
                  });
         }

         CompletableFutures.rethrowExceptionIfPresent(throwable);
         return CompletableFuture.completedFuture(stateTransferFuture);
      });
   }

   private IntSet computeTransactionOnlySegments(CacheTopology cacheTopology, Address address) {
      if (configuration.transaction().transactionMode() != TransactionMode.TRANSACTIONAL ||
          configuration.transaction().lockingMode() != LockingMode.PESSIMISTIC ||
          cacheTopology.getPhase() != CacheTopology.Phase.READ_OLD_WRITE_ALL ||
          !cacheTopology.getCurrentCH().getMembers().contains(address)) {
         return IntSets.immutableEmptySet();
      }

      // In pessimistic caches, the originator does not send the lock command to backups
      // if it is the primary owner for all the keys.
      // The idea is that the locks can only be lost completely if the originator crashes (rolling back the tx)
      // But this means when the owners of a segment change from AB -> BA or AB -> BC,
      // B needs to request the transactions affecting that segment from A,
      // even though B already has the entries of that segment
      IntSet transactionOnlySegments = IntSets.mutableEmptySet(numSegments);
      Set<Integer> pendingPrimarySegments = cacheTopology.getPendingCH().getPrimarySegmentsForOwner(address);
      for (Integer segment : pendingPrimarySegments) {
         List<Address> currentOwners = cacheTopology.getCurrentCH().locateOwnersForSegment(segment);
         if (currentOwners.get(0).equals(address)) {
            // Already primary
            continue;
         }
         if (!currentOwners.contains(address)) {
            // Not a backup, will receive transactions the normal way
            continue;
         }
         transactionOnlySegments.add(segment);
      }
      return transactionOnlySegments;
   }

   private CompletionStage<Void> fetchClusterListeners(CacheTopology cacheTopology) {
      if (!configuration.clustering().cacheMode().isDistributed()) {
         return CompletableFutures.completedNull();
      }

      return getClusterListeners(cacheTopology.getTopologyId(), cacheTopology.getReadConsistentHash().getMembers())
            .thenAccept(callables -> {
         Cache<Object, Object> cache = this.cache.wired();
         for (ClusterListenerReplicateCallable<Object, Object> callable : callables) {
            try {
               // TODO: need security check?
               // We have to invoke a separate method as we can't retrieve the cache as it is still starting
               callable.accept(cache.getCacheManager(), cache);
            } catch (Exception e) {
               log.clusterListenerInstallationFailure(e);
            }
         }
      });
   }

   protected void notifyEndOfStateTransferIfNeeded() {
      if (waitingForState.get()) {
         if (hasActiveTransfers()) {
            if (log.isTraceEnabled())
               log.tracef("No end of state transfer notification, active transfers still exist");
            return;
         }
         if (waitingForState.compareAndSet(true, false)) {
            int topologyId = stateTransferTopologyId.get();
            log.debugf("Finished receiving of segments for cache %s for topology %d.", cacheName, topologyId);
            stopApplyingState(topologyId);
            stateTransferFuture.complete(null);
         }
         if (log.isTraceEnabled())
            log.tracef("No end of state transfer notification, waitingForState already set to false by another thread");
         return;
      }
      if (log.isTraceEnabled())
         log.tracef("No end of state transfer notification, waitingForState already set to false by another thread");
   }

   protected IntSet getOwnedSegments(ConsistentHash consistentHash) {
      Address address = rpcManager.getAddress();
      return IntSets.from(consistentHash.getSegmentsForOwner(address));
   }

   @Override
   public CompletionStage<?> applyState(final Address sender, int topologyId, Collection<StateChunk> stateChunks) {
      ConsistentHash wCh = cacheTopology.getWriteConsistentHash();
      // Ignore responses received after we are no longer a member
      if (!wCh.getMembers().contains(rpcManager.getAddress())) {
         if (log.isTraceEnabled()) {
            log.tracef("Ignoring received state because we are no longer a member of cache %s", cacheName);
         }
         return CompletableFutures.completedNull();
      }

      // Ignore segments that we requested for a previous rebalance
      // Can happen when the coordinator leaves, and the new coordinator cancels the rebalance in progress
      int rebalanceTopologyId = stateTransferTopologyId.get();
      if (rebalanceTopologyId == NO_STATE_TRANSFER_IN_PROGRESS) {
         log.debugf("Discarding state response with topology id %d for cache %s, we don't have a state transfer in progress",
               topologyId, cacheName);
         return CompletableFutures.completedNull();
      }
      if (topologyId < rebalanceTopologyId) {
         log.debugf("Discarding state response with old topology id %d for cache %s, state transfer request topology was %b",
               topologyId, cacheName, waitingForState);
         return CompletableFutures.completedNull();
      }

      if (log.isTraceEnabled()) {
         log.tracef("Before applying the received state the data container of cache %s has %d keys", cacheName,
                    dataContainer.sizeIncludingExpired());
      }
      IntSet mySegments = IntSets.from(wCh.getSegmentsForOwner(rpcManager.getAddress()));
      Iterator<StateChunk> iterator = stateChunks.iterator();
      return applyStateIteration(sender, mySegments, iterator).whenComplete((v, t) -> {
         if (log.isTraceEnabled()) {
            log.tracef("After applying the received state the data container of cache %s has %d keys", cacheName,
                       dataContainer.sizeIncludingExpired());
            synchronized (transferMapsLock) {
               log.tracef("Segments not received yet for cache %s: %s", cacheName, transfersBySource);
            }
         }
      });
   }

   private CompletionStage<?> applyStateIteration(Address sender, IntSet mySegments,
                                                  Iterator<StateChunk> iterator) {
      CompletionStage<?> chunkStage = CompletableFutures.completedNull();
      // Replace recursion with iteration if the state was applied synchronously
      while (iterator.hasNext() && CompletionStages.isCompletedSuccessfully(chunkStage)) {
         StateChunk stateChunk = iterator.next();
         chunkStage = applyChunk(sender, mySegments, stateChunk);
      }
      if (!iterator.hasNext())
         return chunkStage;

      return chunkStage.thenCompose(v -> applyStateIteration(sender, mySegments, iterator));
   }

   private CompletionStage<Void> applyChunk(Address sender, IntSet mySegments, StateChunk stateChunk) {
      if (!mySegments.contains(stateChunk.getSegmentId())) {
         log.debugf("Discarding received cache entries for segment %d of cache %s because they do not belong to this node.", stateChunk.getSegmentId(), cacheName);
         return CompletableFutures.completedNull();
      }

      // Notify the inbound task that a chunk of cache entries was received
      InboundTransferTask inboundTransfer;
      synchronized (transferMapsLock) {
         List<InboundTransferTask> inboundTransfers = transfersBySegment.get(stateChunk.getSegmentId());
         if (inboundTransfers != null) {
            inboundTransfer = inboundTransfers.stream().filter(task -> task.getSource().equals(sender)).findFirst().orElse(null);
         } else {
            inboundTransfer = null;
         }
      }
      if (inboundTransfer != null) {
         return doApplyState(sender, stateChunk.getSegmentId(), stateChunk.getCacheEntries())
                   .thenAccept(v -> {
                      boolean lastChunk = stateChunk.isLastChunk();
                      inboundTransfer.onStateReceived(stateChunk.getSegmentId(), lastChunk);
                      if (lastChunk) {
                         onCompletedSegment(stateChunk.getSegmentId(), inboundTransfer);
                      }
                   });
      } else {
         if (cache.wired().getStatus().allowInvocations()) {
            log.ignoringUnsolicitedState(sender, stateChunk.getSegmentId(), cacheName);
         }
      }
      return CompletableFutures.completedNull();
   }

   private void onCompletedSegment(int segmentId, InboundTransferTask inboundTransfer) {
      synchronized (transferMapsLock) {
         List<InboundTransferTask> innerTransfers = transfersBySegment.get(segmentId);
         if (innerTransfers != null && innerTransfers.remove(inboundTransfer) && innerTransfers.isEmpty()) {
            commitManager.stopTrackFor(PUT_FOR_STATE_TRANSFER, segmentId);
            transfersBySegment.remove(segmentId);
         }
      }
   }

   private CompletionStage<?> doApplyState(Address sender, int segmentId,
                                           Collection<InternalCacheEntry<?, ?>> cacheEntries) {
      if (cacheEntries == null || cacheEntries.isEmpty())
         return CompletableFutures.completedNull();

      if (log.isTraceEnabled()) log.tracef(
            "Applying new state chunk for segment %d of cache %s from node %s: received %d cache entries",
            segmentId, cacheName, sender, cacheEntries.size());

      // CACHE_MODE_LOCAL avoids handling by StateTransferInterceptor and any potential locks in StateTransferLock
      boolean transactional = transactionManager != null;
      if (transactional) {
         Object key = NO_KEY;
         Transaction transaction = new FakeJTATransaction();
         InvocationContext ctx = icf.createInvocationContext(transaction, false);
         LocalTransaction localTransaction = ((LocalTxInvocationContext) ctx).getCacheTransaction();
         try {
            localTransaction.setStateTransferFlag(PUT_FOR_STATE_TRANSFER);
            for (InternalCacheEntry<?, ?> e : cacheEntries) {
               key = e.getKey();
               CompletableFuture<?> future = invokePut(segmentId, ctx, e);
               if (!future.isDone()) {
                  throw new IllegalStateException("State transfer in-tx put should always be synchronous");
               }
            }
         } catch (Throwable t) {
            logApplyException(t, key);
            return invokeRollback(localTransaction).handle((rv, t1) -> {
               transactionTable.removeLocalTransaction(localTransaction);
               if (t1 != null) {
                  t.addSuppressed(t1);
               }
               return null;
            });
         }

         return invoke1PCPrepare(localTransaction).whenComplete((rv, t) -> {
            transactionTable.removeLocalTransaction(localTransaction);
            if (t != null) {
               logApplyException(t, NO_KEY);
            }
         });
      } else {
         // non-tx cache
         AggregateCompletionStage<Void> aggregateStage = CompletionStages.aggregateCompletionStage();
         for (InternalCacheEntry<?, ?> e : cacheEntries) {
            InvocationContext ctx = icf.createSingleKeyNonTxInvocationContext();
            CompletionStage<?> putStage = invokePut(segmentId, ctx, e);
            aggregateStage.dependsOn(putStage.exceptionally(t -> {
               logApplyException(t, e.getKey());
               return null;
            }));
         }
         return aggregateStage.freeze();
      }
   }

   private CompletionStage<?> invoke1PCPrepare(LocalTransaction localTransaction) {
      PrepareCommand prepareCommand;
      if (Configurations.isTxVersioned(configuration)) {
         prepareCommand = commandsFactory.buildVersionedPrepareCommand(localTransaction.getGlobalTransaction(),
                                                                       localTransaction.getModifications(), true);
      } else {
         prepareCommand = commandsFactory.buildPrepareCommand(localTransaction.getGlobalTransaction(),
                                                              localTransaction.getModifications(), true);
      }
      LocalTxInvocationContext ctx = icf.createTxInvocationContext(localTransaction);
      return interceptorChain.invokeAsync(ctx, prepareCommand);
   }

   private CompletionStage<?> invokeRollback(LocalTransaction localTransaction) {
      RollbackCommand prepareCommand = commandsFactory.buildRollbackCommand(localTransaction.getGlobalTransaction());
      LocalTxInvocationContext ctx = icf.createTxInvocationContext(localTransaction);
      return interceptorChain.invokeAsync(ctx, prepareCommand);
   }

   private CompletableFuture<?> invokePut(int segmentId, InvocationContext ctx, InternalCacheEntry<?, ?> e) {
      // CallInterceptor will preserve the timestamps if the metadata is an InternalMetadataImpl instance
      InternalMetadataImpl metadata = new InternalMetadataImpl(e);
      PutKeyValueCommand put = commandsFactory.buildPutKeyValueCommand(e.getKey(), e.getValue(), segmentId,
                                                                       metadata, STATE_TRANSFER_FLAGS);
      put.setInternalMetadata(e.getInternalMetadata());
      ctx.setLockOwner(put.getKeyLockOwner());
      return interceptorChain.invokeAsync(ctx, put);
   }

   private void logApplyException(Throwable t, Object key) {
      if (!cache.wired().getStatus().allowInvocations()) {
         log.tracef("Cache %s is shutting down, stopping state transfer", cacheName);
      } else {
         log.problemApplyingStateForKey(key, t);
      }
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
                     // just in case, set the previous topology id to make the current topology id check for pending locks.
                     tx = transactionTable.getOrCreateRemoteTransaction(gtx, transactionInfo.getModifications(), topologyId - 1);
                     // Force this node to replay the given transaction data by making it think it is 1 behind
                     ((RemoteTransaction) tx).setLookedUpEntriesTopology(topologyId - 1);
                  } catch (Throwable t) {
                     if (log.isTraceEnabled())
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
   @Start
   public void start() {
      cacheName = cache.wired().getName();
      isInvalidationMode = configuration.clustering().cacheMode().isInvalidation();
      isTransactional = configuration.transaction().transactionMode().isTransactional();
      timeout = configuration.clustering().stateTransfer().timeout();
      numSegments = configuration.clustering().hash().numSegments();

      isFetchEnabled = isFetchEnabled();

      rpcOptions = new RpcOptions(DeliverOrder.NONE, timeout, TimeUnit.MILLISECONDS);

      requestedTransactionalSegments = IntSets.concurrentSet(numSegments);

      stateRequestExecutor = new LimitedExecutor("StateRequest-" + cacheName, nonBlockingExecutor, 1);
      running = true;
   }

   private boolean isFetchEnabled() {
      return configuration.clustering().cacheMode().needsStateTransfer() &&
            configuration.clustering().stateTransfer().fetchInMemoryState();
   }

   @Stop
   @Override
   public void stop() {
      if (log.isTraceEnabled()) {
         log.tracef("Shutting down StateConsumer of cache %s on node %s", cacheName, rpcManager.getAddress());
      }
      running = false;

      try {
         synchronized (transferMapsLock) {
            // cancel all inbound transfers
            // make a copy and then clear both maps so that cancel doesn't interfere with the iteration
            Collection<List<InboundTransferTask>> transfers = new ArrayList<>(transfersBySource.values());
            transfersBySource.clear();
            transfersBySegment.clear();
            for (List<InboundTransferTask> inboundTransfers : transfers) {
               inboundTransfers.forEach(InboundTransferTask::cancel);
            }
         }
         requestedTransactionalSegments.clear();
         stateRequestExecutor.shutdownNow();
      } catch (Throwable t) {
         log.errorf(t, "Failed to stop StateConsumer of cache %s on node %s", cacheName, rpcManager.getAddress());
      }
   }

   public void setKeyInvalidationListener(KeyInvalidationListener keyInvalidationListener) {
      this.keyInvalidationListener = keyInvalidationListener;
   }

   protected CompletionStage<Void> handleSegments(IntSet addedSegments, IntSet transactionOnlySegments) {
      if (addedSegments.isEmpty() && transactionOnlySegments.isEmpty()) {
         return CompletableFutures.completedNull();
      }

      // add transfers for new or restarted segments
      log.debugf("Adding inbound state transfer for segments %s", addedSegments);

      // the set of nodes that reported errors when fetching data from them - these will not be retried in this topology
      Set<Address> excludedSources = new HashSet<>();

      // the sources and segments we are going to get from each source
      Map<Address, IntSet> sources = new HashMap<>();

      CompletionStage<Void> stage = CompletableFutures.completedNull();
      if (isTransactional) {
         stage = requestTransactions(addedSegments, transactionOnlySegments, sources, excludedSources);
      }

      if (isFetchEnabled) {
         stage = stage.thenRun(() -> requestSegments(addedSegments, sources, excludedSources));
      }

      return stage;
   }

   private void findSources(IntSet segments, Map<Address, IntSet> sources, Set<Address> excludedSources,
                            boolean ignoreOwnedSegments) {
      if (cache.wired().getStatus().isTerminated())
         return;

      IntSet segmentsWithoutSource = IntSets.mutableEmptySet(numSegments);
      for (PrimitiveIterator.OfInt iter = segments.iterator(); iter.hasNext(); ) {
         int segmentId = iter.nextInt();
         Address source = findSource(segmentId, excludedSources, ignoreOwnedSegments);
         // ignore all segments for which there are no other owners to pull data from.
         // these segments are considered empty (or lost) and do not require a state transfer
         if (source != null) {
            IntSet segmentsFromSource = sources.computeIfAbsent(source, k -> IntSets.mutableEmptySet(numSegments));
            segmentsFromSource.set(segmentId);
         } else {
            segmentsWithoutSource.set(segmentId);
         }
      }
      if (!segmentsWithoutSource.isEmpty()) {
         log.noLiveOwnersFoundForSegments(segmentsWithoutSource, cacheName, excludedSources);
      }
   }

   private Address findSource(int segmentId, Set<Address> excludedSources, boolean ignoreOwnedSegment) {
      List<Address> owners = cacheTopology.getReadConsistentHash().locateOwnersForSegment(segmentId);
      if (!ignoreOwnedSegment || !owners.contains(rpcManager.getAddress())) {
         // We prefer that transactions are sourced from primary owners.
         // Needed in pessimistic mode, if the originator is the primary owner of the key than the lock
         // command is not replicated to the backup owners. See PessimisticDistributionInterceptor
         // .acquireRemoteIfNeeded.
         for (Address o : owners) {
            if (!o.equals(rpcManager.getAddress()) && !excludedSources.contains(o)) {
               return o;
            }
         }
      }
      return null;
   }

   private CompletionStage<Void> requestTransactions(IntSet dataSegments, IntSet transactionOnlySegments,
                                                     Map<Address, IntSet> sources,
                                                     Set<Address> excludedSources) {
      // TODO Remove excludedSources and always only for transactions/segments from the primary owner
      findSources(dataSegments, sources, excludedSources, true);

      AggregateCompletionStage<Void> aggregateStage = CompletionStages.aggregateCompletionStage();
      IntSet failedSegments = IntSets.concurrentSet(numSegments);
      Set<Address> sourcesToExclude = ConcurrentHashMap.newKeySet();
      int topologyId = cacheTopology.getTopologyId();
      sources.forEach((source, segmentsFromSource) -> {
         CompletionStage<Response> sourceStage =
               requestAndApplyTransactions(failedSegments, sourcesToExclude, topologyId, source, segmentsFromSource);
         aggregateStage.dependsOn(sourceStage);
      });
      Map<Address, IntSet> transactionOnlySources = new HashMap<>();
      findSources(transactionOnlySegments, transactionOnlySources, excludedSources, false);
      transactionOnlySources.forEach((source, segmentsFromSource) -> {
         CompletionStage<Response> sourceStage =
               requestAndApplyTransactions(failedSegments, sourcesToExclude, topologyId, source, segmentsFromSource);
         aggregateStage.dependsOn(sourceStage);
      });

      return aggregateStage.freeze().thenCompose(ignored -> {
         if (failedSegments.isEmpty()) {
            return CompletableFutures.completedNull();
         }

         excludedSources.addAll(sourcesToExclude);

         // look for other sources for all failed segments
         sources.clear();
         return requestTransactions(dataSegments, transactionOnlySegments, sources, excludedSources);
      });
   }

   private CompletionStage<Response> requestAndApplyTransactions(IntSet failedSegments, Set<Address> sourcesToExclude,
                                                                 int topologyId, Address source,
                                                                 IntSet segmentsFromSource) {
      // Register the requested transactional segments
      requestedTransactionalSegments.addAll(segmentsFromSource);
      return getTransactions(source, segmentsFromSource, topologyId)
            .whenComplete((response, throwable) -> {
               processTransactionsResponse(failedSegments, sourcesToExclude, topologyId,
                                           source, segmentsFromSource, response, throwable);
               requestedTransactionalSegments.removeAll(segmentsFromSource);
            });
   }

   private void processTransactionsResponse(IntSet failedSegments,
                                            Set<Address> sourcesToExclude, int topologyId, Address source,
                                            IntSet segmentsFromSource, Response response, Throwable throwable) {
      boolean failed = false;
      boolean exclude = false;
      if (throwable != null) {
         if (cache.wired().getStatus().isTerminated()) {
            log.debugf("Cache %s has stopped while requesting transactions", cacheName);
            return;
         } else {
            log.failedToRetrieveTransactionsForSegments(cacheName, source, segmentsFromSource, throwable);
         }
         // The primary owner is still in the cluster, so we can't exclude it - see ISPN-4091
         failed = true;
      }
      if (response instanceof SuccessfulResponse) {
         Collection<TransactionInfo> transactions = ((SuccessfulResponse) response).getResponseCollection();
         applyTransactions(source, transactions, topologyId);
      } else if (response instanceof CacheNotFoundResponse) {
         log.debugf("Cache %s was stopped on node %s before sending transaction information", cacheName, source);
         failed = true;
         exclude = true;
      } else {
         log.unsuccessfulResponseRetrievingTransactionsForSegments(source, response);
         failed = true;
      }

      // If requesting the transactions failed we need to retry
      if (failed) {
         failedSegments.addAll(segmentsFromSource);
      }
      if (exclude) {
         sourcesToExclude.add(source);
      }
   }

   private CompletionStage<Collection<ClusterListenerReplicateCallable<Object, Object>>> getClusterListeners(
         int topologyId, List<Address> sources) {
      // Try the first member. If the request fails, fall back to the second member and so on.
      if (sources.isEmpty()) {
         if (log.isTraceEnabled()) // TODO Ignore self again
            log.trace("Unable to acquire cluster listeners from other members, assuming none are present");
         return CompletableFuture.completedFuture(Collections.emptySet());
      }

      Address source = sources.get(0);
      // Don't send the request to self
      if (sources.get(0).equals(rpcManager.getAddress())) {
         return getClusterListeners(topologyId, sources.subList(1, sources.size()));
      }
      if (log.isTraceEnabled())
         log.tracef("Requesting cluster listeners of cache %s from node %s", cacheName, sources);

      CacheRpcCommand cmd = commandsFactory.buildStateTransferGetListenersCommand(topologyId);

      CompletionStage<ValidResponse> remoteStage =
            rpcManager.invokeCommand(source, cmd, SingleResponseCollector.validOnly(), rpcOptions);
      return handleAndCompose(remoteStage, (response, throwable) -> {
         if (throwable != null) {
            log.exceptionDuringClusterListenerRetrieval(source, throwable);
         }

         if (response instanceof SuccessfulResponse) {
            return CompletableFuture.completedFuture(response.getResponseCollection());
         } else {
            log.unsuccessfulResponseForClusterListeners(source, response);
            return getClusterListeners(topologyId, sources.subList(1, sources.size()));
         }
      });
   }

   private CompletionStage<Response> getTransactions(Address source, IntSet segments, int topologyId) {
      if (log.isTraceEnabled()) {
         log.tracef("Requesting transactions from node %s for segments %s", source, segments);
      }

      // get transactions and locks
      CacheRpcCommand cmd = commandsFactory.buildStateTransferGetTransactionsCommand(topologyId, segments);
      return rpcManager.invokeCommand(source, cmd, PassthroughSingleResponseCollector.INSTANCE, rpcOptions);
   }

   private void requestSegments(IntSet segments, Map<Address, IntSet> sources, Set<Address> excludedSources) {
      if (sources.isEmpty()) {
         findSources(segments, sources, excludedSources, true);
      }

      for (Map.Entry<Address, IntSet> e : sources.entrySet()) {
         addTransfer(e.getKey(), e.getValue());
      }
      if (log.isTraceEnabled()) log.tracef("Finished adding inbound state transfer for segments %s", segments, cacheName);
   }

   /**
    * Cancel transfers for segments we no longer own.
    *
    * @param removedSegments segments to be cancelled
    */
   protected void cancelTransfers(IntSet removedSegments) {
      synchronized (transferMapsLock) {
         List<Integer> segmentsToCancel = new ArrayList<>(removedSegments);
         while (!segmentsToCancel.isEmpty()) {
            int segmentId = segmentsToCancel.remove(0);
            List<InboundTransferTask> inboundTransfers = transfersBySegment.get(segmentId);
            if (inboundTransfers != null) { // we need to check the transfer was not already completed
               for (InboundTransferTask inboundTransfer : inboundTransfers) {
                  IntSet cancelledSegments = IntSets.mutableCopyFrom(removedSegments);
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

   protected CompletionStage<Void> removeStaleData(final IntSet removedSegments) {
      // Invalidation doesn't ever remove stale data
      if (configuration.clustering().cacheMode().isInvalidation()) {
         return CompletableFutures.completedNull();
      }
      log.debugf("Removing no longer owned entries for cache %s", cacheName);
      if (keyInvalidationListener != null) {
         keyInvalidationListener.beforeInvalidation(removedSegments, IntSets.immutableEmptySet());
      }

      // This has to be invoked before removing the segments on the data container
      localPublisherManager.segmentsLost(removedSegments);

      dataContainer.removeSegments(removedSegments);

      // We have to invoke removeSegments above on the data container. This is always done in case if L1 is enabled. L1
      // store removes all the temporary entries when removeSegments is invoked. However there is no reason to mess
      // with the store if no segments are removed, so just exit early.
      if (removedSegments.isEmpty())
         return CompletableFutures.completedNull();

      return persistenceManager.removeSegments(removedSegments)
                               .thenCompose(removed -> invalidateStaleEntries(removedSegments, removed));
   }

   private CompletionStage<Void> invalidateStaleEntries(IntSet removedSegments, Boolean removed) {
      // If there are no stores that couldn't remove segments, we don't have to worry about invaliding entries
      if (removed) {
         return CompletableFutures.completedNull();
      }

      // All these segments have been removed from the data container, so we only care about private stores
      AtomicLong removedEntriesCounter = new AtomicLong();
      Predicate<Object> filter = key -> removedSegments.contains(getSegment(key));
      Publisher<Object> publisher = persistenceManager.publishKeys(filter, PRIVATE);
      return Flowable.fromPublisher(publisher)
                     .onErrorResumeNext(throwable -> {
                        PERSISTENCE.failedLoadingKeysFromCacheStore(throwable);
                        return Flowable.empty();
                     })
                     .buffer(configuration.clustering().stateTransfer().chunkSize())
                     .concatMapCompletable(keysToRemove -> {
                        removedEntriesCounter.addAndGet(keysToRemove.size());
                        return Completable.fromCompletionStage(invalidateBatch(keysToRemove));
                     })
                     .toCompletionStage(null)
                     .thenRun(() -> {
                        if (log.isTraceEnabled()) log.tracef("Removed %d keys, data container now has %d keys",
                                              removedEntriesCounter.get(), dataContainer.sizeIncludingExpired());
                     });
   }

   protected CompletionStage<Void> invalidateBatch(Collection<Object> keysToRemove) {
      InvalidateCommand invalidateCmd = commandsFactory.buildInvalidateCommand(INVALIDATE_FLAGS, keysToRemove.toArray());
      InvocationContext ctx = icf.createNonTxInvocationContext();
      ctx.setLockOwner(invalidateCmd.getKeyLockOwner());
      return interceptorChain.invokeAsync(ctx, invalidateCmd)
                             .handle((ignored, throwable) -> {
                                if (throwable != null && !(throwable instanceof IllegalLifecycleStateException)) {
                                   // Ignore shutdown-related errors, because InvocationContextInterceptor starts
                                   // rejecting commands before any component is stopped
                                   log.failedToInvalidateKeys(throwable);
                                }
                                return null;
                             });
   }

   /**
    * Check if any of the existing transfers should be restarted from a different source because the initial source
    * is no longer a member.
    */
   private void restartBrokenTransfers(CacheTopology cacheTopology, IntSet addedSegments) {
      Set<Address> members = new HashSet<>(cacheTopology.getReadConsistentHash().getMembers());
      synchronized (transferMapsLock) {
         for (Iterator<Map.Entry<Address, List<InboundTransferTask>>> it =
              transfersBySource.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Address, List<InboundTransferTask>> entry = it.next();
            Address source = entry.getKey();
            if (!members.contains(source)) {
               if (log.isTraceEnabled()) {
                  log.tracef("Removing inbound transfers from source %s for cache %s", source, cacheName);
               }
               List<InboundTransferTask> inboundTransfers = entry.getValue();
               it.remove();
               for (InboundTransferTask inboundTransfer : inboundTransfers) {
                  // these segments will be restarted if they are still in new write CH
                  if (log.isTraceEnabled()) {
                     log.tracef("Removing inbound transfers from node %s for segments %s", source, inboundTransfer.getSegments());
                  }
                  IntSet unfinishedSegments = inboundTransfer.getUnfinishedSegments();
                  inboundTransfer.cancel();
                  addedSegments.addAll(unfinishedSegments);
                  transfersBySegment.keySet().removeAll(unfinishedSegments);
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

   private void addTransfer(Address source, IntSet segmentsFromSource) {
      final InboundTransferTask inboundTransfer;

      synchronized (transferMapsLock) {
         if (log.isTraceEnabled()) {
            log.tracef("Adding transfer from %s for segments %s", source, segmentsFromSource);
         }
         segmentsFromSource.removeAll(transfersBySegment.keySet());  // already in progress segments are excluded
         if (segmentsFromSource.isEmpty()) {
            if (log.isTraceEnabled()) {
               log.tracef("All segments are already in progress, skipping");
            }
            return;
         }

         inboundTransfer = new InboundTransferTask(segmentsFromSource, source, cacheTopology.getTopologyId(),
                                                   rpcManager, commandsFactory, timeout, cacheName, true);
         addTransfer(inboundTransfer, segmentsFromSource);
      }

      stateRequestExecutor.executeAsync(() -> {
         CompletionStage<Void> transferStarted = inboundTransfer.requestSegments();
         return transferStarted.whenComplete((aVoid, throwable) -> onTaskCompletion(inboundTransfer));
      });
   }

   @GuardedBy("transferMapsLock")
   protected void addTransfer(InboundTransferTask inboundTransfer, IntSet segments) {
      if (!running)
         throw new IllegalLifecycleStateException("State consumer is not running for cache " + cacheName);

      for (PrimitiveIterator.OfInt iter = segments.iterator(); iter.hasNext(); ) {
         int segmentId = iter.nextInt();
         transfersBySegment.computeIfAbsent(segmentId, s -> new ArrayList<>()).add(inboundTransfer);
      }
      transfersBySource.computeIfAbsent(inboundTransfer.getSource(), s -> new ArrayList<>()).add(inboundTransfer);
   }

   protected void removeTransfer(InboundTransferTask inboundTransfer) {
      synchronized (transferMapsLock) {
         if (log.isTraceEnabled()) log.tracef("Removing inbound transfers from node %s for segments %s",
               inboundTransfer.getSegments(), inboundTransfer.getSource(), cacheName);
         List<InboundTransferTask> transfers = transfersBySource.get(inboundTransfer.getSource());
         if (transfers != null && transfers.remove(inboundTransfer) && transfers.isEmpty()) {
            transfersBySource.remove(inboundTransfer.getSource());
         }
         // Box the segment as the map uses Integer as key
         for (Integer segment : inboundTransfer.getSegments()) {
            List<InboundTransferTask> innerTransfers = transfersBySegment.get(segment);
            if (innerTransfers != null && innerTransfers.remove(inboundTransfer) && innerTransfers.isEmpty()) {
               transfersBySegment.remove(segment);
            }
         }
      }
   }

   protected void onTaskCompletion(final InboundTransferTask inboundTransfer) {
      if (log.isTraceEnabled()) log.tracef("Inbound transfer finished: %s", inboundTransfer);
      if (inboundTransfer.isCompletedSuccessfully()) {
         removeTransfer(inboundTransfer);
         notifyEndOfStateTransferIfNeeded();
      }
   }

   public interface KeyInvalidationListener {
      void beforeInvalidation(IntSet removedSegments, IntSet staleL1Segments);
   }

}
