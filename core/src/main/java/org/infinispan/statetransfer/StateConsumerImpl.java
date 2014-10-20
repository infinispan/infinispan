package org.infinispan.statetransfer;

import net.jcip.annotations.GuardedBy;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.commons.util.concurrent.ParallelIterableMap;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.distribution.L1Manager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.filter.KeyFilter;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.totalorder.TotalOrderLatch;
import org.infinispan.transaction.totalorder.TotalOrderManager;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.BlockingTaskAwareExecutorService;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.TransactionManager;

import java.util.*;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.infinispan.context.Flag.*;
import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;
import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.PRIVATE;

/**
 * {@link StateConsumer} implementation.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class StateConsumerImpl implements StateConsumer {

   private static final Log log = LogFactory.getLog(StateConsumerImpl.class);
   private static final boolean trace = log.isTraceEnabled();
   public static final int NO_REBALANCE_IN_PROGRESS = -1;

   private Cache cache;
   private ExecutorService executorService;
   private StateTransferManager stateTransferManager;
   private String cacheName;
   private Configuration configuration;
   private RpcManager rpcManager;
   private TransactionManager transactionManager;   // optional
   private CommandsFactory commandsFactory;
   private TransactionTable transactionTable;       // optional
   private DataContainer<Object, Object> dataContainer;
   private PersistenceManager persistenceManager;
   private InterceptorChain interceptorChain;
   private InvocationContextFactory icf;
   private StateTransferLock stateTransferLock;
   private CacheNotifier cacheNotifier;
   private TotalOrderManager totalOrderManager;
   private BlockingTaskAwareExecutorService remoteCommandsExecutor;
   private L1Manager l1Manager;
   private long timeout;
   private boolean isFetchEnabled;
   private boolean isTransactional;
   private boolean isInvalidationMode;
   private boolean isTotalOrder;
   private volatile KeyInvalidationListener keyInvalidationListener; //for test purpose only!
   private CommitManager commitManager;

   private volatile CacheTopology cacheTopology;

   /**
    * Indicates if there is a state transfer in progress. It is set to the new topology id when onTopologyUpdate with
    * isRebalance==true is called.
    * It is changed back to NO_REBALANCE_IN_PROGRESS when a topology update with a null pending CH is received.
    */
   private final AtomicInteger stateTransferTopologyId = new AtomicInteger(NO_REBALANCE_IN_PROGRESS);

   /**
    * Indicates if there is a rebalance in progress and there the local node has not yet received
    * all the new segments yet. It is set to true when rebalance starts and becomes when all inbound transfers have completed
    * (before stateTransferTopologyId is set back to NO_REBALANCE_IN_PROGRESS).
    */
   private final AtomicBoolean waitingForState = new AtomicBoolean(false);

   private final Object transferMapsLock = new Object();

   /**
    * A map that keeps track of current inbound state transfers by source address. There could be multiple transfers
    * flowing in from the same source (but for different segments) so the values are lists. This works in tandem with
    * transfersBySegment so they always need to be kept in sync and updates to both of them need to be atomic.
    */
   @GuardedBy("transferMapsLock")
   private final Map<Address, List<InboundTransferTask>> transfersBySource = new HashMap<Address, List<InboundTransferTask>>();

   /**
    * A map that keeps track of current inbound state transfers by segment id. There is at most one transfers per segment.
    * This works in tandem with transfersBySource so they always need to be kept in sync and updates to both of them
    * need to be atomic.
    */
   @GuardedBy("transferMapsLock")
   private final Map<Integer, InboundTransferTask> transfersBySegment = new HashMap<Integer, InboundTransferTask>();

   /**
    * Tasks ready to be executed by the transfer thread. These tasks are also present if transfersBySegment and transfersBySource.
    */
   private final BlockingDeque<InboundTransferTask> taskQueue = new LinkedBlockingDeque<InboundTransferTask>();

   private final AtomicBoolean isTransferThreadRunning = new AtomicBoolean(false);

   private volatile boolean ownsData = false;

   private RpcOptions rpcOptions;

   public StateConsumerImpl() {
   }

   /**
    * Stops applying incoming state. Also stops tracking updated keys. Should be called at the end of state transfer or
    * when a ClearCommand is committed during state transfer.
    */
   @Override
   public void stopApplyingState() {
      if (trace) log.tracef("Stop keeping track of changed keys for state transfer");
      commitManager.stopTrack(PUT_FOR_STATE_TRANSFER);
   }

   @Inject
   public void init(Cache cache,
                    @ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService executorService, //TODO Use a dedicated ExecutorService
                    StateTransferManager stateTransferManager,
                    InterceptorChain interceptorChain,
                    InvocationContextFactory icf,
                    Configuration configuration,
                    RpcManager rpcManager,
                    TransactionManager transactionManager,
                    CommandsFactory commandsFactory,
                    PersistenceManager persistenceManager,
                    DataContainer dataContainer,
                    TransactionTable transactionTable,
                    StateTransferLock stateTransferLock,
                    CacheNotifier cacheNotifier,
                    TotalOrderManager totalOrderManager,
                    @ComponentName(KnownComponentNames.REMOTE_COMMAND_EXECUTOR) BlockingTaskAwareExecutorService remoteCommandsExecutor,
                    L1Manager l1Manager, CommitManager commitManager) {
      this.cache = cache;
      this.cacheName = cache.getName();
      this.executorService = executorService;
      this.stateTransferManager = stateTransferManager;
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
      this.l1Manager = l1Manager;
      this.commitManager = commitManager;

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
      return stateTransferTopologyId.get() != NO_REBALANCE_IN_PROGRESS;
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

      CacheTopology localCacheTopology = cacheTopology;
      if (localCacheTopology == null || localCacheTopology.getPendingCH() == null)
         return false;
      Address address = rpcManager.getAddress();
      boolean keyWillBeLocal = localCacheTopology.getPendingCH().isKeyLocalToNode(address, key);
      boolean keyIsLocal = localCacheTopology.getCurrentCH().isKeyLocalToNode(address, key);
      return keyWillBeLocal && !keyIsLocal;
   }

   @Override
   public boolean ownsData() {
      return ownsData;
   }

   @Override
   public void onTopologyUpdate(final CacheTopology cacheTopology, final boolean isRebalance) {
      final boolean isMember = cacheTopology.getMembers().contains(rpcManager.getAddress());
      if (trace) log.tracef("Received new topology for cache %s, isRebalance = %b, isMember = %b, topology = %s", cacheName, isRebalance, isMember, cacheTopology);

      if (!ownsData && isMember) {
         ownsData = true;
      }

      if (isRebalance) {
         // Only update the rebalance topology id when starting the rebalance, as we're going to ignore any state
         // response with a smaller topology id
         stateTransferTopologyId.compareAndSet(NO_REBALANCE_IN_PROGRESS, cacheTopology.getTopologyId());
         cacheNotifier.notifyDataRehashed(cacheTopology.getCurrentCH(), cacheTopology.getPendingCH(),
                                          cacheTopology.getUnionCH(), cacheTopology.getTopologyId(), true);
      }

      awaitTotalOrderTransactions(cacheTopology, isRebalance);

      // Make sure we don't send a REBALANCE_CONFIRM command before we've added all the transfer tasks
      // even if some of the tasks are removed and re-added
      waitingForState.set(false);

      final ConsistentHash newReadCh = cacheTopology.getReadConsistentHash();
      final ConsistentHash newWriteCh = cacheTopology.getWriteConsistentHash();
      final ConsistentHash previousReadCh = this.cacheTopology != null ? this.cacheTopology.getReadConsistentHash() : null;
      final ConsistentHash previousWriteCh = this.cacheTopology != null ? this.cacheTopology.getWriteConsistentHash() : null;
      // Ensures writes to the data container use the right consistent hash
      // No need for a try/finally block, since it's just an assignment
      stateTransferLock.acquireExclusiveTopologyLock();
      this.cacheTopology = cacheTopology;
      if (isRebalance) {
         if (trace) log.tracef("Start keeping track of keys for rebalance");
         commitManager.stopTrack(PUT_FOR_STATE_TRANSFER);
         commitManager.startTrack(PUT_FOR_STATE_TRANSFER);
      }
      stateTransferLock.releaseExclusiveTopologyLock();
      stateTransferLock.notifyTopologyInstalled(cacheTopology.getTopologyId());

      try {
         // fetch transactions and data segments from other owners if this is enabled
         if (isTransactional || isFetchEnabled) {
            Set<Integer> addedSegments;
            if (previousWriteCh == null) {
               // we start fresh, without any data, so we need to pull everything we own according to writeCh
               addedSegments = getOwnedSegments(newWriteCh);

               // TODO Perhaps we should only do this once we are a member, as listener installation should happen only on cache members?
               if (configuration.clustering().cacheMode().isDistributed()) {
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

               Set<Integer> removedSegments = new HashSet<Integer>(previousSegments);
               removedSegments.removeAll(newSegments);

               // This is a rebalance, we need to request the segments we own in the new CH.
               addedSegments = new HashSet<Integer>(newSegments);
               addedSegments.removeAll(previousSegments);

               if (trace) {
                  log.tracef("On cache %s we have: new segments: %s; old segments: %s; removed segments: %s; added segments: %s",
                        cacheName, newSegments, previousSegments, removedSegments, addedSegments);
               }

               // remove inbound transfers for segments we no longer own
               cancelTransfers(removedSegments);

               // check if any of the existing transfers should be restarted from a different source because the initial source is no longer a member
               restartBrokenTransfers(cacheTopology, addedSegments);
            }

            if (!addedSegments.isEmpty()) {
               addTransfers(addedSegments);  // add transfers for new or restarted segments
            }
         }

         int rebalanceTopologyId = stateTransferTopologyId.get();
         log.tracef("Topology update processed, stateTransferTopologyId = %s, isRebalance = %s, pending CH = %s",
               rebalanceTopologyId, isRebalance, cacheTopology.getPendingCH());
         if (rebalanceTopologyId != NO_REBALANCE_IN_PROGRESS) {
            // there was a rebalance in progress
            if (!isRebalance && cacheTopology.getPendingCH() == null) {
               // we have received a topology update without a pending CH, signalling the end of the rebalance
               boolean changed = stateTransferTopologyId.compareAndSet(rebalanceTopologyId, NO_REBALANCE_IN_PROGRESS);
               if (changed) {
                  stopApplyingState();

                  // if the coordinator changed, we might get two concurrent topology updates,
                  // but we only want to notify the @DataRehashed listeners once
                  cacheNotifier.notifyDataRehashed(previousReadCh, cacheTopology.getCurrentCH(), previousWriteCh,
                        cacheTopology.getTopologyId(), false);
                  if (log.isTraceEnabled()) {
                     log.tracef("Unlock State Transfer in Progress for topology ID %s", cacheTopology.getTopologyId());
                  }
                  if (isTotalOrder) {
                     totalOrderManager.notifyStateTransferEnd();
                  }
               }
            }
         }
      } finally {
         stateTransferLock.notifyTransactionDataReceived(cacheTopology.getTopologyId());
         remoteCommandsExecutor.checkForReadyTasks();

         // Only set the flag here, after all the transfers have been added to the transfersBySource map
         if (stateTransferTopologyId.get() != NO_REBALANCE_IN_PROGRESS) {
            waitingForState.set(true);
         }

         notifyEndOfRebalanceIfNeeded(cacheTopology.getTopologyId(), cacheTopology.getRebalanceId());

         // Remove the transactions whose originators have left the cache.
         // Need to do it now, after we have applied any transactions from other nodes,
         // and after notifyTransactionDataReceived - otherwise the RollbackCommands would block.
         if (transactionTable != null) {
            transactionTable.cleanupLeaverTransactions(cacheTopology);
         }

         // Any data for segments we no longer own should be removed from data container and cache store
         // If L1 is enabled, we need to discard the keys that we might have had in L1 as well
         if (isMember) {
            Set<Integer> removedSegments;
            removedSegments = new HashSet<>(newWriteCh.getNumSegments());
            for (int i = 0; i < newWriteCh.getNumSegments(); i++) {
               removedSegments.add(i);
            }
            Set<Integer> newSegments = newWriteCh.getSegmentsForOwner(rpcManager.getAddress());
            removedSegments.removeAll(newSegments);

            Set<Integer> staleL1Segments = computeStaleL1Segments(newSegments, newWriteCh, previousWriteCh);
            try {
               removeStaleData(removedSegments, staleL1Segments);
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
               throw new CacheException(e);
            }
         }
      }
   }

   private void awaitTotalOrderTransactions(CacheTopology cacheTopology, boolean isRebalance) {
      //in total order, we should wait for remote transactions before proceeding
      if (isTotalOrder) {
         if (log.isTraceEnabled()) {
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
         if (log.isTraceEnabled()) {
            log.trace("State Transfer in Total Order cache. All remote transactions are finished. Moving on...");
         }
      }

   }

   private void notifyEndOfRebalanceIfNeeded(int topologyId, int rebalanceId) {
      if (waitingForState.get() && !hasActiveTransfers()) {
         if (waitingForState.compareAndSet(true, false)) {
            log.debugf("Finished receiving of segments for cache %s for topology %d.", cacheName, topologyId);
            stopApplyingState();
            stateTransferManager.notifyEndOfRebalance(topologyId, rebalanceId);
         }
      }
   }

   private Set<Integer> getOwnedSegments(ConsistentHash consistentHash) {
      Address address = rpcManager.getAddress();
      return consistentHash.getMembers().contains(address) ? consistentHash.getSegmentsForOwner(address)
            : InfinispanCollections.<Integer>emptySet();
   }

   public void applyState(Address sender, int topologyId, Collection<StateChunk> stateChunks) {
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
      if (rebalanceTopologyId == NO_REBALANCE_IN_PROGRESS) {
         log.debugf("Discarding state response with topology id %d for cache %s, we don't have a state transfer in progress",
               topologyId, cacheName);
         return;
      }
      if (topologyId < rebalanceTopologyId) {
         log.debugf("Discarding state response with old topology id %d for cache %s, state transfer request topology was %d",
               topologyId, cacheName, waitingForState.get());
         return;
      }

      if (trace) {
         log.tracef("Before applying the received state the data container of cache %s has %d keys", cacheName, dataContainer.size());
      }

      Set<Integer> mySegments = wCh.getSegmentsForOwner(rpcManager.getAddress());
      for (StateChunk stateChunk : stateChunks) {
         if (!mySegments.contains(stateChunk.getSegmentId())) {
            log.warnf("Discarding received cache entries for segment %d of cache %s because they do not belong to this node.", stateChunk.getSegmentId(), cacheName);
            continue;
         }

         // Notify the inbound task that a chunk of cache entries was received
         InboundTransferTask inboundTransfer;
         synchronized (transferMapsLock) {
            inboundTransfer = transfersBySegment.get(stateChunk.getSegmentId());
         }
         if (inboundTransfer != null) {
            if (stateChunk.getCacheEntries() != null) {
               doApplyState(sender, stateChunk.getSegmentId(), stateChunk.getCacheEntries());
            }

            inboundTransfer.onStateReceived(stateChunk.getSegmentId(), stateChunk.isLastChunk());
         } else {
            log.warnf("Received unsolicited state from node %s for segment %d of cache %s", sender, stateChunk.getSegmentId(), cacheName);
         }
      }

      if (trace) {
         log.tracef("After applying the received state the data container of cache %s has %d keys", cacheName, dataContainer.size());
         synchronized (transferMapsLock) {
            log.tracef("Segments not received yet for cache %s: %s", cacheName, transfersBySource);
         }
      }
   }

   private void doApplyState(Address sender, int segmentId, Collection<InternalCacheEntry> cacheEntries) {
      log.debugf("Applying new state chunk for segment %d of cache %s from node %s: received %d cache entries", segmentId, cacheName, sender, cacheEntries.size());
      if (trace) {
         List<Object> keys = new ArrayList<Object>(cacheEntries.size());
         for (InternalCacheEntry e : cacheEntries) {
            keys.add(e.getKey());
         }
         log.tracef("Received chunk with keys %s for segment %d of cache %s from node %s", keys, segmentId, cacheName, sender);
      }

      // CACHE_MODE_LOCAL avoids handling by StateTransferInterceptor and any potential locks in StateTransferLock
      EnumSet<Flag> flags = EnumSet.of(PUT_FOR_STATE_TRANSFER, CACHE_MODE_LOCAL, IGNORE_RETURN_VALUES, SKIP_REMOTE_LOOKUP, SKIP_SHARED_CACHE_STORE, SKIP_OWNERSHIP_CHECK, SKIP_XSITE_BACKUP);
      for (InternalCacheEntry e : cacheEntries) {
         try {
            InvocationContext ctx;
            if (transactionManager != null) {
               // cache is transactional
               transactionManager.begin();
               ctx = icf.createInvocationContext(transactionManager.getTransaction(), true);
               ((TxInvocationContext) ctx).getCacheTransaction().setStateTransferFlag(PUT_FOR_STATE_TRANSFER);
            } else {
               // non-tx cache
               ctx = icf.createSingleKeyNonTxInvocationContext();
            }

            PutKeyValueCommand put = commandsFactory.buildPutKeyValueCommand(
                  e.getKey(), e.getValue(), e.getMetadata(), flags);

            boolean success = false;
            try {
               interceptorChain.invoke(ctx, put);
               success = true;
            } finally {
               if (ctx.isInTxScope()) {
                  if (success) {
                     try {
                        transactionManager.commit();
                     } catch (Throwable ex) {
                        log.errorf(ex, "Could not commit transaction created by state transfer of key %s", e.getKey());
                        if (transactionManager.getTransaction() != null) {
                           transactionManager.rollback();
                        }
                     }
                  } else {
                     transactionManager.rollback();
                  }
               }
            }
         } catch (Exception ex) {
            log.problemApplyingStateForKey(ex.getMessage(), e.getKey(), ex);
         }
      }
      log.tracef("Finished applying chunk of segment %d of cache %s", segmentId, cacheName);
   }

   private void applyTransactions(Address sender, Collection<TransactionInfo> transactions, int topologyId) {
      log.debugf("Applying %d transactions for cache %s transferred from node %s", transactions.size(), cacheName, sender);
      if (isTransactional) {
         for (TransactionInfo transactionInfo : transactions) {
            GlobalTransaction gtx = transactionInfo.getGlobalTransaction();
            // Mark the global transaction as remote. Only used for logging, hashCode/equals ignore it.
            gtx.setRemote(true);

            CacheTransaction tx = transactionTable.getLocalTransaction(gtx);
            if (tx == null) {
               tx = transactionTable.getRemoteTransaction(gtx);
               if (tx == null) {
                  tx = transactionTable.getOrCreateRemoteTransaction(gtx, transactionInfo.getModifications());
                  // Force this node to replay the given transaction data by making it think it is 1 behind
                  ((RemoteTransaction) tx).setLookedUpEntriesTopology(topologyId - 1);
               }
            }
            for (Object key : transactionInfo.getLockedKeys()) {
               tx.addBackupLockForKey(key);
            }
         }
      }
   }

   // Must run after the PersistenceManager
   @Start(priority = 20)
   public void start() {
      isFetchEnabled = configuration.clustering().stateTransfer().fetchInMemoryState() || configuration.persistence().fetchPersistentState();
      //rpc options does not changes in runtime. we can use always the same instance.
      rpcOptions = rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS)
            .timeout(timeout, TimeUnit.MILLISECONDS).build();
   }

   @Stop(priority = 20)
   @Override
   public void stop() {
      if (trace) {
         log.tracef("Shutting down StateConsumer of cache %s on node %s", cacheName, rpcManager.getAddress());
      }

      try {
         synchronized (transferMapsLock) {
            // cancel all inbound transfers
            taskQueue.clear();
            for (Iterator<List<InboundTransferTask>> it = transfersBySource.values().iterator(); it.hasNext(); ) {
               List<InboundTransferTask> inboundTransfers = it.next();
               it.remove();
               for (InboundTransferTask inboundTransfer : inboundTransfers) {
                  inboundTransfer.cancel();
               }
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

   private void addTransfers(Set<Integer> segments) {
      log.debugf("Adding inbound state transfer for segments %s of cache %s", segments, cacheName);

      // the set of nodes that reported errors when fetching data from them - these will not be retried in this topology
      Set<Address> excludedSources = new HashSet<Address>();

      // the sources and segments we are going to get from each source
      Map<Address, Set<Integer>> sources = new HashMap<Address, Set<Integer>>();

      if (isTransactional && !isTotalOrder) {
         requestTransactions(segments, sources, excludedSources);
      }

      if (isFetchEnabled) {
         requestSegments(segments, sources, excludedSources);
      }

      log.debugf("Finished adding inbound state transfer for segments %s of cache %s", segments, cacheName);
   }

   private void findSources(Set<Integer> segments, Map<Address, Set<Integer>> sources, Set<Address> excludedSources) {
      for (Integer segmentId : segments) {
         Address source = findSource(segmentId, excludedSources);
         // ignore all segments for which there are no other owners to pull data from.
         // these segments are considered empty (or lost) and do not require a state transfer
         if (source != null) {
            Set<Integer> segmentsFromSource = sources.get(source);
            if (segmentsFromSource == null) {
               segmentsFromSource = new HashSet<Integer>();
               sources.put(source, segmentsFromSource);
            }
            segmentsFromSource.add(segmentId);
         }
      }
   }

   private Address findSource(int segmentId, Set<Address> excludedSources) {
      List<Address> owners = cacheTopology.getReadConsistentHash().locateOwnersForSegment(segmentId);
      if (!owners.contains(rpcManager.getAddress())) {
         // We prefer that transactions are sourced from primary owners.
         // Needed in pessimistic mode, if the originator is the primary owner of the key than the lock
         // command is not replicated to the backup owners. See PessimisticDistributionInterceptor.acquireRemoteIfNeeded.
         for (int i = 0; i < owners.size(); i++) {
            Address o = owners.get(i);
            if (!o.equals(rpcManager.getAddress()) && !excludedSources.contains(o)) {
               return o;
            }
         }
         log.noLiveOwnersFoundForSegment(segmentId, cacheName, owners, excludedSources);
      }
      return null;
   }

   private void requestTransactions(Set<Integer> segments, Map<Address, Set<Integer>> sources, Set<Address> excludedSources) {
      findSources(segments, sources, excludedSources);

      boolean seenFailures = false;
      while (true) {
         Set<Integer> failedSegments = new HashSet<Integer>();
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
               log.failedToRetrieveTransactionsForSegments(segments, cacheName, source, e);
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
                                                                                  rpcManager.getAddress(), 0, null);
               Map<Address, Response> responses = rpcManager.invokeRemotely(Collections.singleton(source), cmd, rpcOptions);
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
      log.trace("Unable to acquire cluster listeners from other members, assuming none are present");
      return Collections.emptySet();
   }

   private Response getTransactions(Address source, Set<Integer> segments, int topologyId) {
      if (trace) {
         log.tracef("Requesting transactions for segments %s of cache %s from node %s", segments, cacheName, source);
      }
      // get transactions and locks
      StateRequestCommand cmd = commandsFactory.buildStateRequestCommand(StateRequestCommand.Type.GET_TRANSACTIONS, rpcManager.getAddress(), topologyId, segments);
      Map<Address, Response> responses = rpcManager.invokeRemotely(Collections.singleton(source), cmd, rpcOptions);
      return responses.get(source);
   }

   private void requestSegments(Set<Integer> segments, Map<Address, Set<Integer>> sources, Set<Address> excludedSources) {
      if (sources.isEmpty()) {
         findSources(segments, sources, excludedSources);
      }

      for (Map.Entry<Address, Set<Integer>> e : sources.entrySet()) {
         addTransfer(e.getKey(), e.getValue());
      }

      startTransferThread(excludedSources);
   }

   private void startTransferThread(final Set<Address> excludedSources) {
      boolean success = isTransferThreadRunning.compareAndSet(false, true);
      if (trace) log.tracef("Starting transfer thread: %b", success);

      if (success) {
         executorService.submit(new Runnable() {
            @Override
            public void run() {
               runTransferTasksInOrder(excludedSources);
            }
         });
      }
   }

   private void runTransferTasksInOrder(Set<Address> excludedSources) {
      while (true) {
         InboundTransferTask task;
         task = taskQueue.pollFirst();
         if (task == null) {
            isTransferThreadRunning.set(false);

            if (!taskQueue.isEmpty() && isTransferThreadRunning.compareAndSet(false, true)) {
               // We found a new entry in the queue, and another transfer thread hasn't
               // been started yet. Keep this thread alive.
               continue;
            } else {
               if (trace) log.tracef("Stopping state transfer thread");
               break;
            }
         }

         try {
            boolean successful = task.requestSegments();
            if (successful) {
               successful = task.awaitCompletion();
            }

            if (!successful) {
               retryTransferTask(task, excludedSources);
            }
         } catch(InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
         } catch (Throwable t) {
            log.failedToRequestSegments(task.getSegments(), cacheName, task.getSource(), t);
         }
      }
   }


   private void retryTransferTask(InboundTransferTask task, Set<Address> excludedSources) {
      if (trace) log.tracef("Retrying failed task: %s", task);

      // look for other sources for the failed segments and replace all failed tasks with new tasks to be retried
      // remove+add needs to be atomic
      synchronized (transferMapsLock) {
         Set<Integer> failedSegments = new HashSet<Integer>();
         if (removeTransfer(task)) {
            excludedSources.add(task.getSource());
            failedSegments.addAll(task.getSegments());
         }

         // should re-add only segments we still own and are not already in
         failedSegments.retainAll(getOwnedSegments(cacheTopology.getWriteConsistentHash()));

         Map<Address, Set<Integer>> sources = new HashMap<Address, Set<Integer>>();
         findSources(failedSegments, sources, excludedSources);
         for (Map.Entry<Address, Set<Integer>> e : sources.entrySet()) {
            addTransfer(e.getKey(), e.getValue());
         }
      }
   }

   /**
    * Cancel transfers for segments we no longer own.
    *
    * @param removedSegments segments to be cancelled
    */
   private void cancelTransfers(Set<Integer> removedSegments) {
      synchronized (transferMapsLock) {
         List<Integer> segmentsToCancel = new ArrayList<Integer>(removedSegments);
         while (!segmentsToCancel.isEmpty()) {
            int segmentId = segmentsToCancel.remove(0);
            InboundTransferTask inboundTransfer = transfersBySegment.get(segmentId);
            if (inboundTransfer != null) { // we need to check the transfer was not already completed
               Set<Integer> cancelledSegments = new HashSet<Integer>(removedSegments);
               cancelledSegments.retainAll(inboundTransfer.getSegments());
               segmentsToCancel.removeAll(cancelledSegments);
               transfersBySegment.keySet().removeAll(cancelledSegments);
               //this will also remove it from transfersBySource if the entire task gets cancelled
               inboundTransfer.cancelSegments(cancelledSegments);
            }
         }
      }
   }

   private Set<Integer> computeStaleL1Segments(Set<Integer> newSegments, ConsistentHash newWriteCh, ConsistentHash previousWriteCh) {
      // The actual owners keep track of the nodes that hold a key in L1 ("requestors") and
      // they invalidate the key on every requestor after a change.
      // But this information is only present on the owners where the ClusteredGetKeyValueCommand
      // got executed - if the requestor only contacted one owner, and that node is no longer an owner
      // (perhaps because it left the cluster), the other owners will not know to invalidate the key
      // on that requestor. Furthermore, the requestors list is not copied to the new owners during
      // state transfers.
      // To compensate for this, we delete all L1 entries in segments that changed ownership during
      // this topology update. We can't actually differentiate between L1 entries and regular entries,
      // so we delete all entries that don't belong to this node in the current OR previous topology.
      Set<Integer> segmentsToInvalidate = new HashSet<Integer>();
      if (configuration.clustering().l1().enabled() && previousWriteCh != null) {
         for (int i = 0; i < previousWriteCh.getNumSegments(); i++) {
            if (newSegments.contains(i))
               continue;

            if (!previousWriteCh.locateOwnersForSegment(i).containsAll(newWriteCh.locateOwnersForSegment(i))) {
               segmentsToInvalidate.add(i);
            }
         }
      }
      return segmentsToInvalidate;
   }

   private void removeStaleData(final Set<Integer> removedSegments, final Set<Integer> staleL1Segments) throws InterruptedException {
      if (keyInvalidationListener != null) {
         keyInvalidationListener.beforeInvalidation(removedSegments, staleL1Segments);
      }

      if (removedSegments.isEmpty() && staleL1Segments.isEmpty())
         return;

      // Keys that might have been in L1, and need to be removed from the data container
      final ConcurrentHashSet<Object> keysToInvalidate = new ConcurrentHashSet<Object>();
      // Keys that we used to own, and need to be removed from the data container AND the cache stores
      final ConcurrentHashSet<Object> keysToRemove = new ConcurrentHashSet<Object>();

      dataContainer.executeTask(KeyFilter.LOAD_ALL_FILTER, new ParallelIterableMap.KeyValueAction<Object, InternalCacheEntry<Object, Object>>() {
         @Override
         public void apply(Object o, InternalCacheEntry<Object, Object> ice) {
            Object key = ice.getKey();
            int keySegment = getSegment(key);
            if (removedSegments.contains(keySegment)) {
               keysToRemove.add(key);
            } else if (staleL1Segments.contains(keySegment)) {
               keysToInvalidate.add(key);
            }
         }
      });

      // gather all keys from cache store that belong to the segments that are being removed/moved to L1
      if (!removedSegments.isEmpty()) {
         try {
            KeyFilter filter = new KeyFilter() {
               @Override
               public boolean accept(Object key) {
                  if (dataContainer.containsKey(key))
                     return false;
                  int keySegment = getSegment(key);
                  return (removedSegments.contains(keySegment));
               }
            };
            persistenceManager.processOnAllStores(filter, new AdvancedCacheLoader.CacheLoaderTask() {
               @Override
               public void processEntry(MarshalledEntry marshalledEntry, AdvancedCacheLoader.TaskContext taskContext) throws InterruptedException {
                  keysToRemove.add(marshalledEntry.getKey());
               }
            }, false, false, PRIVATE);
         } catch (CacheException e) {
            log.failedLoadingKeysFromCacheStore(e);
         }
      }

      log.debugf("Removing %d stale L1 entries for segments %s of cache %s", keysToInvalidate.size(), staleL1Segments, cacheName);
      if (!keysToInvalidate.isEmpty()) {
         try {
            InvalidateCommand invalidateCmd = commandsFactory.buildInvalidateFromL1Command(EnumSet.of(CACHE_MODE_LOCAL, SKIP_LOCKING), keysToInvalidate);
            InvocationContext ctx = icf.createNonTxInvocationContext();
            interceptorChain.invoke(ctx, invalidateCmd);

            log.debugf("Removed %d stale L1 entries, data container now has %d keys", keysToInvalidate.size(), dataContainer.size());
            if (trace) log.tracef("Removed stale L1 entries: %s", keysToInvalidate);
         } catch (CacheException e) {
            log.failedToInvalidateKeys(e);
         }
      }

      log.debugf("Removing %s no longer owned entries for segments %s of cache %s", keysToRemove.size(), removedSegments, cacheName);
      if (!keysToRemove.isEmpty()) {
         try {
            InvalidateCommand invalidateCmd = commandsFactory.buildInvalidateCommand(EnumSet.of(CACHE_MODE_LOCAL, SKIP_LOCKING), keysToRemove.toArray());
            InvocationContext ctx = icf.createNonTxInvocationContext();
            interceptorChain.invoke(ctx, invalidateCmd);

            log.debugf("Removed %d keys, data container now has %d keys", keysToRemove.size(), dataContainer.size());
            if (trace) log.tracef("Removed keys: %s", keysToRemove);
         } catch (CacheException e) {
            log.failedToInvalidateKeys(e);
         }
      }
   }

   /**
    * Check if any of the existing transfers should be restarted from a different source because the initial source is no longer a member.
    *
    * @param cacheTopology
    * @param addedSegments
    */
   private void restartBrokenTransfers(CacheTopology cacheTopology, Set<Integer> addedSegments) {
      Set<Address> members = new HashSet<Address>(cacheTopology.getReadConsistentHash().getMembers());
      synchronized (transferMapsLock) {
         for (Iterator<Address> it = transfersBySource.keySet().iterator(); it.hasNext(); ) {
            Address source = it.next();
            if (!members.contains(source)) {
               if (trace) {
                  log.tracef("Removing inbound transfers from source %s for cache %s", source, cacheName);
               }
               List<InboundTransferTask> inboundTransfers = transfersBySource.get(source);
               it.remove();
               for (InboundTransferTask inboundTransfer : inboundTransfers) {
                  // these segments will be restarted if they are still in new write CH
                  if (trace) {
                     log.tracef("Removing inbound transfers for segments %s from source %s for cache %s", inboundTransfer.getSegments(), source, cacheName);
                  }
                  taskQueue.remove(inboundTransfer);
                  inboundTransfer.terminate();
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
      return cacheTopology.getReadConsistentHash().getSegment(key);
   }

   private InboundTransferTask addTransfer(Address source, Set<Integer> segmentsFromSource) {
      synchronized (transferMapsLock) {
         log.tracef("Adding transfer from %s for segments %s", source, segmentsFromSource);
         segmentsFromSource.removeAll(transfersBySegment.keySet());  // already in progress segments are excluded
         if (!segmentsFromSource.isEmpty()) {
            InboundTransferTask inboundTransfer = new InboundTransferTask(segmentsFromSource, source,
                  cacheTopology.getTopologyId(), this, rpcManager, commandsFactory, timeout, cacheName);
            for (int segmentId : segmentsFromSource) {
               transfersBySegment.put(segmentId, inboundTransfer);
            }
            List<InboundTransferTask> inboundTransfers = transfersBySource.get(inboundTransfer.getSource());
            if (inboundTransfers == null) {
               inboundTransfers = new ArrayList<InboundTransferTask>();
               transfersBySource.put(inboundTransfer.getSource(), inboundTransfers);
            }
            inboundTransfers.add(inboundTransfer);
            taskQueue.add(inboundTransfer);
            return inboundTransfer;
         } else {
            return null;
         }
      }
   }

   private boolean removeTransfer(InboundTransferTask inboundTransfer) {
      synchronized (transferMapsLock) {
         log.tracef("Removing inbound transfers for segments %s from source %s for cache %s",
               inboundTransfer.getSegments(), inboundTransfer.getSource(), cacheName);
         taskQueue.remove(inboundTransfer);
         List<InboundTransferTask> transfers = transfersBySource.get(inboundTransfer.getSource());
         if (transfers != null) {
            if (transfers.remove(inboundTransfer)) {
               if (transfers.isEmpty()) {
                  transfersBySource.remove(inboundTransfer.getSource());
               }
               transfersBySegment.keySet().removeAll(inboundTransfer.getSegments());
               return true;
            }
         }
      }
      return false;
   }

   void onTaskCompletion(InboundTransferTask inboundTransfer) {
      log.tracef("Completion of inbound transfer task: %s ", inboundTransfer);
      removeTransfer(inboundTransfer);

      notifyEndOfRebalanceIfNeeded(cacheTopology.getTopologyId(), cacheTopology.getRebalanceId());
   }

   public interface KeyInvalidationListener {
      void beforeInvalidation(Set<Integer> removedSegments, Set<Integer> staleL1Segments);
   }
}
