/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.totalorder.TotalOrderLatch;
import org.infinispan.transaction.totalorder.TotalOrderManager;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.ReadOnlyDataContainerBackedKeySet;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.infinispan.context.Flag.*;
import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;

/**
 * {@link StateConsumer} implementation.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class StateConsumerImpl implements StateConsumer {

   private static final Log log = LogFactory.getLog(StateConsumerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   private ExecutorService executorService;
   private StateTransferManager stateTransferManager;
   private String cacheName;
   private Configuration configuration;
   private RpcManager rpcManager;
   private TransactionManager transactionManager;   // optional
   private CommandsFactory commandsFactory;
   private TransactionTable transactionTable;       // optional
   private DataContainer dataContainer;
   private CacheLoaderManager cacheLoaderManager;
   private InterceptorChain interceptorChain;
   private InvocationContextContainer icc;
   private StateTransferLock stateTransferLock;
   private CacheNotifier cacheNotifier;
   private TotalOrderManager totalOrderManager;
   private long timeout;
   private boolean useVersionedPut;
   private boolean isFetchEnabled;
   private boolean isTransactional;
   private boolean isTotalOrder;

   private volatile CacheTopology cacheTopology;

   /**
    * Keeps track of all keys updated by user code during state transfer. If this is null no keys are being recorded and
    * state transfer is not allowed to update anything. This can be null if there is not state transfer in progress at
    * the moment of there is one but a ClearCommand was encountered.
    */
   private volatile Set<Object> updatedKeys;

   /**
    * The number of topology updates that are being processed concurrently (in method onTopologyUpdate()).
    * This is needed to be able to detect completion.
    */
   private final AtomicInteger activeTopologyUpdates = new AtomicInteger(0);

   /**
    * Indicates if there is a rebalance in progress.
    */
   private final AtomicBoolean rebalanceInProgress = new AtomicBoolean(false);

   /**
    * Indicates if there is a rebalance in progress and there the local node has not yet received
    * all the new segments yet.
    */
   private final AtomicBoolean waitingForState = new AtomicBoolean(false);

   /**
    * A map that keeps track of current inbound state transfers by source address. There could be multiple transfers
    * flowing in from the same source (but for different segments) so the values are lists. This works in tandem with
    * transfersBySegment so they always need to be kept in sync and updates to both of them need to be atomic.
    */
   private final Map<Address, List<InboundTransferTask>> transfersBySource = new HashMap<Address, List<InboundTransferTask>>();

   /**
    * A map that keeps track of current inbound state transfers by segment id. There is at most one transfers per segment.
    * This works in tandem with transfersBySource so they always need to be kept in sync and updates to both of them
    * need to be atomic.
    */
   private final Map<Integer, InboundTransferTask> transfersBySegment = new HashMap<Integer, InboundTransferTask>();

   /**
    * Tasks ready to be executed by the transfer thread. These tasks are also present if transfersBySegment and transfersBySource.
    */
   private final BlockingDeque<InboundTransferTask> taskQueue = new LinkedBlockingDeque<InboundTransferTask>();

   private boolean isTransferThreadRunning;

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
      updatedKeys = null;
   }

   /**
    * Receive notification of updated keys right before they are committed in DataContainer.
    *
    * @param key the key that is being modified
    */
   @Override
   public void addUpdatedKey(Object key) {
      // grab a copy of the reference to prevent issues if another thread calls stopApplyingState() between null check and actual usage
      final Set<Object> localUpdatedKeys = updatedKeys;
      if (localUpdatedKeys != null) {
         if (cacheTopology.getWriteConsistentHash().isKeyLocalToNode(rpcManager.getAddress(), key)) {
            localUpdatedKeys.add(key);
         }
      }
   }

   /**
    * Checks if a given key was updated by user code during state transfer (and consequently it is untouchable by state transfer).
    *
    * @param key the key to check
    * @return true if the key is known to be modified, false otherwise
    */
   @Override
   public boolean isKeyUpdated(Object key) {
      // grab a copy of the reference to prevent issues if another thread calls stopApplyingState() between null check and actual usage
      final Set<Object> localUpdatedKeys = updatedKeys;
      return localUpdatedKeys == null || localUpdatedKeys.contains(key);
   }

   @Inject
   public void init(Cache cache,
                    @ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService executorService, //TODO Use a dedicated ExecutorService
                    StateTransferManager stateTransferManager,
                    InterceptorChain interceptorChain,
                    InvocationContextContainer icc,
                    Configuration configuration,
                    RpcManager rpcManager,
                    TransactionManager transactionManager,
                    CommandsFactory commandsFactory,
                    CacheLoaderManager cacheLoaderManager,
                    DataContainer dataContainer,
                    TransactionTable transactionTable,
                    StateTransferLock stateTransferLock,
                    CacheNotifier cacheNotifier,
                    TotalOrderManager totalOrderManager) {
      this.cacheName = cache.getName();
      this.executorService = executorService;
      this.stateTransferManager = stateTransferManager;
      this.interceptorChain = interceptorChain;
      this.icc = icc;
      this.configuration = configuration;
      this.rpcManager = rpcManager;
      this.transactionManager = transactionManager;
      this.commandsFactory = commandsFactory;
      this.cacheLoaderManager = cacheLoaderManager;
      this.dataContainer = dataContainer;
      this.transactionTable = transactionTable;
      this.stateTransferLock = stateTransferLock;
      this.cacheNotifier = cacheNotifier;
      this.totalOrderManager = totalOrderManager;

      isTransactional = configuration.transaction().transactionMode().isTransactional();
      isTotalOrder = configuration.transaction().transactionProtocol().isTotalOrder();

      // we need to use a special form of PutKeyValueCommand that can apply versions too
      useVersionedPut = isTransactional &&
            Configurations.isVersioningEnabled(configuration) &&
            configuration.clustering().cacheMode().isClustered();

      timeout = configuration.clustering().stateTransfer().timeout();
   }

   public boolean hasActiveTransfers() {
      synchronized (this) {
         return !transfersBySource.isEmpty();
      }
   }

   @Override
   public boolean isStateTransferInProgress() {
      return rebalanceInProgress.get();
   }

   @Override
   public boolean isStateTransferInProgressForKey(Object key) {
      if (configuration.clustering().cacheMode().isInvalidation()) {
         return false;
      }
      synchronized (this) {
         CacheTopology localCacheTopology = cacheTopology;
         if (localCacheTopology == null || localCacheTopology.getPendingCH() == null)
            return false;
         Address address = rpcManager.getAddress();
         boolean keyWillBeLocal = localCacheTopology.getPendingCH().isKeyLocalToNode(address, key);
         boolean keyIsLocal = localCacheTopology.getCurrentCH().isKeyLocalToNode(address, key);
         return keyWillBeLocal && !keyIsLocal;
      }
   }

   @Override
   public boolean ownsData() {
      return ownsData;
   }

   @Override
   public void onTopologyUpdate(CacheTopology cacheTopology, boolean isRebalance) {
      if (trace) log.tracef("Received new topology for cache %s, isRebalance = %b, topology = %s", cacheName, isRebalance, cacheTopology);

      activeTopologyUpdates.incrementAndGet();
      if (isRebalance) {
         if (!ownsData && cacheTopology.getMembers().contains(rpcManager.getAddress())) {
            ownsData = true;
         }
         rebalanceInProgress.set(true);
         waitingForState.set(true);
         cacheNotifier.notifyDataRehashed(cacheTopology.getCurrentCH(), cacheTopology.getPendingCH(),
               cacheTopology.getTopologyId(), true);

         //in total order, we should wait for remote transactions before proceeding
         if (configuration.transaction().transactionProtocol().isTotalOrder()) {
            if (log.isTraceEnabled()) {
               log.trace("State Transfer in Total Order cache. Waiting for remote transactions to finish");
            }
            try {
               for (TotalOrderLatch block : totalOrderManager.notifyStateTransferStart(cacheTopology.getTopologyId())) {
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

         if (log.isTraceEnabled()) {
            log.tracef("Lock State Transfer in Progress for topology ID %s", cacheTopology.getTopologyId());
         }
      } else {
         if (cacheTopology.getMembers().size() == 1 && cacheTopology.getMembers().get(0).equals(rpcManager.getAddress())) {
            //we are the first member in the cache...
            ownsData = true;
         }
      }
      final ConsistentHash previousReadCh = this.cacheTopology != null ? this.cacheTopology.getReadConsistentHash() : null;
      final ConsistentHash previousWriteCh = this.cacheTopology != null ? this.cacheTopology.getWriteConsistentHash() : null;
      // Ensures writes to the data container use the right consistent hash
      // No need for a try/finally block, since it's just an assignment
      stateTransferLock.acquireExclusiveTopologyLock();
      this.cacheTopology = cacheTopology;
      if (isRebalance) {
         updatedKeys = new ConcurrentHashSet<Object>();
      }
      stateTransferLock.releaseExclusiveTopologyLock();
      stateTransferLock.notifyTopologyInstalled(cacheTopology.getTopologyId());

      try {
         // fetch transactions and data segments from other owners if this is enabled
         if (isTransactional || isFetchEnabled) {
            Set<Integer> addedSegments;
            if (previousWriteCh == null) {
               // we start fresh, without any data, so we need to pull everything we own according to writeCh

               addedSegments = getOwnedSegments(cacheTopology.getWriteConsistentHash());

               if (trace) {
                  log.tracef("On cache %s we have: added segments: %s", cacheName, addedSegments);
               }
            } else {
               Set<Integer> previousSegments = getOwnedSegments(previousWriteCh);
               Set<Integer> newSegments = getOwnedSegments(cacheTopology.getWriteConsistentHash());

               Set<Integer> removedSegments = new HashSet<Integer>(previousSegments);
               removedSegments.removeAll(newSegments);

               // This is a rebalance, we need to request the segments we own in the new CH.
               addedSegments = new HashSet<Integer>(newSegments);
               addedSegments.removeAll(previousSegments);

               if (trace) {
                  log.tracef("On cache %s we have: removed segments: %s; new segments: %s; old segments: %s; added segments: %s",
                        cacheName, removedSegments, newSegments, previousSegments, addedSegments);
               }

               // remove inbound transfers for segments we no longer own
               cancelTransfers(removedSegments);

               // any data for segments we no longer own should be removed from data container and cache store or moved to L1 if enabled
               // If L1.onRehash is enabled, "removed" segments are actually moved to L1. The new (and old) owners
               // will automatically add the nodes that no longer own a key to that key's requestors list.
               invalidateSegments(newSegments, removedSegments);

               // check if any of the existing transfers should be restarted from a different source because the initial source is no longer a member
               Set<Address> members = new HashSet<Address>(cacheTopology.getReadConsistentHash().getMembers());
               synchronized (this) {
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

            if (!addedSegments.isEmpty()) {
               addTransfers(addedSegments);  // add transfers for new or restarted segments
            }
         }

         log.tracef("Topology update processed, rebalanceInProgress = %s, isRebalance = %s, pending CH = %s",
               rebalanceInProgress.get(), isRebalance, cacheTopology.getPendingCH());
         if (rebalanceInProgress.get()) {
            // there was a rebalance in progress
            if (!isRebalance && cacheTopology.getPendingCH() == null) {
               // we have received a topology update without a pending CH, signalling the end of the rebalance
               boolean changed = rebalanceInProgress.compareAndSet(true, false);
               if (changed) {
                  // if the coordinator changed, we might get two concurrent topology updates,
                  // but we only want to notify the @DataRehashed listeners once
                  cacheNotifier.notifyDataRehashed(previousReadCh, cacheTopology.getCurrentCH(),
                        cacheTopology.getTopologyId(), false);
                  if (log.isTraceEnabled()) {
                     log.tracef("Unlock State Transfer in Progress for topology ID %s", cacheTopology.getTopologyId());
                  }
                  totalOrderManager.notifyStateTransferEnd();
               }
            }
         }
      } finally {
         stateTransferLock.notifyTransactionDataReceived(cacheTopology.getTopologyId());

         if (activeTopologyUpdates.decrementAndGet() == 0) {
            notifyEndOfTopologyUpdate(cacheTopology.getTopologyId());
         }

         // Remove the transactions whose originators have left the cache.
         // Need to do it now, after we have applied any transactions from other nodes,
         // and after notifyTransactionDataReceived - otherwise the RollbackCommands would block.
         if (transactionTable != null) {
            transactionTable.cleanupStaleTransactions(cacheTopology);
         }
      }
   }

   private void notifyEndOfTopologyUpdate(int topologyId) {
      if (!hasActiveTransfers()) {
         if (waitingForState.compareAndSet(true, false)) {
            log.debugf("Finished receiving of segments for cache %s for topology %d.", cacheName, topologyId);
            stopApplyingState();
            stateTransferManager.notifyEndOfRebalance(topologyId);
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
      // ignore responses received after we are no longer a member
      if (!wCh.getMembers().contains(rpcManager.getAddress())) {
         if (trace) {
            log.tracef("Ignoring received state because we are no longer a member");
         }

         return;
      }

      if (trace) {
         log.tracef("Before applying the received state the data container of cache %s has %d keys", cacheName, dataContainer.size());
      }

      for (StateChunk stateChunk : stateChunks) {
         // it's possible to receive a late message so we must be prepared to ignore segments we no longer own
         //todo [anistor] this check should be based on topologyId
         if (!wCh.getSegmentsForOwner(rpcManager.getAddress()).contains(stateChunk.getSegmentId())) {
            log.warnf("Discarding received cache entries for segment %d of cache %s because they do not belong to this node.", stateChunk.getSegmentId(), cacheName);
            continue;
         }

         // notify the inbound task that a chunk of cache entries was received
         InboundTransferTask inboundTransfer;
         synchronized (this) {
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
         synchronized (this) {
            log.tracef("Segments not received yet for cache %s: %s", cacheName, transfersBySource);
         }
      }
   }

   private void doApplyState(Address sender, int segmentId, Collection<InternalCacheEntry> cacheEntries) {
      log.debugf("Applying new state for segment %d of cache %s from node %s: received %d cache entries", segmentId, cacheName, sender, cacheEntries.size());
      if (trace) {
         List<Object> keys = new ArrayList<Object>(cacheEntries.size());
         for (InternalCacheEntry e : cacheEntries) {
            keys.add(e.getKey());
         }
         log.tracef("Received keys %s for segment %d of cache %s from node %s", keys, segmentId, cacheName, sender);
      }

      // CACHE_MODE_LOCAL avoids handling by StateTransferInterceptor and any potential locks in StateTransferLock
      EnumSet<Flag> flags = EnumSet.of(PUT_FOR_STATE_TRANSFER, CACHE_MODE_LOCAL, IGNORE_RETURN_VALUES, SKIP_REMOTE_LOOKUP, SKIP_SHARED_CACHE_STORE, SKIP_OWNERSHIP_CHECK, SKIP_XSITE_BACKUP);
      for (InternalCacheEntry e : cacheEntries) {
         try {
            InvocationContext ctx;
            if (transactionManager != null) {
               // cache is transactional
               transactionManager.begin();
               Transaction transaction = transactionManager.getTransaction();
               ctx = icc.createInvocationContext(transaction);
               ((TxInvocationContext) ctx).setImplicitTransaction(true);
            } else {
               // non-tx cache
               ctx = icc.createSingleKeyNonTxInvocationContext();
            }

            PutKeyValueCommand put = useVersionedPut ?
                  commandsFactory.buildVersionedPutKeyValueCommand(e.getKey(), e.getValue(), e.getLifespan(), e.getMaxIdle(), e.getVersion(), flags)
                  : commandsFactory.buildPutKeyValueCommand(e.getKey(), e.getValue(), e.getLifespan(), e.getMaxIdle(), flags);

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
      log.debugf("Finished applying state for segment %d of cache %s", segmentId, cacheName);
   }

   private void applyTransactions(Address sender, Collection<TransactionInfo> transactions) {
      log.debugf("Applying %d transactions for cache %s transferred from node %s", transactions.size(), cacheName, sender);
      if (isTransactional) {
         for (TransactionInfo transactionInfo : transactions) {
            CacheTransaction tx = transactionTable.getLocalTransaction(transactionInfo.getGlobalTransaction());
            if (tx == null) {
               tx = transactionTable.getRemoteTransaction(transactionInfo.getGlobalTransaction());
               if (tx == null) {
                  tx = transactionTable.getOrCreateRemoteTransaction(transactionInfo.getGlobalTransaction(), transactionInfo.getModifications());
                  ((RemoteTransaction) tx).setMissingLookedUpEntries(true);
               }
            }
            for (Object key : transactionInfo.getLockedKeys()) {
               tx.addBackupLockForKey(key);
            }
         }
      }
   }

   // Must run after the CacheLoaderManager
   @Start(priority = 20)
   public void start() {
      isFetchEnabled = configuration.clustering().stateTransfer().fetchInMemoryState() || cacheLoaderManager.isFetchPersistentState();
      //rpc options does not changes in runtime. we can use always the same instance.
      rpcOptions = rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS)
            .timeout(timeout, TimeUnit.MILLISECONDS).build();
   }

   @Stop(priority = 20)
   @Override
   public void stop() {
      if (trace) {
         log.tracef("Shutting down StateConsumer of cache %s on node %s", cacheName, rpcManager.getAddress());
      }

      try {
         synchronized (this) {
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
      if (owners.size() == 1 && owners.get(0).equals(rpcManager.getAddress())) {
         return null;
      }

      for (int i = owners.size() - 1; i >= 0; i--) {   // iterate backwards because we prefer to fetch from newer nodes
         Address o = owners.get(i);
         if (!o.equals(rpcManager.getAddress()) && !excludedSources.contains(o)) {
            return o;
         }
      }
      log.noLiveOwnersFoundForSegment(segmentId, cacheName, owners, excludedSources);
      return null;
   }

   private void requestTransactions(Set<Integer> segments, Map<Address, Set<Integer>> sources, Set<Address> excludedSources) {
      findSources(segments, sources, excludedSources);

      boolean seenFailures = false;
      while (true) {
         Set<Integer> failedSegments = new HashSet<Integer>();
         for (Map.Entry<Address, Set<Integer>> e : sources.entrySet()) {
            Address source = e.getKey();
            Set<Integer> segmentsFromSource = e.getValue();
            List<TransactionInfo> transactions = getTransactions(source, segmentsFromSource, cacheTopology.getTopologyId());
            if (transactions != null) {
               applyTransactions(source, transactions);
            } else {
               // if requesting the transactions failed we need to retry from another source
               failedSegments.addAll(segmentsFromSource);
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

   private List<TransactionInfo> getTransactions(Address source, Set<Integer> segments, int topologyId) {
      if (trace) {
         log.tracef("Requesting transactions for segments %s of cache %s from node %s", segments, cacheName, source);
      }
      // get transactions and locks
      try {
         StateRequestCommand cmd = commandsFactory.buildStateRequestCommand(StateRequestCommand.Type.GET_TRANSACTIONS, rpcManager.getAddress(), topologyId, segments);
         Map<Address, Response> responses = rpcManager.invokeRemotely(Collections.singleton(source), cmd, rpcOptions);
         Response response = responses.get(source);
         if (response instanceof SuccessfulResponse) {
            return (List<TransactionInfo>) ((SuccessfulResponse) response).getResponseValue();
         }
         log.failedToRetrieveTransactionsForSegments(segments, cacheName, source, null);
      } catch (CacheException e) {
         log.failedToRetrieveTransactionsForSegments(segments, cacheName, source, e);
      }
      return null;
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
      synchronized (this) {
         if (isTransferThreadRunning) {
            return;
         }
         isTransferThreadRunning = true;
      }

      executorService.submit(new Runnable() {
         @Override
         public void run() {
            try {
               while (true) {
                  List<InboundTransferTask> failedTasks = new ArrayList<InboundTransferTask>();
                  while (true) {
                     InboundTransferTask task;
                     try {
                        task = taskQueue.pollFirst(200, TimeUnit.MILLISECONDS);
                        if (task == null) {
                           break;
                        }
                     } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                     }

                     if (!task.requestSegments()) {
                        // if requesting the segments failed we'll take care of it later
                        failedTasks.add(task);
                     } else {
                        try {
                           if (!task.awaitCompletion()) {
                              failedTasks.add(task);
                           }
                        } catch (InterruptedException e) {
                           Thread.currentThread().interrupt();
                           return;
                        }
                     }
                  }

                  if (failedTasks.isEmpty() && taskQueue.isEmpty()) {
                     break;
                  }

                  log.tracef("Retrying %d failed tasks", failedTasks.size());

                  // look for other sources for the failed segments and replace all failed tasks with new tasks to be retried
                  // remove+add needs to be atomic
                  synchronized (StateConsumerImpl.this) {
                     Set<Integer> failedSegments = new HashSet<Integer>();
                     for (InboundTransferTask task : failedTasks) {
                        if (removeTransfer(task)) {
                           excludedSources.add(task.getSource());
                           failedSegments.addAll(task.getSegments());
                        }
                     }

                     // should re-add only segments we still own and are not already in
                     failedSegments.retainAll(getOwnedSegments(cacheTopology.getWriteConsistentHash()));
                     failedSegments.removeAll(getOwnedSegments(cacheTopology.getReadConsistentHash()));

                     Map<Address, Set<Integer>> sources = new HashMap<Address, Set<Integer>>();
                     findSources(failedSegments, sources, excludedSources);
                     for (Map.Entry<Address, Set<Integer>> e : sources.entrySet()) {
                        addTransfer(e.getKey(), e.getValue());
                     }
                  }
               }
            } finally {
               synchronized (StateConsumerImpl.this) {
                  isTransferThreadRunning = false;
               }
            }
         }
      });
   }

   /**
    * Cancel transfers for segments we no longer own.
    *
    * @param removedSegments segments to be cancelled
    */
   private void cancelTransfers(Set<Integer> removedSegments) {
      synchronized (this) {
         List<Integer> segmentsToCancel = new ArrayList<Integer>(removedSegments);
         while (!segmentsToCancel.isEmpty()) {
            int segmentId = segmentsToCancel.remove(0);
            InboundTransferTask inboundTransfer = transfersBySegment.remove(segmentId);
            if (inboundTransfer != null) { // we need to check the transfer was not already completed
               Set<Integer> cancelledSegments = new HashSet<Integer>(removedSegments);
               cancelledSegments.retainAll(inboundTransfer.getSegments());
               segmentsToCancel.removeAll(cancelledSegments);
               inboundTransfer.cancelSegments(cancelledSegments);   //this will also remove it from transfersBySource if the entire task gets cancelled
            }
         }
      }
   }

   private void invalidateSegments(Set<Integer> newSegments, Set<Integer> segmentsToL1) {
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
      Set<Object> keysToL1 = new HashSet<Object>();
      Set<Object> keysToRemove = new HashSet<Object>();

      // gather all keys from data container that belong to the segments that are being removed/moved to L1
      for (InternalCacheEntry ice : dataContainer) {
         Object key = ice.getKey();
         int keySegment = getSegment(key);
         if (segmentsToL1.contains(keySegment)) {
            keysToL1.add(key);
         } else if (!newSegments.contains(keySegment)) {
            keysToRemove.add(key);
         }
      }

      // gather all keys from cache store that belong to the segments that are being removed/moved to L1
      CacheStore cacheStore = getCacheStore();
      if (cacheStore != null) {
         //todo [anistor] extend CacheStore interface to be able to specify a filter when loading keys (ie. keys should belong to desired segments)
         try {
            Set<Object> storedKeys = cacheStore.loadAllKeys(new ReadOnlyDataContainerBackedKeySet(dataContainer));
            for (Object key : storedKeys) {
               int keySegment = getSegment(key);
               if (segmentsToL1.contains(keySegment)) {
                  keysToL1.add(key);
               } else if (!newSegments.contains(keySegment)) {
                  keysToRemove.add(key);
               }
            }

         } catch (CacheLoaderException e) {
            log.failedLoadingKeysFromCacheStore(e);
         }
      }

      if (configuration.clustering().l1().onRehash()) {
         log.debugf("Moving to L1 state for segments %s of cache %s", segmentsToL1, cacheName);
      } else {
         log.debugf("Removing state for segments %s of cache %s", segmentsToL1, cacheName);
      }
      if (!keysToL1.isEmpty()) {
         try {
            InvalidateCommand invalidateCmd = commandsFactory.buildInvalidateFromL1Command(true, EnumSet.of(CACHE_MODE_LOCAL, SKIP_LOCKING), keysToL1);
            InvocationContext ctx = icc.createNonTxInvocationContext();
            interceptorChain.invoke(ctx, invalidateCmd);

            log.debugf("Invalidated %d keys, data container now has %d keys", keysToL1.size(), dataContainer.size());
            if (trace) log.tracef("Invalidated keys: %s", keysToL1);
         } catch (CacheException e) {
            log.failedToInvalidateKeys(e);
         }
      }

      log.debugf("Removing L1 state for segments not in %s or %s for cache %s", newSegments, segmentsToL1, cacheName);
      if (!keysToRemove.isEmpty()) {
         try {
            InvalidateCommand invalidateCmd = commandsFactory.buildInvalidateFromL1Command(false, EnumSet.of(CACHE_MODE_LOCAL, SKIP_LOCKING), keysToRemove);
            InvocationContext ctx = icc.createNonTxInvocationContext();
            interceptorChain.invoke(ctx, invalidateCmd);

            log.debugf("Invalidated %d keys, data container of cache %s now has %d keys", keysToRemove.size(), cacheName, dataContainer.size());
            if (trace) log.tracef("Invalidated keys: %s", keysToRemove);
         } catch (CacheException e) {
            log.failedToInvalidateKeys(e);
         }
      }
   }

   private int getSegment(Object key) {
      // there we can use any CH version because the routing table is not involved
      return cacheTopology.getReadConsistentHash().getSegment(key);
   }

   /**
    * Obtains the CacheStore that will be used for purging segments that are no longer owned by this node.
    * The CacheStore will be purged only if it is enabled and it is not shared.
    */
   private CacheStore getCacheStore() {
      if (cacheLoaderManager.isEnabled() && !cacheLoaderManager.isShared()) {
         return cacheLoaderManager.getCacheStore();
      }
      return null;
   }

   private InboundTransferTask addTransfer(Address source, Set<Integer> segmentsFromSource) {
      synchronized (this) {
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
      synchronized (this) {
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

      if (activeTopologyUpdates.get() == 0) {
         notifyEndOfTopologyUpdate(cacheTopology.getTopologyId());
      }
   }
}
