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
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.ReadOnlyDataContainerBackedKeySet;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.infinispan.context.Flag.*;

/**
 * {@link StateConsumer} implementation.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class StateConsumerImpl implements StateConsumer {

   private static final Log log = LogFactory.getLog(StateConsumerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   private StateTransferManager stateTransferManager;
   private String cacheName;
   private Configuration configuration;
   private RpcManager rpcManager;
   private TransactionManager transactionManager;   // optional
   private CommandsFactory commandsFactory;
   private TransactionTable transactionTable;       // optional
   private DataContainer dataContainer;
   private CacheLoaderManager cacheLoaderManager;   // optional
   private InterceptorChain interceptorChain;
   private InvocationContextContainer icc;
   private StateTransferLock stateTransferLock;
   private long timeout;
   private boolean useVersionedPut;
   private boolean fetchEnabled;

   private volatile CacheTopology cacheTopology;

   /**
    * Keeps track of all keys updated by user code during state transfer. If this is null no keys are being recorded and
    * state transfer is not allowed to update anything. This can be null if there is not state transfer in progress at
    * the moment of there is one but a ClearCommand was encountered.
    */
   private volatile Set<Object> updatedKeys;

   /**
    * Stops applying incoming state. Also stops tracking updated keys. Should be called at the end of state transfer or
    * when a ClearCommand is committed during state transfer.
    */
   public void stopApplyingState() {
      updatedKeys = null;
   }

   /**
    * Receive notification of updated keys right before they are committed in DataContainer.
    *
    * @param key the key that is being modified
    */
   public void addUpdatedKey(Object key) {
      if (updatedKeys != null) {
         if (cacheTopology.getWriteConsistentHash().isKeyLocalToNode(rpcManager.getAddress(), key)) {
            updatedKeys.add(key);
         }
      }
   }

   /**
    * Checks if a given key was updated by user code during state transfer (and consequently it is untouchable by state transfer).
    *
    * @param key the key to check
    * @return true if the key is known to be modified, false otherwise
    */
   public boolean isKeyUpdated(Object key) {
      return updatedKeys == null || updatedKeys.contains(key);
   }

   /**
    * The number of topology updates that are being processed concurrently (in method onTopologyUpdate()).
    * This is needed to be able to detect completion.
    */
   private final AtomicInteger activeTopologyUpdates = new AtomicInteger(0);

   /**
    * Indicates if the currently executing topology update is a rebalance.
    */
   private final AtomicBoolean rebalanceInProgress = new AtomicBoolean(false);

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

   public StateConsumerImpl() {
   }

   @Inject
   public void init(Cache cache,
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
                    StateTransferLock stateTransferLock) {
      this.cacheName = cache.getName();
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

      // we need to use a special form of PutKeyValueCommand that can apply versions too
      useVersionedPut = configuration.transaction().transactionMode().isTransactional() &&
            configuration.versioning().enabled() &&
            configuration.locking().writeSkewCheck() &&
            configuration.transaction().lockingMode() == LockingMode.OPTIMISTIC &&
            configuration.clustering().cacheMode().isClustered();

      timeout = configuration.clustering().stateTransfer().timeout();
   }

   public boolean isStateTransferInProgress() {
      // TODO This is called quite often, use an extra volatile, a concurrent collection or a RWLock instead
      synchronized (this) {
         return !transfersBySource.isEmpty();
      }
   }

   @Override
   public boolean isStateTransferInProgressForKey(Object key) {
      if (configuration.clustering().cacheMode().isInvalidation()) {
         return false;
      }
      // todo [anistor] also return true for keys to be removed (now we report only keys to be added)
      synchronized (this) {
         return cacheTopology != null && transfersBySegment.containsKey(getSegment(key));
      }
   }

   @Override
   public void onTopologyUpdate(CacheTopology cacheTopology, boolean isRebalance) {
      if (trace) log.tracef("Received new CH %s for cache %s", cacheTopology.getWriteConsistentHash(), cacheName);

      int numStartedTopologyUpdates = activeTopologyUpdates.incrementAndGet();
      if (isRebalance) {
         rebalanceInProgress.set(true);
      }
      final ConsistentHash previousCh = this.cacheTopology != null ? this.cacheTopology.getWriteConsistentHash() : null;
      // Ensures writes to the data container use the right consistent hash
      // No need for a try/finally block, since it's just an assignment
      stateTransferLock.acquireExclusiveTopologyLock();
      this.cacheTopology = cacheTopology;
      if (numStartedTopologyUpdates == 1) {
         updatedKeys = new ConcurrentHashSet<Object>();
      }
      stateTransferLock.releaseExclusiveTopologyLock();
      stateTransferLock.notifyTopologyInstalled(cacheTopology.getTopologyId());

      try {
         Set<Integer> addedSegments;
         if (previousCh == null) {
            // we start fresh, without any data, so we need to pull everything we own according to writeCh

            addedSegments = getOwnedSegments(cacheTopology.getWriteConsistentHash());

            if (trace) {
               log.tracef("On cache %s we have: added segments: %s", cacheName, addedSegments);
            }
         } else {
            Set<Integer> previousSegments = getOwnedSegments(previousCh);
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

            // remove inbound transfers and any data for segments we no longer own
            cancelTransfers(removedSegments);

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
      } finally {
         stateTransferLock.notifyTransactionDataReceived(cacheTopology.getTopologyId());

         if (activeTopologyUpdates.decrementAndGet() == 0) {
            notifyEndOfTopologyUpdate(cacheTopology.getTopologyId());
         }
      }
   }

   private void notifyEndOfTopologyUpdate(int topologyId) {
      if (!isStateTransferInProgress()) {
         if (rebalanceInProgress.compareAndSet(true, false)) {
            log.debugf("Finished receiving of segments for cache %s for topology %d.", cacheName, topologyId);
            stopApplyingState();
            stateTransferManager.notifyEndOfTopologyUpdate(topologyId);
         }
      }
   }

   private Set<Integer> getOwnedSegments(ConsistentHash consistentHash) {
      Address address = rpcManager.getAddress();
      return consistentHash.getMembers().contains(address) ? consistentHash.getSegmentsForOwner(address)
            : InfinispanCollections.<Integer>emptySet();
   }

   public void applyState(Address sender, int topologyId, int segmentId, Collection<InternalCacheEntry> cacheEntries, boolean isLastChunk) {
      // it's possible to receive a late message so we must be prepared to ignore segments we no longer own
      //todo [anistor] this check should be based on topologyId
      if (!cacheTopology.getWriteConsistentHash().getSegmentsForOwner(rpcManager.getAddress()).contains(segmentId)) {
         if (trace) {
            log.warnf("Discarding received cache entries for segment %d of cache %s because they do not belong to this node.", segmentId, cacheName);
         }
         return;
      }

      // notify the inbound task that a chunk of cache entries was received
      InboundTransferTask inboundTransfer;
      synchronized (this) {
         inboundTransfer = transfersBySegment.get(segmentId);
      }
      if (inboundTransfer != null) {
         if (trace) {
            log.tracef("Before applying the received state the data container of cache %s has %d keys", cacheName, dataContainer.size());
         }

         if (cacheEntries != null) {
            doApplyState(sender, segmentId, cacheEntries);
         }

         inboundTransfer.onStateReceived(segmentId, isLastChunk);

         if (trace) {
            log.tracef("After applying the received state the data container of cache %s has %d keys", cacheName, dataContainer.size());
            synchronized (this) {
               log.tracef("Segments not received yet for cache %s: %s", cacheName, transfersBySource);
            }
         }
      } else {
         log.warnf("Received unsolicited state from node %s for segment %d of cache %s", sender, segmentId, cacheName);
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
                     ((LocalTransaction)((TxInvocationContext)ctx).getCacheTransaction()).setFromStateTransfer(true);
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

   public void applyTransactions(Address sender, int topologyId, Collection<TransactionInfo> transactions) {
      log.debugf("Applying %d transactions for cache %s transferred from node %s", transactions.size(), cacheName, sender);
      if (configuration.transaction().transactionMode().isTransactional()) {
         for (TransactionInfo transactionInfo : transactions) {
            CacheTransaction tx = transactionTable.getLocalTransaction(transactionInfo.getGlobalTransaction());
            if (tx == null) {
               tx = transactionTable.getRemoteTransaction(transactionInfo.getGlobalTransaction());
               if (tx == null) {
                  tx = transactionTable.createRemoteTransaction(transactionInfo.getGlobalTransaction(), transactionInfo.getModifications());
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
      fetchEnabled = configuration.clustering().stateTransfer().fetchInMemoryState() || cacheLoaderManager.isFetchPersistentState();
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

      Set<Integer> segmentsToProcess = new HashSet<Integer>(segments);
      Set<Address> faultysources = new HashSet<Address>();

      // ignore all segments for which there are no other owners to pull data from.
      // these segments are considered empty (or lost) and do not require a state transfer
      for (Iterator<Integer> it = segmentsToProcess.iterator(); it.hasNext(); ) {
         Integer segmentId = it.next();
         Address source = pickSourceOwner(segmentId, faultysources);
         if (source == null) {
            it.remove();
         }
      }

      while (!segmentsToProcess.isEmpty()) {
         Map<Address, Set<Integer>> segmentsBySource = new HashMap<Address, Set<Integer>>();
         for (int segmentId : segmentsToProcess) {
            synchronized (this) {
               // already active transfers do not need to be added again
               if (transfersBySegment.containsKey(segmentId)) {
                  continue;
               }
            }
            Address source = pickSourceOwner(segmentId, faultysources);
            if (source != null) {
               Set<Integer> segmentsFromSource = segmentsBySource.get(source);
               if (segmentsFromSource == null) {
                  segmentsFromSource = new HashSet<Integer>();
                  segmentsBySource.put(source, segmentsFromSource);
               }
               segmentsFromSource.add(segmentId);
            }
         }

         Set<Integer> failedSegments = new HashSet<Integer>();
         for (Address source : segmentsBySource.keySet()) {
            Set<Integer> segmentsFromSource = segmentsBySource.get(source);
            InboundTransferTask inboundTransfer;
            synchronized (this) {
               segmentsFromSource.removeAll(transfersBySegment.keySet());  // already in progress segments are excluded
               if (segmentsFromSource.isEmpty()) {
                  continue;
               }

               inboundTransfer = new InboundTransferTask(segmentsFromSource, source, cacheTopology.getTopologyId(), this, rpcManager, commandsFactory, timeout, cacheName);
               for (int segmentId : segmentsFromSource) {
                  transfersBySegment.put(segmentId, inboundTransfer);
               }
               List<InboundTransferTask> inboundTransfers = transfersBySource.get(inboundTransfer.getSource());
               if (inboundTransfers == null) {
                  inboundTransfers = new ArrayList<InboundTransferTask>();
                  transfersBySource.put(inboundTransfer.getSource(), inboundTransfers);
               }
               inboundTransfers.add(inboundTransfer);
            }

            // if requesting the transactions fails we need to retry from another source
            if (configuration.transaction().transactionMode().isTransactional()) {
               if (!inboundTransfer.requestTransactions()) {
                  failedSegments.addAll(segmentsFromSource);
                  faultysources.add(source);
                  removeTransfer(inboundTransfer);  // will be retried from another source
                  continue;
               }
            }

            // if requesting the segments fails we need to retry from another source
            if (fetchEnabled) {
               if (!inboundTransfer.requestSegments()) {
                  failedSegments.addAll(segmentsFromSource);
                  faultysources.add(source);
                  removeTransfer(inboundTransfer);  // will be retried from another source
               }
            } else {
               removeTransfer(inboundTransfer);  // we consider it complete
            }
         }

         segmentsToProcess = failedSegments;
      }
      log.debugf("Finished adding inbound state transfer for segments %s of cache %s", segments, cacheName);
   }

   private Address pickSourceOwner(int segmentId, Set<Address> faultySources) {
      List<Address> owners = cacheTopology.getReadConsistentHash().locateOwnersForSegment(segmentId);
      if (owners.size() == 1 && owners.get(0).equals(rpcManager.getAddress())) {
         return null;
      }

      for (int i = owners.size() - 1; i >= 0; i--) {   // iterate backwards because we prefer to fetch from newer nodes
         Address o = owners.get(i);
         if (!o.equals(rpcManager.getAddress()) && !faultySources.contains(o)) {
            return o;
         }
      }
      log.noLiveOwnersFoundForSegment(segmentId, cacheName, owners, faultySources);
      return null;
   }

   /**
    * Remove the segment's data from the data container and cache store because we no longer own it.
    *
    * @param removedSegments to be cancelled and discarded
    */
   private void cancelTransfers(Set<Integer> removedSegments) {
      synchronized (this) {
         List<Integer> segmentsToCancel = new ArrayList<Integer>(removedSegments);
         while (!segmentsToCancel.isEmpty()) {
            int segmentId = segmentsToCancel.remove(0);
            InboundTransferTask inboundTransfer = transfersBySegment.remove(segmentId);
            if (inboundTransfer != null) { // we need to check the transfer was not already completed
               log.debugf("Cancelling inbound state transfer for segment %d of cache %s", segmentId, cacheName);
               Set<Integer> cancelledSegments = new HashSet<Integer>(segmentsToCancel);
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

      // gather all keys from cache store that belong to the segments that are being removed/moved to L1
      for (InternalCacheEntry ice : dataContainer) {
         Object key = ice.getKey();
         int keySegment = getSegment(key);
         if (segmentsToL1.contains(keySegment)) {
            keysToL1.add(key);
         } else if (!newSegments.contains(keySegment)) {
            keysToRemove.add(key);
         }
      }

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

      //todo [anistor] call CacheNotifier.notifyDataRehashed
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
      if (cacheLoaderManager != null && cacheLoaderManager.isEnabled() && !cacheLoaderManager.isShared()) {
         return cacheLoaderManager.getCacheStore();
      }
      return null;
   }

   private void removeTransfer(InboundTransferTask inboundTransfer) {
      synchronized (this) {
         List<InboundTransferTask> transfers = transfersBySource.get(inboundTransfer.getSource());
         if (transfers != null) {
            if (transfers.remove(inboundTransfer)) {
               if (transfers.isEmpty()) {
                  transfersBySource.remove(inboundTransfer.getSource());
               }
               transfersBySegment.keySet().removeAll(inboundTransfer.getSegments());
            }
         }
      }
   }

   void onTaskCompletion(InboundTransferTask inboundTransfer) {
      log.tracef("Completion of inbound transfer task: %s ", inboundTransfer);
      removeTransfer(inboundTransfer);

      if (activeTopologyUpdates.get() == 0) {
         notifyEndOfTopologyUpdate(cacheTopology.getTopologyId());
      }
   }
}
