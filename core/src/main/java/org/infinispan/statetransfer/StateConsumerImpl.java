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
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distribution.ch.*;
import org.infinispan.distribution.group.GroupManager;
import org.infinispan.distribution.group.GroupingConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheJoinInfo;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.CacheTopologyHandler;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.util.ReadOnlyDataContainerBackedKeySet;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.*;

import static org.infinispan.context.Flag.*;

/**
 * // TODO [anistor] Document this
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class StateConsumerImpl implements StateConsumer {

   private static final Log log = LogFactory.getLog(StateConsumerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   private StateProvider stateProvider;
   private LocalTopologyManager localTopologyManager;
   private String cacheName;
   private CacheNotifier cacheNotifier;
   private Configuration configuration;
   private GlobalConfiguration globalConfiguration;
   private RpcManager rpcManager;
   private GroupManager groupManager;
   private CommandsFactory commandsFactory;
   private TransactionTable transactionTable;
   private DataContainer dataContainer;
   private CacheLoaderManager cacheLoaderManager;
   private InterceptorChain interceptorChain;
   private InvocationContextContainer icc;
   private StateTransferLock stateTransferLock;
   private long timeout;
   private boolean useVersionedPut;

   /**
    * Current topology id.
    */
   private int topologyId;

   /**
    * CH used for read operations. It is never null.
    */
   private ConsistentHash readCh;

   /**
    * CH used for write operations. In some cases this can be the same as readCh. It is never null.
    */
   private ConsistentHash writeCh;

   private volatile CacheTopology cacheTopology;

   /**
    * The number of topology updates that are being processed concurrently (in method onTopologyUpdate()).
    * This is needed to be able to detect completion.
    */
   private int isTopologyUpdate = 0;

   private volatile boolean rebalanceInProgress;

   /**
    * A map that keeps track of current inbound state transfers by source address. There could be multiple transfers
    * flowing in from the same source (but for different segments) so the values are lists. This works in tandem with
    * transfersBySegment so they always need to be kept in sync and updates to both of them need to be atomic.
    */
   private Map<Address, List<InboundTransferTask>> transfersBySource = new HashMap<Address, List<InboundTransferTask>>();

   /**
    * A map that keeps track of current inbound state transfers by segment id. There is at most one transfers per segment.
    * This works in tandem with transfersBySource so they always need to be kept in sync and updates to both of them
    * need to be atomic.
    */
   private Map<Integer, InboundTransferTask> transfersBySegment = new HashMap<Integer, InboundTransferTask>();

   public StateConsumerImpl() {
   }

   @Inject
   public void init(Cache cache,
                    StateProvider stateProvider,
                    LocalTopologyManager localTopologyManager,
                    CacheNotifier cacheNotifier,
                    InterceptorChain interceptorChain,
                    InvocationContextContainer icc,
                    Configuration configuration,
                    GlobalConfiguration globalConfiguration,
                    RpcManager rpcManager,
                    GroupManager groupManager,
                    CommandsFactory commandsFactory,
                    CacheLoaderManager cacheLoaderManager,
                    DataContainer dataContainer,
                    TransactionTable transactionTable,
                    StateTransferLock stateTransferLock) {
      this.cacheName = cache.getName();
      this.stateProvider = stateProvider;
      this.localTopologyManager = localTopologyManager;
      this.cacheNotifier = cacheNotifier;
      this.interceptorChain = interceptorChain;
      this.icc = icc;
      this.configuration = configuration;
      this.globalConfiguration = globalConfiguration;
      this.rpcManager = rpcManager;
      this.groupManager = groupManager;
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
         return readCh != null && transfersBySegment.containsKey(getSegment(key));
      }
   }

   @Override
   public void onTopologyUpdate(CacheTopology cacheTopology, boolean isRebalance) {
      ConsistentHash readCh = cacheTopology.getReadConsistentHash();
      ConsistentHash writeCh = cacheTopology.getWriteConsistentHash();
      int topologyId = cacheTopology.getTopologyId();

      if (trace) log.tracef("Received new CH: %s", writeCh);

      ConsistentHash previousCh;
      synchronized (this) {
         isTopologyUpdate++;
         this.cacheTopology = cacheTopology;
         this.topologyId = cacheTopology.getTopologyId();
         rebalanceInProgress |= isRebalance;
         previousCh = this.writeCh != null ? this.writeCh : this.readCh;
         this.readCh = readCh;
         this.writeCh = writeCh;
      }

      stateTransferLock.setTopologyId(topologyId);

      try {
         Set<Integer> addedSegments = null;
         if (previousCh == null) {
            // we start fresh, without any data, so we need to pull everything we own according to writeCh
            if (configuration.clustering().stateTransfer().fetchInMemoryState() && !configuration.clustering().cacheMode().isInvalidation()) {
               addedSegments = getOwnedSegments(writeCh);
            }
         } else {
            Set<Integer> previousSegments = getOwnedSegments(previousCh);
            Set<Integer> newSegments = getOwnedSegments(writeCh);

            // we need to diff the routing tables of the two CHes
            Set<Integer> removedSegments = new HashSet<Integer>(previousSegments);
            removedSegments.removeAll(newSegments);

            // remove inbound transfers and any data for segments we no longer own
            discardSegments(removedSegments);       //todo [anistor] what do we do with transactions and locks of removed segments?

            if (configuration.clustering().stateTransfer().fetchInMemoryState() && !configuration.clustering().cacheMode().isInvalidation()) {
               Set<Integer> currentSegments = getOwnedSegments(readCh);
               addedSegments = new HashSet<Integer>(newSegments);
               addedSegments.removeAll(currentSegments);

               // check if any of the existing transfers should be restarted from a different source because the initial source is no longer a member
               Set<Address> members = new HashSet<Address>(readCh.getMembers());
               synchronized (this) {
                  for (Address source : transfersBySource.keySet()) {
                     if (!members.contains(source)) {
                        List<InboundTransferTask> inboundTransfers = transfersBySource.remove(source);
                        if (inboundTransfers != null) {
                           for (InboundTransferTask inboundTransfer : inboundTransfers) {
                              for (int segmentId : inboundTransfer.getSegments()) {
                                 transfersBySegment.remove(segmentId);   //todo [anistor] what do we do with the locks and transactions for restarted segments?
                                 addedSegments.add(segmentId);   // this segment will be restarted
                              }
                           }
                        }
                     }
                  }
               }
            }
         }

         if (addedSegments != null && !addedSegments.isEmpty()) {
            stateTransferLock.commandsExclusiveLock();
            try {
               addTransfers(addedSegments);  // add transfers for new or restarted segments
            } finally {
               stateTransferLock.commandsExclusiveUnlock();
            }
         }
      } finally {
         synchronized (this) {
            isTopologyUpdate--;
            if (isTopologyUpdate == 0 && !isStateTransferInProgress()) {
               notifyEndOfStateTransfer(topologyId);
            }
         }
      }
   }

   private void notifyEndOfStateTransfer(int topologyId) {
      if (rebalanceInProgress) {
         rebalanceInProgress = false;
         localTopologyManager.confirmRebalance(cacheName, topologyId, null);
      }
   }

   private Set<Integer> getOwnedSegments(ConsistentHash consistentHash) {
      Address address = rpcManager.getAddress();
      return consistentHash.getMembers().contains(address) ? consistentHash.getSegmentsForOwner(address)
            : Collections.<Integer>emptySet();
   }

   //todo [anistor] check topologyId
   public void applyState(Address sender, int topologyId, int segmentId, Collection<InternalCacheEntry> cacheEntries, boolean isLastChunk) {
      // it's possible to receive a late message so we must be prepared to ignore segments we no longer own
      if (writeCh == null || !writeCh.getSegmentsForOwner(rpcManager.getAddress()).contains(segmentId)) {
         if (trace) {
            log.tracef("Discarding received cache entries for segment %d because they do not belong to this node.", segmentId);
         }
         return;
      }

      if (cacheEntries != null) {
         doApplyState(sender, segmentId, cacheEntries);
      }

      // notify the inbound task that a chunk of cache entries was received
      InboundTransferTask inboundTransfer;
      synchronized (this) {
         inboundTransfer = transfersBySegment.get(segmentId);
      }
      if (inboundTransfer != null) {
         inboundTransfer.onStateReceived(segmentId, isLastChunk);
      } else {
         log.debugf("Received unsolicited state for segment %d from node %s", segmentId, sender);
         return;
      }

      if (trace) {
         log.tracef("After applying the received state the data container has %d keys", dataContainer.size());
      }
   }

   private void doApplyState(Address sender, int segmentId, Collection<InternalCacheEntry> cacheEntries) {
      log.debugf("Applying new state for segment %d from %s: received %d cache entries", segmentId, sender, cacheEntries.size());
      if (trace) {
         List<Object> keys = new ArrayList<Object>(cacheEntries.size());
         for (InternalCacheEntry e : cacheEntries) {
            keys.add(e.getKey());
         }
         log.tracef("Received keys: %s", keys);
      }

      for (InternalCacheEntry e : cacheEntries) {
         InvocationContext ctx = icc.createInvocationContext(false, 1);
         // locking not necessary as during rehashing we block all transactions
         ctx.setFlags(CACHE_MODE_LOCAL, IGNORE_RETURN_VALUES, SKIP_SHARED_CACHE_STORE, SKIP_LOCKING, SKIP_OWNERSHIP_CHECK);
         try {
            PutKeyValueCommand put = useVersionedPut ?
                  commandsFactory.buildVersionedPutKeyValueCommand(e.getKey(), e.getValue(), e.getLifespan(), e.getMaxIdle(), e.getVersion(), ctx.getFlags())
                  : commandsFactory.buildPutKeyValueCommand(e.getKey(), e.getValue(), e.getLifespan(), e.getMaxIdle(), ctx.getFlags());
            put.setPutIfAbsent(true); //todo [anistor] this still does not solve removal cases. we need tombstones for deleted keys. we need to keep a separate map of deleted keys an use it during apply state
            interceptorChain.invoke(ctx, put);
         } catch (Exception ex) {
            log.problemApplyingStateForKey(ex.getMessage(), e.getKey());
         }
      }
   }

   public void applyTransactions(Address sender, int topologyId, Collection<TransactionInfo> transactions) {
      log.debugf("Transferring %d transaction from %s", transactions.size(), sender);
      for (TransactionInfo transactionInfo : transactions) {
         CacheTransaction tx = transactionTable.getLocalTransaction(transactionInfo.getGlobalTransaction());
         if (tx == null) {
            tx = transactionTable.getRemoteTransaction(transactionInfo.getGlobalTransaction());
            if (tx == null) {
               tx = transactionTable.createRemoteTransaction(transactionInfo.getGlobalTransaction(), transactionInfo.getModifications());
            }
         }
         for (Object key : transactionInfo.getLockedKeys()) {
            tx.addBackupLockForKey(key);
         }
      }
   }

   // needs to be AFTER the DistributionManager and *after* the cache loader manager (if any) inits and preloads
   @Start(priority = 60)
   public void start() throws Exception {
      if (trace) {
         log.tracef("Starting StateConsumer on %s", rpcManager.getAddress());
      }

      CacheJoinInfo joinInfo = new CacheJoinInfo(
            pickConsistentHashFactory(),
            configuration.clustering().hash().hash(),
            configuration.clustering().hash().numSegments(),
            configuration.clustering().hash().numOwners(),
            configuration.clustering().stateTransfer().timeout(),
            rpcManager.getTransport().getViewId());

      localTopologyManager.join(cacheName, joinInfo, new CacheTopologyHandler() {
         @Override
         public void updateConsistentHash(CacheTopology cacheTopology) {
            doTopologyUpdate(cacheTopology, false);
         }

         @Override
         public void rebalance(CacheTopology cacheTopology) {
            doTopologyUpdate(cacheTopology, true);
         }
      });
   }

   /**
    * If no ConsistentHashFactory was explicitly configured we choose a suitable one based on cache mode.
    */
   private ConsistentHashFactory pickConsistentHashFactory() {
      ConsistentHashFactory factory = configuration.clustering().hash().consistentHashFactory();
      if (factory == null) {
         CacheMode cacheMode = configuration.clustering().cacheMode();
         if (cacheMode.isClustered()) {
            if (cacheMode.isDistributed()) {
               if (globalConfiguration.transport().hasTopologyInfo()) {
                  factory = new TopologyAwareConsistentHashFactory();
               } else {
                  factory = new DefaultConsistentHashFactory();
               }
            } else {
               // this is also used for invalidation mode
               factory = new ReplicatedConsistentHashFactory();
            }
         }
      }
      return factory;
   }

   private void doTopologyUpdate(CacheTopology newCacheTopology, boolean isRebalance) {
      if (trace) {
         log.tracef("Installing new cache topology %s", newCacheTopology);
      }

      // handle grouping
      newCacheTopology = addGrouping(newCacheTopology);

      CacheTopology oldCacheTopology = cacheTopology;
      ConsistentHash oldCH = oldCacheTopology != null ? oldCacheTopology.getWriteConsistentHash() : null;
      ConsistentHash newCH = newCacheTopology.getWriteConsistentHash();

      cacheNotifier.notifyTopologyChanged(oldCH, newCH, true);
      cacheNotifier.notifyTopologyChanged(oldCH, newCH, false);

      stateProvider.onTopologyUpdate(newCacheTopology, isRebalance);
      onTopologyUpdate(newCacheTopology, isRebalance);
   }

   /**
    * Decorates the given cache topology to add key grouping. The ConsistentHash objects of the cache topology
    * are wrapped to provide key grouping (if configured).
    *
    * @param cacheTopology the given cache topology
    * @return the decorated topology
    */
   private CacheTopology addGrouping(CacheTopology cacheTopology) {
      if (groupManager == null) {
         return cacheTopology;
      }

      ConsistentHash currentCH = cacheTopology.getCurrentCH();
      currentCH = new GroupingConsistentHash(currentCH, groupManager);
      ConsistentHash pendingCH = cacheTopology.getPendingCH();
      if (pendingCH != null) {
         pendingCH = new GroupingConsistentHash(pendingCH, groupManager);
      }
      return new CacheTopology(cacheTopology.getTopologyId(), currentCH, pendingCH);
   }

   @Override
   @Stop(priority = 20)
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

      localTopologyManager.leave(cacheName);
   }

   @Override
   public CacheTopology getCacheTopology() {
      return cacheTopology;
   }

   private void addTransfers(Set<Integer> segments) {
      log.debugf("Adding state transfer for segments: %s", segments);

      Set<Integer> segmentsToProcess = new HashSet<Integer>(segments);
      Set<Address> blacklistedSources = new HashSet<Address>();

      // ignore all segments for which there are no other owners to pull data from.
      // these segments are considered empty (or lost) and do not require a state transfer
      for (Iterator<Integer> it = segmentsToProcess.iterator(); it.hasNext(); ) {
         Integer segmentId = it.next();
         Address source = pickSourceOwner(segmentId, blacklistedSources);
         if (source == null) {
            it.remove();
         }
      }

      synchronized (this) {
         // already active transfers do not need to be added again
         segmentsToProcess.removeAll(transfersBySegment.keySet());

         while (!segmentsToProcess.isEmpty()) {
            Map<Address, Set<Integer>> segmentsBySource = new HashMap<Address, Set<Integer>>();
            for (int segmentId : segmentsToProcess) {
               if (transfersBySegment.containsKey(segmentId)) {
                  throw new IllegalStateException("Cannot have more than one transfer for segment " + segmentId);
               }

               Address source = pickSourceOwner(segmentId, blacklistedSources);
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
               InboundTransferTask inboundTransfer = new InboundTransferTask(segmentsFromSource, source, topologyId, this, rpcManager, commandsFactory, timeout);
               for (int segmentId : segmentsFromSource) {
                  transfersBySegment.put(segmentId, inboundTransfer);
               }
               List<InboundTransferTask> inboundTransfers = transfersBySource.get(inboundTransfer.getSource());
               if (inboundTransfers == null) {
                  inboundTransfers = new ArrayList<InboundTransferTask>();
                  transfersBySource.put(inboundTransfer.getSource(), inboundTransfers);
               }
               inboundTransfers.add(inboundTransfer);

               // if requesting the transactions fails we need to retry from another source
               if (inboundTransfer.requestTransactions()) {
                  if (!inboundTransfer.requestSegments()) {
                     log.errorf("Failed to request segments %s from %s", segmentsFromSource, source);
                     //todo [anistor] maybe I need to remove this transfer to be retried from another source
                  }
               } else {
                  log.errorf("Failed to retrieve transactions for segments %s from %s", segmentsFromSource, source);
                  log.tracef("Adding members %s to black list", source);
                  failedSegments.addAll(segmentsFromSource);
                  blacklistedSources.add(source);
                  removeTransfer(inboundTransfer);
               }
            }

            segmentsToProcess = failedSegments;
         }
      }
   }

   private Address pickSourceOwner(int segmentId, Set<Address> blacklistedSources) {
      List<Address> owners = readCh.locateOwnersForSegment(segmentId);
      for (int i = owners.size() - 1; i >= 0; i--) {
         Address o = owners.get(i);
         if (!blacklistedSources.contains(o) && !o.equals(rpcManager.getAddress())) {
            return o;
         }
      }
      log.errorf("No live owners found for segment %d. Current owners are %s", segmentId, owners);
      return null;
   }

   /**
    * Remove the segment's data from the data container and cache store because we no longer own it.
    *
    * @param segments to be cancelled and discarded
    */
   private void discardSegments(Set<Integer> segments) {
      synchronized (this) {
         List<Integer> segmentsToCancel = new ArrayList<Integer>(segments);
         while (!segmentsToCancel.isEmpty()) {
            int segmentId = segmentsToCancel.remove(0);
            log.debugf("Removing state transfer for segment %d", segmentId);
            InboundTransferTask inboundTransfer = transfersBySegment.remove(segmentId);
            if (inboundTransfer != null) { // we need to check the transfer was not already completed
               Set<Integer> cancelledSegments = new HashSet<Integer>(segmentsToCancel);
               cancelledSegments.retainAll(inboundTransfer.getSegments());
               segmentsToCancel.removeAll(cancelledSegments);
               inboundTransfer.cancelSegments(cancelledSegments);
            }
         }
      }

      Set<Object> keysToRemove = new HashSet<Object>();
      for (InternalCacheEntry ice : dataContainer) {
         Object key = ice.getKey();
         if (segments.contains(getSegment(key))) {
            keysToRemove.add(key);
         }
      }

      // we also remove keys from the cache store
      CacheStore cacheStore = getCacheStore();
      if (cacheStore != null) {
         //todo [anistor] extend CacheStore interface to be able to specify a filter when loading keys (ie. keys should belong to desired segments)
         try {
            Set<Object> storedKeys = cacheStore.loadAllKeys(new ReadOnlyDataContainerBackedKeySet(dataContainer));
            for (Object key : storedKeys) {
               if (segments.contains(getSegment(key))) {
                  keysToRemove.add(key);
               }
            }

         } catch (CacheLoaderException e) {
            log.failedLoadingKeysFromCacheStore(e);
         }
      }

      if (!keysToRemove.isEmpty()) {
         try {
            InvalidateCommand invalidateCmd = commandsFactory.buildInvalidateFromL1Command(true, keysToRemove);
            InvocationContext ctx = icc.createNonTxInvocationContext();
            ctx.setFlags(CACHE_MODE_LOCAL, SKIP_LOCKING);
            interceptorChain.invoke(ctx, invalidateCmd);

            log.debugf("Invalidated %d keys, data container now has %d keys", keysToRemove.size(), dataContainer.size());
            if (trace) log.tracef("Invalidated keys: %s", keysToRemove);
         } catch (CacheException e) {
            log.failedToInvalidateKeys(e);
         }
      }
      //todo [anistor] CacheNotifier.notifyDataRehashed
   }

   private int getSegment(Object key) {
      // there we can use any CH version because the routing table is not involved
      return readCh.getSegment(key);
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
               for (int segmentId : inboundTransfer.getSegments()) {
                  transfersBySegment.remove(segmentId);
               }
            }
         }
      }
   }

   void onTaskCompletion(InboundTransferTask inboundTransfer) {
      removeTransfer(inboundTransfer);

      boolean allTasksCompleted;
      synchronized (this) {
         allTasksCompleted = isTopologyUpdate == 0 && !isStateTransferInProgress();
      }
      if (allTasksCompleted) {
         notifyEndOfStateTransfer(topologyId);
      }
   }
}
