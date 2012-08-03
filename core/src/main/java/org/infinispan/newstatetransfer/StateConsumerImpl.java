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

package org.infinispan.newstatetransfer;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.util.ReadOnlyDataContainerBackedKeySet;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;

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

   private Configuration configuration;
   private RpcManager rpcManager;
   private CommandsFactory commandsFactory;
   private TransactionTable transactionTable;
   private DataContainer dataContainer;
   private CacheLoaderManager cacheLoaderManager;
   private ExecutorService executorService;
   private InterceptorChain interceptorChain;
   private InvocationContextContainer icc;
   private long timeout;
   private boolean useVersionedPut;

   private int topologyId;
   private ConsistentHash currentCh;

   private Map<Address, List<InboundTransferTask>> transfersBySource = new HashMap<Address, List<InboundTransferTask>>();
   private Map<Integer, InboundTransferTask> transfersBySegment = new HashMap<Integer, InboundTransferTask>();

   public StateConsumerImpl(ExecutorService executorService,
                            InterceptorChain interceptorChain,
                            InvocationContextContainer icc,
                            Configuration configuration,
                            RpcManager rpcManager,
                            CommandsFactory commandsFactory,
                            CacheLoaderManager cacheLoaderManager,
                            DataContainer dataContainer,
                            TransactionTable transactionTable) {
      this.executorService = executorService;
      this.interceptorChain = interceptorChain;
      this.icc = icc;
      this.configuration = configuration;
      this.rpcManager = rpcManager;
      this.commandsFactory = commandsFactory;
      this.cacheLoaderManager = cacheLoaderManager;
      this.dataContainer = dataContainer;
      this.transactionTable = transactionTable;

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
      synchronized (this) {
         return currentCh != null && transfersBySegment.containsKey(currentCh.getSegment(key));
      }
   }

   @Override
   public void onTopologyUpdate(int topologyId, ConsistentHash newCh) {
      log.debugf("Received new CH: %s", newCh);
      this.topologyId = topologyId;

      synchronized (this) {
         if (currentCh == null) {
            // we start fresh, without any data, so we need to pull everything we own according to this CH
            currentCh = newCh;

            if (configuration.clustering().stateTransfer().fetchInMemoryState()) {
               Set<Integer> segments = currentCh.getSegmentsForOwner(rpcManager.getAddress());   //todo [anistor] in replicated case we need to generate all segments rather than get them from the CH
               addTransfers(segments);
            }
         } else {
            Set<Integer> oldSegments = currentCh.getSegmentsForOwner(rpcManager.getAddress());
            currentCh = newCh;
            Set<Integer> newSegments = currentCh.getSegmentsForOwner(rpcManager.getAddress());

            // we need to diff the addressing tables of the two CHes
            Set<Integer> removedSegments = new HashSet<Integer>(oldSegments);
            removedSegments.removeAll(newSegments);
            Set<Integer> addedSegments = new HashSet<Integer>(newSegments);
            addedSegments.removeAll(oldSegments);

            // remove inbound transfers and received data for segments we no longer own
            discardSegments(removedSegments);       //todo [anistor] what do we do with transactions and locks of removed segments?

            if (configuration.clustering().stateTransfer().fetchInMemoryState()) {
               // check if any of the existing transfers should be restarted from a different source because the initial source is no longer a member
               //todo [anistor] what do we do with the locks and transactions for restarted segments?
               Set<Address> members = new HashSet<Address>(currentCh.getMembers());
               for (Address source : transfersBySource.keySet()) {
                  if (!members.contains(source)) {
                     List<InboundTransferTask> inboundTransfers = transfersBySource.get(source);
                     if (inboundTransfers != null) {
                        for (InboundTransferTask inboundTransfer : inboundTransfers) {
                           removeTransfer(inboundTransfer);
                           addedSegments.addAll(inboundTransfer.getSegments());   // these segments will be restarted
                        }
                     }
                  }
               }

               // add transfers for new or restarted segments
               addTransfers(addedSegments);
            }
         }
      }
   }

   public void applyState(Address sender, int topologyId, int segmentId, Collection<InternalCacheEntry> cacheEntries) {
      // it's possible to receive a late message so we must be prepared to ignore segments we no longer own
      if (currentCh == null || !currentCh.locateOwnersForSegment(segmentId).contains(rpcManager.getAddress())) {  //todo [anistor] optimize
         log.tracef("Discarding the received cache entries because they do not belong to this node.");
         return;
      }

      // notify the inbound task that a chunk of cache entries was received
      InboundTransferTask inboundTransfer = transfersBySegment.get(segmentId);
      if (inboundTransfer != null) {
         inboundTransfer.onStateReceived(segmentId, cacheEntries.size());
      } else {
         log.debugf("Received unsolicited state for segment %d from node %s", segmentId, sender);
         return;
      }

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

      if (trace) {
         log.tracef("After applying state data container has %d keys", dataContainer.size());
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

   @Override
   public void shutdown() {
      synchronized (this) {
         // cancel all inbound transfers
         for (Iterator<Address> it = transfersBySource.keySet().iterator(); it.hasNext(); ) {
            Address source = it.next();
            List<InboundTransferTask> inboundTransfers = transfersBySource.get(source);
            it.remove();
            for (InboundTransferTask inboundTransfer : inboundTransfers) {
               inboundTransfer.cancel();
            }
         }
         transfersBySource.clear();
         transfersBySegment.clear();
      }
   }

   private void addTransfers(Set<Integer> segments) {
      log.debugf("Adding state transfer for segments: %s", segments);
      synchronized (this) {
         Map<Address, Set<Integer>> segmentsBySource = new HashMap<Address, Set<Integer>>();
         for (int segmentId : segments) {
            if (transfersBySegment.containsKey(segmentId)) {
               throw new IllegalArgumentException("Cannot have more than one transfer for segment " + segmentId);
            }

            List<Address> owners = currentCh.locateOwnersForSegment(segmentId);
            Address source = owners.get(owners.size() - 1);  //pick the last owner as source

            Set<Integer> seg = segmentsBySource.get(source);
            if (seg == null) {
               seg = new HashSet<Integer>();
               segmentsBySource.put(source, seg);
            }
            seg.add(segmentId);
         }

         for (Address source : segmentsBySource.keySet()) {
            Set<Integer> seg = segmentsBySource.get(source);
            InboundTransferTask inboundTransfer = new InboundTransferTask(seg, source, topologyId, this, rpcManager, commandsFactory, timeout);
            for (int segmentId : seg) {
               transfersBySegment.put(segmentId, inboundTransfer);
            }
            List<InboundTransferTask> inboundTransfers = transfersBySource.get(inboundTransfer.getSource());
            if (inboundTransfers == null) {
               inboundTransfers = new ArrayList<InboundTransferTask>();
               transfersBySource.put(inboundTransfer.getSource(), inboundTransfers);
            }
            inboundTransfers.add(inboundTransfer);
            executorService.submit(inboundTransfer);
         }
      }
   }

   /**
    * Remove the segment's data from the data container because we no longer own it.
    *
    * @param segments to be cancelled and discarded
    */
   private void discardSegments(Set<Integer> segments) {
      synchronized (this) {
         List<Integer> segmentsToCancel = new ArrayList<Integer>(segments);
         while (!segmentsToCancel.isEmpty()) {
            int segmentId = segmentsToCancel.get(0);
            log.debugf("Removing state transfer for segment %d", segmentId);
            InboundTransferTask inboundTransfer = transfersBySegment.get(segmentId);
            Set<Integer> cancelledSegments = new HashSet<Integer>(segmentsToCancel);
            cancelledSegments.retainAll(inboundTransfer.getSegments());
            segmentsToCancel.removeAll(cancelledSegments);
            inboundTransfer.cancelSegments(cancelledSegments);
         }
      }

      Set<Object> keysToRemove = new HashSet<Object>();
      for (InternalCacheEntry ice : dataContainer) {
         Object key = ice.getKey();
         if (segments.contains(currentCh.getSegment(key))) {
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
               if (segments.contains(currentCh.getSegment(key))) {
                  keysToRemove.add(key);
               }
            }

         } catch (CacheLoaderException e) {
            e.printStackTrace();  // TODO [anistor] handle properly
         }
      }

      if (!keysToRemove.isEmpty()) {
         try {
            InvalidateCommand invalidateCmd = commandsFactory.buildInvalidateFromL1Command(true, keysToRemove);  //todo [anistor] is this ok for removing keys or should we use a remove command?
            InvocationContext ctx = icc.createNonTxInvocationContext();
            ctx.setFlags(CACHE_MODE_LOCAL, SKIP_LOCKING);
            interceptorChain.invoke(ctx, invalidateCmd);

            log.debugf("Invalidated %d keys, data container now has %d keys", keysToRemove.size(), dataContainer.size());
            log.tracef("Invalidated keys: %s", keysToRemove);
         } catch (CacheException e) {
            log.failedToInvalidateKeys(e);
         }
      }
      //todo [anistor] notifyDataRehashed
   }

   private CacheStore getCacheStore() {
      if (configuration.clustering().cacheMode().isInvalidation()) {
         // the cache store is ignored in case of invalidation mode caches
         return null;
      }
      if (cacheLoaderManager != null && cacheLoaderManager.isEnabled() && !cacheLoaderManager.isShared() && cacheLoaderManager.isFetchPersistentState()) {
         return cacheLoaderManager.getCacheStore();
      }
      return null;
   }

   private void removeTransfer(InboundTransferTask inboundTransfer) {
      synchronized (this) {
         List<InboundTransferTask> transfers = transfersBySource.get(inboundTransfer.getSource());
         if (transfers != null) {
            if (transfers.remove(inboundTransfer)) {
               for (int segmentId : inboundTransfer.getSegments()) {
                  transfersBySegment.remove(segmentId);
               }
               if (transfers.isEmpty()) {
                  transfersBySource.remove(inboundTransfer.getSource());
               }
            }
         }
      }
   }

   void onTaskCompletion(InboundTransferTask inboundTransfer) {
      removeTransfer(inboundTransfer);
   }
}
