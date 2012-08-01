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

import org.infinispan.commands.CommandsFactory;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.ch.AdvancedConsistentHash;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.ReadOnlyDataContainerBackedKeySet;
import org.infinispan.util.concurrent.AggregatingNotifyingFutureBuilder;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.infinispan.util.concurrent.ConcurrentMapFactory;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.*;

/**
 * // TODO [anistor] Document this
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class OutboundTransferTask implements Runnable {

   private static final Log log = LogFactory.getLog(OutboundTransferTask.class);

   private final boolean trace = log.isTraceEnabled();

   private StateProviderImpl stateProvider;

   private volatile boolean isCancelled = false;

   private final int topologyId;

   private final Address destination;

   private final Set<Integer> segments = new ConcurrentHashSet<Integer>();

   private final int stateTransferChunkSize;

   private final AdvancedConsistentHash ch;

   private final DataContainer dataContainer;

   private final CacheLoaderManager cacheLoaderManager;

   private final RpcManager rpcManager;

   private final CommandsFactory commandsFactory;

   private final long timeout;

   private final Map<Integer, List<InternalCacheEntry>> entriesBySegment = ConcurrentMapFactory.makeConcurrentMap();

   private final NotifyingNotifiableFuture<Object> sendFuture = new AggregatingNotifyingFutureBuilder(null, 10);  //todo [anistor] why do we have to specify a capacity?

   public OutboundTransferTask(Address destination, Set<Integer> segments, int stateTransferChunkSize,
                               int topologyId, AdvancedConsistentHash ch, StateProviderImpl stateProvider, DataContainer dataContainer,
                               CacheLoaderManager cacheLoaderManager, RpcManager rpcManager,
                               CommandsFactory commandsFactory, long timeout) {
      if (segments == null || segments.isEmpty()) {
         throw new IllegalArgumentException("Segments must not be null or empty");
      }
      if (destination == null) {
         throw new IllegalArgumentException("Destination address cannot be null");
      }
      if (stateTransferChunkSize <= 0) {
         throw new IllegalArgumentException("stateTransferChunkSize must be greater than 0");
      }
      this.stateProvider = stateProvider;
      this.destination = destination;
      this.segments.addAll(segments);
      this.stateTransferChunkSize = stateTransferChunkSize;
      this.topologyId = topologyId;
      this.ch = ch;
      this.dataContainer = dataContainer;
      this.cacheLoaderManager = cacheLoaderManager;
      this.rpcManager = rpcManager;
      this.commandsFactory = commandsFactory;
      this.timeout = timeout;
   }

   public Address getDestination() {
      return destination;
   }

   @Override
   public void run() {
      //todo [anistor] do we still need configuration.clustering().stateTransfer().fetchInMemoryState() ?

      try {
         // send data container entries
         for (InternalCacheEntry ice : dataContainer) {
            if (isCancelled) {
               return;
            }

            Object key = ice.getKey();
            int segmentId = ch.getSegment(key);
            if (segments.contains(segmentId)) {
               sendEntry(ice, segmentId);
            }
         }

         if (isCancelled) {
            return;
         }

         // send cache store entries if needed
         if (cacheLoaderManager != null && cacheLoaderManager.isEnabled() && !cacheLoaderManager.isShared()) {
            CacheStore cacheStore = cacheLoaderManager.getCacheStore();
            try {
               //todo [anistor] extend CacheStore interface to be able to specify a filter when loading keys (ie. keys should belong to desired segments)
               Set<Object> storedKeys = cacheStore.loadAllKeys(new ReadOnlyDataContainerBackedKeySet(dataContainer));
               for (Object key : storedKeys) {
                  if (isCancelled) {
                     return;
                  }
                  int segmentId = ch.getSegment(key);
                  if (segments.contains(segmentId)) {
                     try {
                        InternalCacheEntry ice = cacheStore.load(key);
                        if (ice != null) { // check entry still exists
                           sendEntry(ice, segmentId);
                        }
                     } catch (CacheLoaderException e) {
                        log.failedLoadingValueFromCacheStore(key);
                     }
                  }
               }
            } catch (CacheLoaderException e) {
               e.printStackTrace();  // TODO [anistor] handle properly
            }
         } else {
            if (trace) {
               log.tracef("No cache store or the cache store is shared, no need to send any stored cache entries for segments %s", segments);
            }
         }

         // send the last chunk of all segments. if the last chunk is not empty an additional empty chunk is sent to signal completion
         for (int segmentId : segments) {
            if (isCancelled) {
               return;
            }

            List<InternalCacheEntry> entries = entriesBySegment.get(segmentId);
            sendEntries(entries, segmentId);
            if (!entries.isEmpty()) {
               sendEntries(Collections.<InternalCacheEntry>emptyList(), segmentId);
            }
         }
      } finally {
         stateProvider.onTaskCompletion(this);
      }
   }

   private void sendEntry(InternalCacheEntry ice, int segmentId) {
      List<InternalCacheEntry> entries = entriesBySegment.get(segmentId);
      if (entries == null) {
         entries = new ArrayList<InternalCacheEntry>();
         entriesBySegment.put(segmentId, entries);
      }
      entries.add(ice);

      // send if we have a full chunk
      if (entries.size() >= stateTransferChunkSize) {
         sendEntries(entries, segmentId);
         entries.clear();
      }
   }

   private void sendEntries(List<InternalCacheEntry> entries, int segmentId) {
      if (!isCancelled) {
         StateResponseCommand cmd = commandsFactory.buildStateResponseCommand(rpcManager.getAddress(), topologyId, segmentId, entries);
         // send synchronously, in FIFO mode. it is important that the last chunk is received last in order to correctly detect completion of the stream of chunks
         rpcManager.invokeRemotelyInFuture(Collections.singleton(destination), cmd, false, sendFuture, timeout);
      }
   }

   public void cancelSegments(Set<Integer> cancelledSegments) {
      if (segments.removeAll(cancelledSegments)) {
         entriesBySegment.keySet().removeAll(cancelledSegments);
         if (segments.isEmpty()) {
            cancel();
         }
      }
   }

   /**
    * Cancel the whole task.
    */
   public void cancel() {
      if (!isCancelled) {
         isCancelled = true;
         sendFuture.cancel(true);    //todo [anistor] is this going to cancel the unprocessed jgroup send operations? I hope so
         stateProvider.onTaskCompletion(this);
      }
   }
}
