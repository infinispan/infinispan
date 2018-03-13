package org.infinispan.conflict.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.NullCacheEntry;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.DataRehashed;
import org.infinispan.notifications.cachelistener.event.DataRehashedEvent;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.InboundTransferTask;
import org.infinispan.statetransfer.StateChunk;
import org.infinispan.topology.CacheTopology;

/**
 * @author Ryan Emerson
 * @since 9.1
 */
@Listener
public class StateReceiverImpl<K, V> implements StateReceiver<K, V> {

   private static final Log log = LogFactory.getLog(StateReceiverImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   @Inject private Cache<K, V> cache;
   @Inject private CommandsFactory commandsFactory;
   @Inject private DataContainer<K, V> dataContainer;
   @Inject private RpcManager rpcManager;

   private String cacheName;
   private long transferTimeout;

   private final ConcurrentHashMap<Integer, SegmentRequest> requestMap = new ConcurrentHashMap<>();

   @Start
   public void start() {
      this.cache.addListener(this);
      this.cacheName = cache.getName();
      this.transferTimeout = cache.getCacheConfiguration().clustering().stateTransfer().timeout();
   }

   @Override
   @Stop
   public void stop() {
      if (trace) log.tracef("Stop called on StateReceiverImpl for cache %s", cacheName);
      for (SegmentRequest request : requestMap.values())
         request.cancel(null);
   }

   @DataRehashed
   @SuppressWarnings("WeakerAccess")
   public void onDataRehash(DataRehashedEvent dataRehashedEvent) {
      if (dataRehashedEvent.isPre()) {
         for (SegmentRequest request : requestMap.values())
            request.cancel(new CacheException("Cancelling replica request as the owners of the requested " +
               "segment have changed."));
      }
   }

   @Override
   public CompletableFuture<List<Map<Address, CacheEntry<K, V>>>> getAllReplicasForSegment(int segmentId, LocalizedCacheTopology topology) {
      return requestMap.computeIfAbsent(segmentId, id -> new SegmentRequest(id, topology)).requestState();
   }

   @Override
   public void receiveState(Address sender, int topologyId, Collection<StateChunk> stateChunks) {
      if (stateChunks.isEmpty()) {
         if (trace)
            log.tracef("Cache %s Ignoring received state from %s because stateChunks are empty", cacheName, sender);
         return;
      }

      int segmentId = stateChunks.iterator().next().getSegmentId();
      SegmentRequest request = requestMap.get(segmentId);
      if (request == null) {
         if (trace) log.tracef("Cache %s Ignoring received state because the associated request was completed or cancelled %s", cacheName);
         return;
      }
      request.receiveState(sender, topologyId, stateChunks);
   }

   Map<K, Map<Address, CacheEntry<K, V>>> getKeyReplicaMap(int segmentId) {
      return requestMap.get(segmentId).keyReplicaMap;
   }

   Map<Address, InboundTransferTask> getTransferTaskMap(int segmentId) {
      return requestMap.get(segmentId).transferTaskMap;
   }

   InboundTransferTask createTransferTask(int segmentId, Address source, CacheTopology topology) {
      return new InboundTransferTask(Collections.singleton(segmentId), source, topology.getTopologyId(),
            rpcManager, commandsFactory, transferTimeout, cacheName, false);
   }

   class SegmentRequest {
      final int segmentId;
      final LocalizedCacheTopology topology;
      final List<Address> replicaHosts;
      final Map<K, Map<Address, CacheEntry<K, V>>> keyReplicaMap = new HashMap<>();
      final Map<Address, InboundTransferTask> transferTaskMap = new ConcurrentHashMap<>();
      CompletableFuture<List<Map<Address, CacheEntry<K, V>>>> future;

      SegmentRequest(int segmentId, LocalizedCacheTopology topology) {
         this.segmentId = segmentId;
         this.topology = topology;
         this.replicaHosts = topology.getDistributionForSegment(segmentId).writeOwners();
      }

      synchronized CompletableFuture<List<Map<Address, CacheEntry<K, V>>>> requestState() {
         assert future == null;
         if (trace) log.tracef("Cache %s Attempting to receive replicas for segment %s from %s with topology %s",
               cacheName, segmentId, replicaHosts, topology);

         List<CompletableFuture<Void>> completableFutures = new ArrayList<>();
         for (Address replica : replicaHosts) {
            if (replica.equals(rpcManager.getAddress())) {
               dataContainer.forEach(entry -> {
                  int keySegment = topology.getDistribution(entry.getKey()).segmentId();
                  if (keySegment == segmentId) {
                     addKeyToReplicaMap(replica, entry);
                  }
               });
            } else {
               InboundTransferTask transferTask = createTransferTask(segmentId, replica, topology);
               transferTaskMap.put(replica, transferTask);
               completableFutures.add(transferTask.requestSegments());
            }
         }

         CompletableFuture<Void> allSegmentRequests = CompletableFuture
               .allOf(completableFutures.toArray(new CompletableFuture[completableFutures.size()]));

         // If an exception is thrown by any of the inboundTransferTasks, then remove all segment results and cancel all tasks
         allSegmentRequests.exceptionally(throwable -> {
            if (trace) log.tracef(throwable, "Exception when processing InboundTransferTask for cache %s", cacheName);
            cancel(throwable);
            return null;
         });

         future = allSegmentRequests.thenApply(aVoid -> {
            List<Map<Address, CacheEntry<K, V>>> retVal = keyReplicaMap.entrySet().stream()
                  .map(Map.Entry::getValue)
                  .collect(Collectors.toList());
            clear();
            return Collections.unmodifiableList(retVal);
         });
         return future;
      }

      synchronized void clear() {
         keyReplicaMap.clear();
         transferTaskMap.clear();
         requestMap.remove(segmentId);
      }

      synchronized void receiveState(Address sender, int topologyId, Collection<StateChunk> stateChunks) {
         if (topologyId < topology.getTopologyId()) {
            if (trace)
               log.tracef("Discarding state response with old topology id %d for cache %s, the smallest allowed topology id is %d",
                     topologyId, topology.getTopologyId(), cacheName);
            return;
         }

         InboundTransferTask transferTask = transferTaskMap.get(sender);
         if (transferTask == null) {
            if (trace)
               log.tracef("State received for an unknown request. No record of a state request exists for node %s", sender);
            return;
         }

         if (trace) log.tracef("State chunks received from %s, with topologyId %s, statechunks %s", sender, topologyId, stateChunks);
         for (StateChunk chunk : stateChunks) {
            boolean isLastChunk = chunk.isLastChunk();
            chunk.getCacheEntries().forEach(ice -> addKeyToReplicaMap(sender, ice));
            transferTask.onStateReceived(chunk.getSegmentId(), isLastChunk);

            if (isLastChunk)
               transferTaskMap.remove(sender);
         }
      }

      synchronized void cancel(Throwable throwable) {
         log.debugf(throwable, "Cancelling request for segment %s on cache %s", segmentId, cacheName);
         transferTaskMap.forEach((address, inboundTransferTask) -> inboundTransferTask.cancel());
         if (throwable != null) {
            future.completeExceptionally(throwable);
         } else {
            future.cancel(true);
         }
         clear();
      }

      void addKeyToReplicaMap(Address address, CacheEntry<K, V> ice) {
         // If a map doesn't already exist for a given key, then init a map that contains all hos with a NullValueEntry
         // This is necessary to determine if a key is missing on a given host as it artificially introduces a conflict
         keyReplicaMap.computeIfAbsent(ice.getKey(), k -> {
            Map<Address, CacheEntry<K, V>> map = new HashMap<>();
            replicaHosts.forEach(a -> map.put(a, NullCacheEntry.getInstance()));
            return map;
         }).put(address, ice);
      }

      @Override
      public String toString() {
         return "SegmentRequest{" +
               "segmentId=" + segmentId +
               ", topology=" + topology.getTopologyId() +
               ", replicaHosts=" + replicaHosts +
               ", keyReplicaMap=" + keyReplicaMap +
               ", transferTaskMap=" + transferTaskMap +
               ", future=" + future +
               '}';
      }
   }
}
