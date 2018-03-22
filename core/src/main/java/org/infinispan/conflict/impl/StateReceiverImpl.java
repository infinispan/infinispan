package org.infinispan.conflict.impl;

import static org.infinispan.factories.KnownComponentNames.STATE_TRANSFER_EXECUTOR;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
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
import org.infinispan.executors.LimitedExecutor;
import org.infinispan.factories.annotations.ComponentName;
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
   @Inject @ComponentName(STATE_TRANSFER_EXECUTOR)
   private ExecutorService stateTransferExecutor;

   private String cacheName;
   private LimitedExecutor stateReceiverExecutor;

   private final ConcurrentHashMap<Integer, SegmentRequest> requestMap = new ConcurrentHashMap<>();

   @Start
   public void start() {
      this.cache.addListener(this);
      this.cacheName = cache.getName();
      this.stateReceiverExecutor = new LimitedExecutor("StateReceiver-" + cacheName, stateTransferExecutor, 1);
   }

   @Override
   @Stop
   public void stop() {
      if (trace) log.tracef("Cache %s stop() called on StateReceiverImpl", cacheName);
      for (SegmentRequest request : requestMap.values())
         request.cancel(null);
      stateReceiverExecutor.cancelQueuedTasks();
   }

   @DataRehashed
   @SuppressWarnings("WeakerAccess")
   public void onDataRehash(DataRehashedEvent dataRehashedEvent) {
      if (dataRehashedEvent.isPre()) {
         if (trace) log.tracef("Cache %s received event: %s", cacheName, dataRehashedEvent);
         for (SegmentRequest request : requestMap.values())
            request.cancel(new CacheException("Cancelling replica request as the owners of the requested " +
               "segment have changed."));
         stateReceiverExecutor.cancelQueuedTasks();
      }
   }

   @Override
   public CompletableFuture<List<Map<Address, CacheEntry<K, V>>>> getAllReplicasForSegment(int segmentId, LocalizedCacheTopology topology, long timeout) {
      return requestMap.computeIfAbsent(segmentId, id -> new SegmentRequest(id, topology, timeout)).requestState();
   }

   @Override
   public void receiveState(Address sender, int topologyId, Collection<StateChunk> stateChunks) {
      if (stateChunks.isEmpty()) {
         if (trace)
            log.tracef("Cache %s ignoring received state from %s because stateChunks are empty", cacheName, sender);
         return;
      }

      int segmentId = stateChunks.iterator().next().getSegmentId();
      SegmentRequest request = requestMap.get(segmentId);
      if (request == null) {
         if (trace) log.tracef("Cache %s ignoring received state because the associated request was completed or cancelled", cacheName);
         return;
      }

      if (trace) log.tracef("Cache %s received state for %s", cacheName, request);
      request.receiveState(sender, topologyId, stateChunks);
   }

   Map<K, Map<Address, CacheEntry<K, V>>> getKeyReplicaMap(int segmentId) {
      return requestMap.get(segmentId).keyReplicaMap;
   }

   Map<Address, InboundTransferTask> getTransferTaskMap(int segmentId) {
      return requestMap.get(segmentId).transferTaskMap;
   }

   InboundTransferTask createTransferTask(int segmentId, Address source, CacheTopology topology, long transferTimeout) {
      return new InboundTransferTask(Collections.singleton(segmentId), source, topology.getTopologyId(),
            rpcManager, commandsFactory, transferTimeout, cacheName, false);
   }

   class SegmentRequest {
      final int segmentId;
      final LocalizedCacheTopology topology;
      final long timeout;
      final List<Address> replicaHosts;
      final Map<K, Map<Address, CacheEntry<K, V>>> keyReplicaMap = new HashMap<>();
      final Map<Address, InboundTransferTask> transferTaskMap = new ConcurrentHashMap<>();
      CompletableFuture<List<Map<Address, CacheEntry<K, V>>>> future;

      SegmentRequest(int segmentId, LocalizedCacheTopology topology, long timeout) {
         this.segmentId = segmentId;
         this.topology = topology;
         this.timeout = timeout;
         this.replicaHosts = topology.getDistributionForSegment(segmentId).writeOwners();
      }

      synchronized CompletableFuture<List<Map<Address, CacheEntry<K, V>>>> requestState() {
         assert future == null;
         if (trace) log.tracef("Cache %s attempting to receive replicas for segment %s from %s with topology %s",
               cacheName, segmentId, replicaHosts, topology);

         for (final Address replica : replicaHosts) {
            if (replica.equals(rpcManager.getAddress())) {
               dataContainer.forEach(entry -> {
                  int keySegment = topology.getDistribution(entry.getKey()).segmentId();
                  if (keySegment == segmentId) {
                     addKeyToReplicaMap(replica, entry);
                  }
               });
            } else {
               final InboundTransferTask transferTask = createTransferTask(segmentId, replica, topology, timeout);
               transferTaskMap.put(replica, transferTask);

               stateReceiverExecutor.executeAsync(() -> {
                  // If the transferTaskMap does not contain an entry for this replica, then it must have been cancelled
                  // before this request was executed..
                  if (!transferTaskMap.containsKey(replica))
                     return null;

                  CompletableFuture<Void> transferStarted = transferTask.requestSegments();
                  return transferStarted.exceptionally(throwable -> {
                     if (trace) log.tracef(throwable, "Cache %s exception when processing InboundTransferTask", cacheName);
                     cancel(throwable);
                     return null;
                  });
               });
            }
         }
         future = new CompletableFuture<>();
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
               log.tracef("Cache %s discarding state response with old topology id %d, the smallest allowed topology id is %d",
                     topologyId, topology.getTopologyId(), cacheName);
            return;
         }

         InboundTransferTask transferTask = transferTaskMap.get(sender);
         if (transferTask == null) {
            if (trace)
               log.tracef("Cache %s state received for an unknown request. No record of a state request exists for node %s", cacheName, sender);
            return;
         }

         if (trace) log.tracef("Cache %s state chunks received from %s, with topologyId %s, statechunks %s", cacheName, sender, topologyId, stateChunks);
         for (StateChunk chunk : stateChunks) {
            boolean isLastChunk = chunk.isLastChunk();
            chunk.getCacheEntries().forEach(ice -> addKeyToReplicaMap(sender, ice));
            transferTask.onStateReceived(chunk.getSegmentId(), isLastChunk);

            if (isLastChunk) {
               transferTaskMap.remove(sender);

               if (transferTaskMap.isEmpty()) {
                  List<Map<Address, CacheEntry<K, V>>> retVal = keyReplicaMap.entrySet().stream()
                        .map(Map.Entry::getValue)
                        .collect(Collectors.toList());
                  clear();

                  future.complete(Collections.unmodifiableList(retVal));
               }
            }
         }
      }

      synchronized void cancel(Throwable throwable) {
         log.debugf(throwable, "Cache %s cancelling request for segment %s", cacheName, segmentId);
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
