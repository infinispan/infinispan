package org.infinispan.conflict.impl;

import static org.infinispan.factories.KnownComponentNames.CACHE_NAME;
import static org.infinispan.factories.KnownComponentNames.NON_BLOCKING_EXECUTOR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.IntSets;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.NullCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.executors.LimitedExecutor;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheNotifier;
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
@Scope(Scopes.NAMED_CACHE)
public class StateReceiverImpl<K, V> implements StateReceiver<K, V> {

   private static final Log log = LogFactory.getLog(StateReceiverImpl.class);

   @ComponentName(CACHE_NAME)
   @Inject String cacheName;
   @Inject CacheNotifier<K, V> cacheNotifier;
   @Inject CommandsFactory commandsFactory;
   @Inject InternalDataContainer<K, V> dataContainer;
   @Inject RpcManager rpcManager;
   @Inject @ComponentName(NON_BLOCKING_EXECUTOR)
   ExecutorService nonBlockingExecutor;

   private LimitedExecutor stateReceiverExecutor;

   private final ConcurrentHashMap<Integer, SegmentRequest> requestMap = new ConcurrentHashMap<>();

   @Start
   public void start() {
      cacheNotifier.addListener(this);
      stateReceiverExecutor = new LimitedExecutor("StateReceiver-" + cacheName, nonBlockingExecutor, 1);
   }

   @Stop
   public void stop() {
      cancelRequests();
      stateReceiverExecutor.shutdownNow();
   }

   @Override
   public void cancelRequests() {
      if (log.isTraceEnabled()) log.tracef("Cache %s stop() called on StateReceiverImpl", cacheName);
      for (SegmentRequest request : requestMap.values()) {
         request.cancel(null);
      }
   }

   @DataRehashed
   @SuppressWarnings("WeakerAccess")
   public void onDataRehash(DataRehashedEvent<K, V> dataRehashedEvent) {
      if (dataRehashedEvent.isPre()) {
         if (log.isTraceEnabled()) log.tracef("Cache %s received event: %s", cacheName, dataRehashedEvent);
         for (SegmentRequest request : requestMap.values())
            request.cancel(new CacheException("Cancelling replica request as the owners of the requested " +
               "segment have changed."));
      }
   }

   @Override
   public CompletableFuture<List<Map<Address, CacheEntry<K, V>>>> getAllReplicasForSegment(int segmentId, LocalizedCacheTopology topology, long timeout) {
      return requestMap.computeIfAbsent(segmentId, id -> new SegmentRequest(id, topology, timeout)).requestState();
   }

   @Override
   public void receiveState(Address sender, int topologyId, Collection<StateChunk> stateChunks) {
      if (stateChunks.isEmpty()) {
         if (log.isTraceEnabled())
            log.tracef("Cache %s ignoring received state from %s because stateChunks are empty", cacheName, sender);
         return;
      }

      int segmentId = stateChunks.iterator().next().getSegmentId();
      SegmentRequest request = requestMap.get(segmentId);
      if (request == null) {
         if (log.isTraceEnabled()) log.tracef("Cache %s ignoring received state because the associated request was completed or cancelled", cacheName);
         return;
      }

      if (log.isTraceEnabled()) log.tracef("Cache %s received state for %s", cacheName, request);
      request.receiveState(sender, topologyId, stateChunks);
   }

   Map<K, Map<Address, CacheEntry<K, V>>> getKeyReplicaMap(int segmentId) {
      return requestMap.get(segmentId).keyReplicaMap;
   }

   Map<Address, InboundTransferTask> getTransferTaskMap(int segmentId) {
      return requestMap.get(segmentId).transferTaskMap;
   }

   InboundTransferTask createTransferTask(int segmentId, Address source, CacheTopology topology, long transferTimeout) {
      return new InboundTransferTask(IntSets.immutableSet(segmentId), source, topology.getTopologyId(),
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
         this.replicaHosts = topology.getSegmentDistribution(segmentId).writeOwners();
      }

      synchronized CompletableFuture<List<Map<Address, CacheEntry<K, V>>>> requestState() {
         if (future != null) {
            assert future.isCompletedExceptionally();
            if (log.isTraceEnabled()) log.tracef("Cache %s already cancelled replicas request for segment %s from %s with topology %s",
                                  cacheName, segmentId, replicaHosts, topology);
            return future;
         }

         if (log.isTraceEnabled()) log.tracef("Cache %s attempting to receive replicas for segment %s from %s with topologyId=%s, timeout=%d",
               cacheName, segmentId, replicaHosts, topology.getTopologyId(), timeout);

         future = new CompletableFuture<>();
         future.whenComplete((v, t) -> {
            if (t != null) {
               if (log.isTraceEnabled()) log.tracef("Cache %s segment request(s) cancelled due to exception=%s", cacheName, t);
               // If an exception has occurred, possibly a CancellationException, we must must cancel all ongoing transfers
               cancel(t);
            }
         });

         for (final Address replica : replicaHosts) {
            if (replica.equals(rpcManager.getAddress())) {
               dataContainer.forEach(entry -> {
                  int keySegment = topology.getDistribution(entry.getKey()).segmentId();
                  if (keySegment == segmentId) {
                     addKeyToReplicaMap(replica, entry);
                  }
               });
               // numOwner == 1, then we cannot rely on receiveState to complete the future
               if (replicaHosts.size() == 1) {
                  completeRequest();
               }
            } else {
               final InboundTransferTask transferTask = createTransferTask(segmentId, replica, topology, timeout);
               transferTaskMap.put(replica, transferTask);

               stateReceiverExecutor.execute(() -> {
                  // If the transferTaskMap does not contain an entry for this replica, then it must have been cancelled
                  // before this request was executed..
                  if (!transferTaskMap.containsKey(replica))
                     return;

                  transferTask.requestSegments().exceptionally(throwable -> {
                     if (log.isTraceEnabled()) log.tracef(throwable, "Cache %s exception when processing InboundTransferTask", cacheName);
                     cancel(throwable);
                     return null;
                  });
               });
            }
         }
         return future;
      }

      synchronized void clear() {
         keyReplicaMap.clear();
         transferTaskMap.clear();
         requestMap.remove(segmentId);
      }

      synchronized void receiveState(Address sender, int topologyId, Collection<StateChunk> stateChunks) {
         if (topologyId < topology.getTopologyId()) {
            if (log.isTraceEnabled())
               log.tracef("Cache %s discarding state response with old topology id %d, the smallest allowed topology id is %d",
                     topologyId, topology.getTopologyId(), cacheName);
            return;
         }

         InboundTransferTask transferTask = transferTaskMap.get(sender);
         if (transferTask == null) {
            if (log.isTraceEnabled())
               log.tracef("Cache %s state received for an unknown request. No record of a state request exists for node %s", cacheName, sender);
            return;
         }

         if (log.isTraceEnabled()) log.tracef("Cache %s state chunks received from %s, with topologyId %s, statechunks %s", cacheName, sender, topologyId, stateChunks);
         for (StateChunk chunk : stateChunks) {
            boolean isLastChunk = chunk.isLastChunk();
            chunk.getCacheEntries().forEach(ice -> addKeyToReplicaMap(sender, (CacheEntry<K, V>) ice));
            transferTask.onStateReceived(chunk.getSegmentId(), isLastChunk);

            if (isLastChunk) {
               transferTaskMap.remove(sender);

               if (transferTaskMap.isEmpty()) {
                  completeRequest();
               }
            }
         }
      }

      synchronized void cancel(Throwable throwable) {
         if (future.isDone())
            return;

         log.debugf(throwable, "Cache %s cancelling request for segment %s", cacheName, segmentId);
         if (future == null) {
            // requestState() has not run yet, so we create the future first
            future = new CompletableFuture<>();
         }
         if (throwable != null) {
            future.completeExceptionally(throwable);
         } else {
            future.cancel(true);
         }
         transferTaskMap.forEach((address, inboundTransferTask) -> inboundTransferTask.cancel());
         clear();
      }

      synchronized void completeRequest() {
         List<Map<Address, CacheEntry<K, V>>> retVal = new ArrayList<>(keyReplicaMap.values());
         clear();
         future.complete(Collections.unmodifiableList(retVal));
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
