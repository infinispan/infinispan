package org.infinispan.scattered.impl;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.InvalidateVersionsCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.SimpleClusteredVersion;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.filter.KeyFilter;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.persistence.manager.OrderedUpdatesManager;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.scattered.ScatteredVersionManager;
import org.infinispan.statetransfer.StateConsumer;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.ClusterTopologyManager;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class ScatteredVersionManagerImpl<K> implements ScatteredVersionManager<K> {

   private static final AtomicReferenceFieldUpdater<ScatteredVersionManagerImpl, ConcurrentMap> scheduledKeysSwapper
      = AtomicReferenceFieldUpdater.newUpdater(ScatteredVersionManagerImpl.class, ConcurrentMap.class, "scheduledKeys");
   private static final AtomicReferenceFieldUpdater<ScatteredVersionManagerImpl, ConcurrentMap> removedKeysSwapper
      = AtomicReferenceFieldUpdater.newUpdater(ScatteredVersionManagerImpl.class, ConcurrentMap.class, "removedKeys");
   private static final AtomicIntegerFieldUpdater<ScatteredVersionManagerImpl> topologyIdUpdater
         = AtomicIntegerFieldUpdater.newUpdater(ScatteredVersionManagerImpl.class, "topologyId");

   private static final Log log = LogFactory.getLog(ScatteredVersionManager.class);
   private static final boolean trace = log.isTraceEnabled();
   private Configuration configuration;
   private int invalidationBatchSize;
   private int numSegments;
   private ComponentRegistry componentRegistry;
   private ExecutorService executorService;
   private CommandsFactory commandsFactory;
   private RpcManager rpcManager;
   private DataContainer<K, ?> dataContainer;
   private RpcOptions syncIgnoreLeavers;
   private PersistenceManager persistenceManager;
   private StateConsumer stateConsumer;
   private ClusterTopologyManager clusterTopologyManager;
   private OrderedUpdatesManager orderedUpdatesManager;

   private int preloadedTopologyId = 0;
   private volatile int topologyId = 0;
   private AtomicReferenceArray<SegmentState> segmentStates;
   private AtomicReferenceArray<CompletableFuture<Void>> blockedFutures;
   private AtomicLongArray segmentVersions;
   // holds the topologies in which this node has become the owner of given segment
   private AtomicIntegerArray ownerTopologyIds;
   private volatile ConcurrentMap<K, InvalidationInfo> scheduledKeys;
   private volatile ConcurrentMap<K, InvalidationInfo> removedKeys;

   private volatile boolean transferringValues = false;
   private volatile int valuesTopology = -1;
   private CompletableFuture<Void> valuesFuture = CompletableFutures.completedNull();
   private final Object valuesLock = new Object();

   @Inject
   public void init(Configuration configuration,
                    ComponentRegistry componentRegistry,
                    @ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService executorService,
                    CommandsFactory commandsFactory,
                    RpcManager rpcManager,
                    DataContainer<K, ?> dataContainer,
                    PersistenceManager persistenceManager,
                    StateConsumer stateConsumer,
                    ClusterTopologyManager clusterTopologyManager,
                    OrderedUpdatesManager orderedUpdatesManager) {
      this.componentRegistry = componentRegistry;
      this.configuration = configuration;
      this.executorService = executorService;
      this.commandsFactory = commandsFactory;
      this.rpcManager = rpcManager;
      this.dataContainer = dataContainer;
      this.persistenceManager = persistenceManager;
      this.stateConsumer = stateConsumer;
      this.clusterTopologyManager = clusterTopologyManager;
      this.orderedUpdatesManager = orderedUpdatesManager;
   }

   @Start(priority = 15) // before StateConsumerImpl and StateTransferManagerImpl
   public void start() {
      // this assumes that number of segments does not change
      numSegments = configuration.clustering().hash().numSegments();
      segmentVersions = new AtomicLongArray(numSegments);
      segmentStates = new AtomicReferenceArray<>(numSegments);
      blockedFutures = new AtomicReferenceArray<>(numSegments);
      ownerTopologyIds = new AtomicIntegerArray(numSegments);
      CacheTopology cacheTopology = stateConsumer.getCacheTopology();
      ConsistentHash consistentHash = cacheTopology == null ? null : cacheTopology.getCurrentCH();
      for (int i = 0; i < numSegments; ++i) {
         // The component can be rewired, and then this is executed without any topology change
         SegmentState state = SegmentState.NOT_OWNED;
         if (consistentHash != null && consistentHash.isSegmentLocalToNode(rpcManager.getAddress(), i)) {
            state = SegmentState.OWNED;
         }
         segmentStates.set(i, state);
      }
      printTable();
      configuration.clustering().attributes().attribute(ClusteringConfiguration.REMOTE_TIMEOUT)
            .addListener(((a, o) -> initRpcOptions()));
      initRpcOptions();
      scheduledKeys = new ConcurrentHashMap<>(invalidationBatchSize);
      invalidationBatchSize = configuration.clustering().invalidationBatchSize();
      removedKeys = new ConcurrentHashMap<>(invalidationBatchSize);
   }

   private void initRpcOptions() {
      syncIgnoreLeavers = rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, DeliverOrder.NONE).build();
   }

   @Start(priority = 57) // just after preload
   public void initTopologyId() {
      if (persistenceManager.isPreloaded()) {
         // We don't have to do the preload, already done
         if (preloadedTopologyId > 0) {
            clusterTopologyManager.setInitialCacheTopologyId(componentRegistry.getCacheName(), preloadedTopologyId + 1);
         }
         return;
      }
      // An alternative (not implemented) approach is storing the max versions as a part of the persisted global state
      // in ScatteredConsistentHash, but that works only for the orderly shutdown.
      // TODO: implement start after shutdown
      AtomicInteger maxTopologyId = new AtomicInteger(preloadedTopologyId);
      persistenceManager.processOnAllStores(new WithinThreadExecutor(), KeyFilter.ACCEPT_ALL_FILTER,
         (marshalledEntry, taskContext) -> {
            InternalMetadata metadata = marshalledEntry.getMetadata();
            if (metadata != null) {
               EntryVersion entryVersion = metadata.version();
               if (entryVersion instanceof SimpleClusteredVersion) {
                  int entryTopologyId = ((SimpleClusteredVersion) entryVersion).topologyId;
                  if (maxTopologyId.get() < entryTopologyId) {
                     maxTopologyId.updateAndGet(current -> Math.max(current, entryTopologyId));
                  }
               }
            }
         }, false, true);
      if (maxTopologyId.get() > 0) {
         clusterTopologyManager.setInitialCacheTopologyId(componentRegistry.getCacheName(), maxTopologyId.get() + 1);
      }
   }

   @Stop
   public void stop() {
      log.trace("Stopping " + this + " on " + rpcManager.getAddress());
      synchronized (valuesLock) {
         valuesTopology = Integer.MAX_VALUE;
         valuesFuture.completeExceptionally(new CacheException("Cache is stopping"));
      }
      log.trace("Stopped " + this + " on " + rpcManager.getAddress());
   }

   @Override
   public EntryVersion incrementVersion(int segment) {
      switch (segmentStates.get(segment)) {
         case NOT_OWNED:
            throw new CacheException("Segment " + segment + " is not owned by " + rpcManager.getAddress());
         case BLOCKED:
            // If the segment is blocked, the PrefetchInterceptor should block execution until we receive
            // max version number. If the topology is changed afterwards, ScatteringInterceptor should throw OTE.
            throw new CacheException("Segment " + segment + " is currently blocked");
         case KEY_TRANSFER:
         case VALUE_TRANSFER:
         case OWNED:
            return new SimpleClusteredVersion(topologyId, segmentVersions.addAndGet(segment, 1));
         default:
            throw new IllegalStateException();
      }
   }

   @Override
   public void scheduleKeyInvalidation(K key, EntryVersion version, boolean removal) {
      ConcurrentMap<K, InvalidationInfo> scheduledKeys;
      do {
         scheduledKeys = this.scheduledKeys;
         InvalidationInfo ii = new InvalidationInfo((SimpleClusteredVersion) version, removal);
         // under race conditions the key is inserted twice but that does not matter too much
         scheduledKeys.compute(key, (k, old) -> old == null ? ii :
            ii.version > old.version || (ii.removal && ii.version == old.version) ? ii : old);
      } while (scheduledKeys != this.scheduledKeys);
      if (scheduledKeys.size() > invalidationBatchSize) {
         tryRegularInvalidations(scheduledKeys, false);
      }
   }

   protected boolean startFlush() {
      ConcurrentMap<K, InvalidationInfo> scheduledKeys = this.scheduledKeys;
      if (!scheduledKeys.isEmpty()) {
         tryRegularInvalidations(scheduledKeys, true);
         return true;
      } else {
         // when there are some invalidations, removed invalidations are triggered automatically
         // but here we have to start it manually
         ConcurrentMap<K, InvalidationInfo> removedKeys = this.removedKeys;
         if (!removedKeys.isEmpty()) {
            tryRemovedInvalidations(removedKeys);
            return true;
         } else {
            return false;
         }
      }
   }

   @Override
   public synchronized void registerSegment(int segment) {
      ownerTopologyIds.set(segment, topologyId);
      segmentVersions.set(segment, 0);
      blockedFutures.set(segment, new CompletableFuture<>());
      if (!segmentStates.compareAndSet(segment, SegmentState.NOT_OWNED, SegmentState.BLOCKED)) {
         throw new IllegalStateException("Segment " + segment + " is in state " + segmentStates.get(segment));
      } else {
         log.tracef("Node %s blocks access to segment %d", rpcManager.getAddress(), segment);
      }
   }

   @Override
   public synchronized void unregisterSegment(int segment) {
      SegmentState previous = segmentStates.getAndSet(segment, SegmentState.NOT_OWNED);
      if (trace) {
         log.tracef("Unregistered segment %d (previous=%s)", segment, previous);
      }
      CompletableFuture<Void> blockedFuture = blockedFutures.get(segment);
      if (blockedFuture != null) {
         blockedFuture.completeExceptionally(new CacheException("The segment is no longer owned."));
      }
   }

   @Override
   public boolean isVersionActual(int segment, EntryVersion version) {
      SimpleClusteredVersion clusteredVersion = (SimpleClusteredVersion) version;
      return clusteredVersion.topologyId >= ownerTopologyIds.get(segment);
   }

   @Override
   public void notifyKeyTransferFinished(int segment, boolean expectValues, boolean cancelled) {
      SegmentState update;
      if (cancelled) {
         // The transfer is cancelled when a newer topology is being installed.
         update = SegmentState.NOT_OWNED;
         assert !expectValues;
      } else if (expectValues) {
         update = SegmentState.VALUE_TRANSFER;
      } else {
         update = SegmentState.OWNED;
      }
      // It is possible that the segment is not in KEY_TRANSFER state, but can be in BLOCKED states as well
      // when the CONFIRM_REVOKED_SEGMENTS failed.
      SegmentState previous = segmentStates.getAndSet(segment, update);
      if (trace) {
         log.tracef("Finished transfer for segment %d = %s -> %s", segment, previous, update);
      }
      CompletableFuture<Void> blockedFuture = blockedFutures.get(segment);
      if (blockedFuture != null) {
         blockedFuture.completeExceptionally(new CacheException("Segment state transition did not complete correctly."));
      }
      if (trace) {
         if (expectValues) {
            log.tracef("Node %s, segment %d has all keys in, expects value transfer", rpcManager.getAddress(), segment);
         } else {
            log.tracef("Node %s, segment %d did not transfer any keys, segment is owned now", rpcManager.getAddress(), segment);
         }
      }
   }

   @Override
   public SegmentState getSegmentState(int segment) {
      return segmentStates.get(segment);
   }

   @Override
   public void setValuesTransferTopology(int topologyId) {
      log.tracef("Node will transfer value for topology %d", topologyId);
      synchronized (valuesLock) {
         transferringValues = true;
      }
   }

   @Override
   public void notifyValueTransferFinished() {
      for (int i = 0; i < numSegments; ++i) {
         LOOP: for (;;) {
            SegmentState state = segmentStates.get(i);
            switch (state) {
               case NOT_OWNED:
               case OWNED:
                  break LOOP;
               case BLOCKED:
               case KEY_TRANSFER:
                  blockedFutures.get(i).completeExceptionally(new CacheException("Failed to request versions"));
                  log.warnf("Stopped applying state for segment %d in topology %d but the segment is in state %s", i, topologyId, state);
                  // no break
               case VALUE_TRANSFER:
                  if (segmentStates.compareAndSet(i, state, SegmentState.OWNED)) {
                     break LOOP;
                  }
            }
         }
      }
      synchronized (valuesLock) {
         valuesTopology = Math.max(topologyId, valuesTopology);
         transferringValues = false;
         valuesFuture.complete(null);
         valuesFuture = new CompletableFuture<>();
      }
      log.debugf("Node %s received values for all segments in topology %d", rpcManager.getAddress(), topologyId);
   }

   @Override
   public CompletableFuture<Void> getBlockingFuture(int segment) {
      return blockedFutures.get(segment);
   }

   @Override
   public void setTopologyId(int topologyId) {
      int currentTopologyId = this.topologyId;
      if (currentTopologyId >= topologyId) {
         throw new IllegalArgumentException("Updating to topology " + topologyId + " but current is " + currentTopologyId);
      } else if (!topologyIdUpdater.compareAndSet(this, currentTopologyId, topologyId)) {
         throw new IllegalStateException("Concurrent update to topology " + topologyId +
               ", current was " + currentTopologyId + " but now it's " + this.topologyId);
      }
   }

   @Override
   public void updatePreloadedEntryVersion(EntryVersion version) {
      if (version instanceof SimpleClusteredVersion) {
         int topologyId = ((SimpleClusteredVersion) version).topologyId;
         preloadedTopologyId = Math.max(preloadedTopologyId, topologyId);
      }
   }

   @Override
   public CompletableFuture<Void> valuesFuture(int topologyId) {
      // it is possible that someone will ask with topologyId that does not belong to a rebalance,
      // while the valuesTopology is updated only on rebalance. Therefore, without the extra boolean
      // we would get stuck here.
      if (transferringValues && topologyId > valuesTopology) {
         synchronized (valuesLock) {
            if (transferringValues && topologyId > valuesTopology) {
               return valuesFuture.thenCompose(nil -> valuesFuture(topologyId));
            }
         }
      }
      return CompletableFutures.completedNull();
   }

   @Override
   public void setOwnedSegments(Set<Integer> segments) {
      for (int segment : segments) {
         segmentVersions.set(segment, 0);
         ownerTopologyIds.set(segment, topologyId);
         if (!segmentStates.compareAndSet(segment, SegmentState.NOT_OWNED, SegmentState.OWNED)) {
            throw new IllegalStateException("Segment %d is in state " + segmentStates.get(segment));
         }
      }
      if (log.isDebugEnabled()) {
         log.debugf("Node %s is now owner of segments %s", rpcManager.getAddress(), sorted(segments));
         printTable();
      }
   }

   @Override
   public void startKeyTransfer(Set<Integer> segments) {
      for (int segment : segments) {
         if (!segmentStates.compareAndSet(segment, SegmentState.BLOCKED, SegmentState.KEY_TRANSFER)) {
            throw new IllegalStateException("Segment " + segment + " is in state " + segmentStates.get(segment));
         }
         blockedFutures.get(segment).complete(null);
         log.tracef("Node %s, segment %d expects key transfer", rpcManager.getAddress(), segment);
      }
   }

   private void printTable() {
      StringBuilder sb = new StringBuilder("Segments for node ").append(rpcManager.getAddress()).append(':');
      for (int i = 0; i < numSegments; i += 16) {
         sb.append('\n');
         for (int j = 0; j < 16 && i + j < numSegments; ++j) {
            sb.append(String.format("%4d=%c ", i + j, segmentStates.get(i + j).singleChar()));
         }
      }
      log.debug(sb.toString());
   }

   private List<Integer> sorted(Set<Integer> segments) {
      Integer[] array = segments.toArray(new Integer[segments.size()]);
      Arrays.sort(array);
      return Arrays.asList(array);
   }

   private void tryRegularInvalidations(ConcurrentMap<K, InvalidationInfo> scheduledKeys, boolean force) {
      if (!scheduledKeysSwapper.compareAndSet(this, scheduledKeys, new ConcurrentHashMap<>(invalidationBatchSize))) {
         // ignore if there are two concurrent updates
         return;
      }
      executorService.execute(() -> {
         // we'll invalidate all keys in one run
         // we don't have to keep any topology lock, because the versions increase monotonically
         int numKeys = scheduledKeys.size();
         Object[] keys = new Object[numKeys];
         int[] topologyIds = new int[numKeys];
         long[] versions = new long[numKeys];
         boolean[] isRemoved = new boolean[numKeys];
         int numRemoved = 0;
         int i = 0;
         for (Map.Entry<K, InvalidationInfo> entry : scheduledKeys.entrySet()) {
            keys[i] = entry.getKey();
            topologyIds[i] = entry.getValue().topologyId;
            versions[i] = entry.getValue().version;
            if (isRemoved[i] = entry.getValue().removal) { // intentional assignment
               numRemoved++;
            }
            if (++i > numKeys) {
               // concurrent modification?
               numKeys = scheduledKeys.size();
               keys = Arrays.copyOf(keys, numKeys);
               topologyIds = Arrays.copyOf(topologyIds, numKeys);
               versions = Arrays.copyOf(versions, numKeys);
               isRemoved = Arrays.copyOf(isRemoved, numKeys);
            }
         }
         InvalidateVersionsCommand command = commandsFactory.buildInvalidateVersionsCommand(-1, keys, topologyIds, versions, false);
         sendRegularInvalidations(command, keys, topologyIds, versions, numRemoved, isRemoved, force);
      });
   }

   private void sendRegularInvalidations(InvalidateVersionsCommand command, Object[] keys, int[] topologyIds, long[] versions, int numRemoved, boolean[] isRemoved, boolean force) {
      CompletableFuture<Map<Address, Response>> future = rpcManager.invokeRemotelyAsync(null, command, syncIgnoreLeavers);
      future.whenComplete((r, t) -> {
         if (t != null) {
            log.failedInvalidatingRemoteCache(t);
            sendRegularInvalidations(command, keys, topologyIds, versions, numRemoved, isRemoved, force);
         } else if (numRemoved > 0 || force) {
            regularInvalidationFinished(keys, topologyIds, versions, isRemoved, force);
         }
      });
   }

   protected void regularInvalidationFinished(Object[] keys, int[] topologyIds, long[] versions, boolean[] isRemoved, boolean force) {
      ConcurrentMap<K, InvalidationInfo> removedKeys;
      do {
         removedKeys = this.removedKeys;
         for (int i = 0; i < isRemoved.length; ++i) {
            if (isRemoved[i]) {
               int topologyId = topologyIds[i];
               long version = versions[i];
               removedKeys.compute((K) keys[i], (k, prev) -> {
                  if (prev == null || prev.topologyId < topologyId
                        || prev.topologyId == topologyId && prev.version < version) {
                     return new InvalidationInfo(topologyId, version);
                  } else {
                     return prev;
                  }
               });
            }
         }
      } while (removedKeys != this.removedKeys);
      if (removedKeys.size() > invalidationBatchSize || (force && !removedKeys.isEmpty())) {
         tryRemovedInvalidations(removedKeys);
      }
   }

   private void tryRemovedInvalidations(ConcurrentMap<K, InvalidationInfo> removedKeys) {
      if (removedKeysSwapper.compareAndSet(this, removedKeys, new ConcurrentHashMap<>(invalidationBatchSize))) {
         final ConcurrentMap<K, InvalidationInfo> rk = removedKeys;
         executorService.execute(() -> {
            int numKeys = rk.size();
            Object[] keys = new Object[numKeys];
            int[] topologyIds = new int[numKeys];
            long[] versions = new long[numKeys];
            int i = 0;
            for (Map.Entry<K, InvalidationInfo> entry : rk.entrySet()) {
               keys[i] = entry.getKey();
               topologyIds[i] = entry.getValue().topologyId;
               versions[i] = entry.getValue().version;
               if (++i > numKeys) {
                  numKeys = rk.size();
                  keys = Arrays.copyOf(keys, numKeys);
                  topologyIds = Arrays.copyOf(topologyIds, numKeys);
                  versions = Arrays.copyOf(versions, numKeys);
               }
            }
            InvalidateVersionsCommand removeCommand = commandsFactory.buildInvalidateVersionsCommand(-1, keys, topologyIds, versions, true);
            sendRemoveInvalidations(removeCommand);
         });
      }
   }

   private void sendRemoveInvalidations(InvalidateVersionsCommand removeCommand) {
      rpcManager.invokeRemotelyAsync(null, removeCommand, syncIgnoreLeavers).whenComplete((r, t) -> {
         if (t != null) {
            log.failedInvalidatingRemoteCache(t);
            sendRemoveInvalidations(removeCommand);
         } else {
            removeInvalidationsFinished();
         }
      });
      // remove the entries on self, too
      removeCommand.init(dataContainer, orderedUpdatesManager, null, null, null);
      removeCommand.invokeAsync();
   }

   protected void removeInvalidationsFinished() {
      // testing hook
   }

   @Override
   public void clearInvalidations() {
      scheduledKeysSwapper.set(this, new ConcurrentHashMap<>(invalidationBatchSize));
      removedKeysSwapper.set(this, new ConcurrentHashMap<>(invalidationBatchSize));
   }

   private static class InvalidationInfo {
      public final int topologyId;
      public final long version;
      public final boolean removal;

      private InvalidationInfo(SimpleClusteredVersion version, boolean removal) {
         this.topologyId = version.topologyId;
         this.version = version.version;
         this.removal = removal;
      }

      private InvalidationInfo(int topologyId, long version) {
         this.topologyId = topologyId;
         this.version = version;
         this.removal = true;
      }
   }
}
