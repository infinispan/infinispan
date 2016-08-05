package org.infinispan.scattered.impl;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.InvalidateVersionsCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.RemoteMetadata;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.filter.KeyFilter;
import org.infinispan.globalstate.GlobalStateManager;
import org.infinispan.globalstate.GlobalStateProvider;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.metadata.InternalMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.scattered.ScatteredVersionManager;
import org.infinispan.statetransfer.StateConsumer;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.ReadOnlyDataContainerBackedKeySet;
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

   private static final Log log = LogFactory.getLog(ScatteredVersionManager.class);
   private static final boolean trace = log.isTraceEnabled();
   private Configuration configuration;
   private int invalidationBatchSize;
   private int numSegments;
   private ExecutorService executorService;
   private CommandsFactory commandsFactory;
   private RpcManager rpcManager;
   private DataContainer<K, ?> dataContainer;
   private RpcOptions syncRpcOptions;
   private PersistenceManager persistenceManager;
   private StateConsumer stateConsumer;

   private AtomicReferenceArray<SegmentState> segmentStates;
   private AtomicLongArray stateTransferVersions;
   private AtomicLongArray segmentVersions;
   private volatile ConcurrentMap<K, InvalidationInfo> scheduledKeys;
   private volatile ConcurrentMap<K, Long> removedKeys;

   private int maxVersionsTopology = -1;
   private Object maxVersionsLock = new Object();
   private CompletableFuture<long[]> maxVersions;

   private volatile boolean transferingValues = false;
   private volatile int valuesTopology = -1;
   private Object valuesLock = new Object();

   @Inject
   public void init(Configuration configuration,
                    @ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService executorService,
                    CommandsFactory commandsFactory,
                    RpcManager rpcManager,
                    DataContainer<K, ?> dataContainer,
                    PersistenceManager persistenceManager,
                    StateConsumer stateConsumer) {
      this.configuration = configuration;
      this.executorService = executorService;
      this.commandsFactory = commandsFactory;
      this.rpcManager = rpcManager;
      this.dataContainer = dataContainer;
      this.persistenceManager = persistenceManager;
      this.stateConsumer = stateConsumer;
   }

   @Start(priority = 15) // before StateConsumerImpl and StateTransferManagerImpl
   public void start() {
      // this assumes that number of segments does not change
      numSegments = configuration.clustering().hash().numSegments();
      stateTransferVersions = new AtomicLongArray(numSegments);
      segmentVersions = new AtomicLongArray(numSegments);
      // TODO: after preload, the entries can already have some versions. Update based on these
      segmentStates = new AtomicReferenceArray<>(numSegments);
      CacheTopology cacheTopology = stateConsumer.getCacheTopology();
      ConsistentHash consistentHash = cacheTopology == null ? null : cacheTopology.getReadConsistentHash();
      for (int i = 0; i < numSegments; ++i) {
         // The component can be rewired, and then this is executed without any topology change
         SegmentState state = SegmentState.NOT_OWNED;
         if (consistentHash != null && consistentHash.isSegmentLocalToNode(rpcManager.getAddress(), i)) {
            state = SegmentState.OWNED;
         }
         segmentStates.set(i, state);
         log.tracef("Node %s, segment %d, setting state to %s", rpcManager.getAddress(), i, state);
      }
      syncRpcOptions = rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, DeliverOrder.NONE).build();
      scheduledKeys = new ConcurrentHashMap<>(invalidationBatchSize);
      invalidationBatchSize = configuration.clustering().invalidationBatchSize();
      removedKeys = new ConcurrentHashMap<>(invalidationBatchSize);
   }

   /*@Start(priority = 61)
   public void initSegmentVersions() {
      // This is a forced preload but only after we have cache topology installed (and we can check segments, therefore)
      CacheTopology cacheTopology = stateConsumer.getCacheTopology();
      // during start from state, topology can be null here
      ConsistentHash ch = cacheTopology.getWriteConsistentHash();
      persistenceManager.processOnAllStores(new WithinThreadExecutor(), KeyFilter.ACCEPT_ALL_FILTER,
         (marshalledEntry, taskContext) -> {
            InternalMetadata metadata = marshalledEntry.getMetadata();
            if (metadata != null) {
               EntryVersion entryVersion = metadata.version();
               if (entryVersion instanceof NumericVersion) {
                  long version = ((NumericVersion) entryVersion).getVersion();
                  int segment = ch.getSegment(marshalledEntry.getKey());
                  long currentVersion = segmentVersions.get(segment);
                  while (version > currentVersion) {
                     if (segmentVersions.compareAndSet(segment, currentVersion, version)) break;
                     currentVersion = segmentVersions.get(segment);
                  }
               }
            }
         }, false, true);
   }   */

   @Stop
   public void stop() {
      log.trace("Stopping " + this + " on " + rpcManager.getAddress());
      synchronized (valuesLock) {
         valuesTopology = Integer.MAX_VALUE;
         valuesLock.notifyAll();
      }
      log.trace("Stopped " + this + " on " + rpcManager.getAddress());
   }

   @Override
   public long incrementVersion(int segment) {
      switch (segmentStates.get(segment)) {
         case NOT_OWNED:
            throw new CacheException("Segment " + segment + " is not owned by " + rpcManager.getAddress());
         case BLOCKED:
            return Long.MIN_VALUE;
         case KEY_TRANSFER:
         case VALUE_TRANSFER:
         case OWNED:
            return segmentVersions.addAndGet(segment, 1);
         default:
            throw new IllegalStateException();
      }
   }

   @Override
   public void scheduleKeyInvalidation(K key, long version, boolean removal) {
      ConcurrentMap<K, InvalidationInfo> scheduledKeys;
      do {
         scheduledKeys = this.scheduledKeys;
         InvalidationInfo ii = new InvalidationInfo(version, removal);
         // under race conditions the key is inserted twice but that does not matter too much
         scheduledKeys.compute(key, (k, old) -> old == null ? ii :
            ii.version > old.version || (ii.removal && ii.version == old.version) ? ii : old);
      } while (scheduledKeys != this.scheduledKeys);
      if (scheduledKeys.size() > invalidationBatchSize) {
         tryRegularInvalidations(scheduledKeys, false);
      }
   }

   @Override
   public boolean startFlush() {
      ConcurrentMap<K, InvalidationInfo> scheduledKeys = this.scheduledKeys;
      if (!scheduledKeys.isEmpty()) {
         tryRegularInvalidations(scheduledKeys, true);
         return true;
      } else {
         // when there are some invalidations, removed invalidations are triggered automatically
         // but here we have to start it manually
         ConcurrentMap<K, Long> removedKeys = this.removedKeys;
         if (!removedKeys.isEmpty()) {
            tryRemovedInvalidations(removedKeys);
            return true;
         } else {
            return false;
         }
      }
   }

   @Override
   public void registerSegment(int segment) {
      if (!segmentStates.compareAndSet(segment, SegmentState.NOT_OWNED, SegmentState.BLOCKED)) {
         throw new IllegalStateException("Segment " + segment + " in in state " + segmentStates.get(segment));
      } else {
         log.tracef("Node %s blocks access to segment %d", rpcManager.getAddress(), segment);
      }
   }

   @Override
   public void unregisterSegment(int segment) {
      segmentStates.set(segment, SegmentState.NOT_OWNED);
      log.tracef("Node %s now does not own segment %d", rpcManager.getAddress(), segment);
   }

   @Override
   public void setSegmentVersion(int segment, long version) {
      long prev = segmentVersions.get(segment);
      if (version < prev) {
         throw new IllegalStateException("Segment: " + segment + ", previous: " + prev + ", new: " + version);
      }
      stateTransferVersions.set(segment, version);
      if (!segmentVersions.compareAndSet(segment, prev, version)) {
         throw new IllegalStateException();
      }
      if (!segmentStates.compareAndSet(segment, SegmentState.BLOCKED, SegmentState.KEY_TRANSFER)) {
         throw new IllegalStateException("Segment " + segment + " is in state " + segmentStates.get(segment));
      }
      log.tracef("Node %s, segment %d has version %d, expects key transfer", rpcManager.getAddress(), segment, version);
   }

   @Override
   public boolean isVersionActual(int segment, long version) {
      return version > stateTransferVersions.get(segment);
   }

   @Override
   public void keyTransferFinished(int segment, boolean expectValues) {
      SegmentState update = expectValues ? SegmentState.VALUE_TRANSFER : SegmentState.OWNED;
      SegmentState previous;
      do {
         previous = segmentStates.get(segment);
         // It is possible that the segment is not in KEY_TRANSFER state, but can be in BLOCKED states as well
         // when the GET_MAX_VERSIONS failed.
         log.tracef("Finishing transfer for segment %d = %s -> %s", segment, previous, update);
      } while (!segmentStates.compareAndSet(segment, previous, update));
      if (trace) {
         if (expectValues) {
            log.tracef("Node %s, segment %d has all keys in, expects value transfer", rpcManager.getAddress(), segment);
         } else {
            log.tracef("Node %s, segment %d did not transfer any keys, segment is owned now", rpcManager.getAddress(), segment);
         }
      }
   }

   @Override
   public CompletableFuture<long[]> computeMaxVersions(CacheTopology cacheTopology) {
      CompletableFuture<long[]> maxVersions;
      synchronized (maxVersionsLock) {
         log.tracef("Requesting max versions for topology %d, last versions topology is %d", cacheTopology.getTopologyId(), maxVersionsTopology);
         if (cacheTopology.getTopologyId() > maxVersionsTopology) {
            maxVersionsTopology = cacheTopology.getTopologyId();
            this.maxVersions = maxVersions = new CompletableFuture<>();
         } else {
            log.trace("Computing max versions already in progress");
            return this.maxVersions;
         }
      }
      executorService.submit(() -> {
         long[] versions = new long[configuration.clustering().hash().numSegments()];
         // we use readCH here as it contains the old segments for which we were an owner before the topology change
         ConsistentHash consistentHash = cacheTopology.getReadConsistentHash();
         // We have to initialize even the non-owned versions as we could fail later in #setSegmentVersions
         // if we agreed to version 0 and there have been writes to the segment in the past. This happens
         // when the cache is cleared and then primary crashes - we don't find any version anywhere, noone was
         // previous owner of the segment but the local segment version is > 0.
         for (int segment = 0; segment < consistentHash.getNumSegments(); ++segment) {
            versions[segment] = segmentVersions.get(segment);
         }
         // TODO: don't iterate through all entries if no primary was lost
         for (InternalCacheEntry ice : dataContainer) {
            Metadata metadata = ice.getMetadata();
            if (metadata == null) {
               log.tracef("No metadata in %s (segment %d)", ice, consistentHash.getSegment(ice.getKey()));
               continue;
            }
            NumericVersion version = (NumericVersion) metadata.version();
            int segment = consistentHash.getSegment(ice.getKey());
            // for determining segments readCH/writeCH does not matter
            log.tracef("Key %s segment %d version %d", ice.getKey(), segment, version.getVersion());
            if (version != null && version.getVersion() > versions[segment]) {
               versions[segment] = version.getVersion();
            }
         }

         AdvancedCacheLoader stProvider = persistenceManager.getStateTransferProvider();
         if (stProvider != null) {
            try {
               CollectionKeyFilter filter = new CollectionKeyFilter(new ReadOnlyDataContainerBackedKeySet(dataContainer));
               AdvancedCacheLoader.CacheLoaderTask task = (me, taskContext) -> {
                  try {
                     int segment = consistentHash.getSegment(me.getKey());
                     Metadata metadata = me.getMetadata();
                     if (metadata == null) {
                        return;
                     }
                     NumericVersion version = (NumericVersion) metadata.version();
                     if (version != null && version.getVersion() > versions[segment]) {
                        versions[segment] = version.getVersion();
                     }
                  } catch (CacheException e) {
                     log.failedLoadingValueFromCacheStore(me.getKey(), e);
                  }
               };
               stProvider.process(filter, task, new WithinThreadExecutor(), true, true);
            } catch (CacheException e) {
               log.failedLoadingKeysFromCacheStore(e);
            }
         }
         maxVersions.complete(versions);
         log.tracef("Node %s, computation of max versions complete, versions are: %s", rpcManager.getAddress(), Arrays.toString(versions));
      });
      return maxVersions;
   }

   @Override
   public SegmentState getSegmentState(int segment) {
      return segmentStates.get(segment);
   }

   @Override
   public void setValuesTransferTopology(int topologyId) {
      log.tracef("Node will transfer value for topology %d", topologyId);
      synchronized (valuesLock) {
         transferingValues = true;
      }
   }

   @Override
   public void valuesReceived(int topologyId) {
      for (int i = 0; i < numSegments; ++i) {
         do {
            SegmentState state = segmentStates.get(i);
            switch (state) {
               case BLOCKED:
               case KEY_TRANSFER:
                  log.warnf("Stopped applying state for segment %d in topology %d but the segment is in state %s", i, topologyId, state);
                  // no break
               case VALUE_TRANSFER:
                  if (!segmentStates.compareAndSet(i, state, SegmentState.OWNED)) {
                     continue; // goto please
                  }
            }
         } while (false);
      }
      synchronized (valuesLock) {
         valuesTopology = Math.max(topologyId, valuesTopology);
         transferingValues = false;
         valuesLock.notifyAll();
      }
      log.debugf("Node %s received values for all segments in topology %d", rpcManager.getAddress(), topologyId);
   }

   @Override
   public void waitForValues(int topologyId) {
      // TODO: I am not 100% confident about this
      // it is possible that someone will query use with topologyId that does not belong to a rebalance,
      // while the valuesTopology is updated only on rebalance. Therefore, without the extra boolean
      // we would get stuck here.
      if (!transferingValues || topologyId <= valuesTopology) {
         return;
      }
      synchronized (valuesLock) {
         long now = System.currentTimeMillis();
         long deadline = now + configuration.clustering().stateTransfer().timeout();
         while (transferingValues && valuesTopology < topologyId) {
            if (now >= deadline) {
               throw new IllegalStateException("Timed out waiting for values for topology " + topologyId);
            }
            log.tracef("Waiting for values from topology %d, current is %d", topologyId, valuesTopology);
            try {
               valuesLock.wait(deadline - now);
            } catch (InterruptedException e) {
               Thread.currentThread().interrupt();
               throw new CacheException("Interrupted when waiting for values for topology " + topologyId + ", current values topology is " + valuesTopology);
            }
            now = System.currentTimeMillis();
         }
         log.tracef("Finished waiting for values from topology %s", topologyId);
      }
   }

   @Override
   public void setOwnedSegments(Set<Integer> segments) {
      for (int segment : segments) {
         segmentStates.compareAndSet(segment, SegmentState.NOT_OWNED, SegmentState.OWNED);
      }
      if (log.isDebugEnabled()) {
         log.debugf("Node %s is now owner of segments %s", rpcManager.getAddress(), sorted(segments));
         printTable();
      }
   }

   @Override
   public void setNonOwnedSegments(Set<Integer> segments) {
      for (int segment : segments) {
         segmentStates.set(segment, SegmentState.NOT_OWNED);
      }
      if (log.isDebugEnabled()) {
         log.debugf("Node %s is no longer owner of segments %s", rpcManager.getAddress(), sorted(segments));
         printTable();
      }
   }

   private void printTable() {
      StringBuilder sb = new StringBuilder("Segments for node ").append(rpcManager.getAddress()).append(':');
      for (int i = 0; i < numSegments; i += 16) {
         sb.append('\n');
         for (int j = 0; j < 16 && i + j < numSegments; ++j) {
            sb.append(String.format("%4d=%c ", i + j, segmentStates.get(i + j).singleLetter()));
         }
      }
      log.debug(sb.toString());
   }

   private List<Integer> sorted(Set<Integer> segments) {
      Integer[] array = segments.toArray(new Integer[segments.size()]);
      Arrays.sort(array);
      return Arrays.asList(array);
   }

   protected void tryRegularInvalidations(ConcurrentMap<K, InvalidationInfo> scheduledKeys, boolean force) {
      // ignore if there are two concurrent updates
      if (scheduledKeysSwapper.compareAndSet(this, scheduledKeys, new ConcurrentHashMap<>(invalidationBatchSize))) {
         executorService.execute(() -> {
            // we'll invalidate all keys in one run
            // we don't have to keep any topology lock, because the versions increase monotonically
            int numKeys = scheduledKeys.size();
            Object[] keys = new Object[numKeys];
            long[] versions = new long[numKeys];
            boolean[] isRemoved = new boolean[numKeys];
            int numRemoved = 0;
            int i = 0;
            for (Map.Entry<K, InvalidationInfo> entry : scheduledKeys.entrySet()) {
               keys[i] = entry.getKey();
               versions[i] = entry.getValue().version;
               if (isRemoved[i] = entry.getValue().removal) { // intentional assignment
                  numRemoved++;
               }
               if (++i > numKeys) {
                  // concurrent modification?
                  numKeys = scheduledKeys.size();
                  keys = Arrays.copyOf(keys, numKeys);
                  versions = Arrays.copyOf(versions, numKeys);
                  isRemoved = Arrays.copyOf(isRemoved, numKeys);
               }
            }
            InvalidateVersionsCommand command = commandsFactory.buildInvalidateVersionsCommand(keys, versions, false);
            sendRegularInvalidations(command, keys, versions, numRemoved, isRemoved, force);
         });
      }
   }

   protected void sendRegularInvalidations(InvalidateVersionsCommand command, Object[] keys, long[] versions, int numRemoved, boolean[] isRemoved, boolean force) {
      CompletableFuture<Map<Address, Response>> future = rpcManager.invokeRemotelyAsync(null, command, syncRpcOptions);
      future.whenComplete((r, t) -> {
         if (t != null) {
            log.error("Failed to send invalidations, will retry", t);
            sendRegularInvalidations(command, keys, versions, numRemoved, isRemoved, force);
         } else if (numRemoved > 0 || force) {
            regularInvalidationFinished(keys, versions, isRemoved, force);
         }
      });
   }

   protected void regularInvalidationFinished(Object[] keys, long[] versions, boolean[] isRemoved, boolean force) {
      ConcurrentMap<K, Long> removedKeys;
      do {
         removedKeys = this.removedKeys;
         for (int i = 0; i < isRemoved.length; ++i) {
            if (isRemoved[i]) {
               long version = versions[i];
               removedKeys.compute((K) keys[i], (k, prev) -> prev == null || prev.longValue() < version ? version : prev);
            }
         }
      } while (removedKeys != this.removedKeys);
      if (removedKeys.size() > invalidationBatchSize || (force && !removedKeys.isEmpty())) {
         tryRemovedInvalidations(removedKeys);
      }
   }

   private void tryRemovedInvalidations(ConcurrentMap<K, Long> removedKeys) {
      if (removedKeysSwapper.compareAndSet(this, removedKeys, new ConcurrentHashMap<>(invalidationBatchSize))) {
         final ConcurrentMap<K, Long> rk = removedKeys;
         executorService.execute(() -> {
            int numKeys = rk.size();
            Object[] remKeys = new Object[numKeys];
            long[] remVersions = new long[numKeys];
            int i = 0;
            for (Map.Entry<K, Long> entry : rk.entrySet()) {
               remKeys[i] = entry.getKey();
               remVersions[i] = entry.getValue().longValue();
               if (++i > numKeys) {
                  numKeys = rk.size();
                  remKeys = Arrays.copyOf(remKeys, numKeys);
                  remVersions = Arrays.copyOf(remVersions, numKeys);
               }
            }
            InvalidateVersionsCommand removeCommand = commandsFactory.buildInvalidateVersionsCommand(remKeys, remVersions, true);
            sendRemoveInvalidations(removeCommand);
         });
      }
   }

   protected void sendRemoveInvalidations(InvalidateVersionsCommand removeCommand) {
      rpcManager.invokeRemotelyAsync(null, removeCommand, syncRpcOptions).whenComplete((r, t) -> {
         if (t != null) {
            log.error("Failed to send remove invalidations, will retry", t);
            sendRemoveInvalidations(removeCommand);
         } else {
            removeInvalidationsFinished();
         }
      });
      // remove the entries on self, too
      removeCommand.init(dataContainer);
      removeCommand.perform(null);
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
      public final long version;
      public final boolean removal;

      private InvalidationInfo(long version, boolean removal) {
         this.version = version;
         this.removal = removal;
      }
   }
}
