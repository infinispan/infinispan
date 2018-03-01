package org.infinispan.statetransfer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.SmallIntSet;
import org.infinispan.container.DataContainer;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.reactivex.Flowable;

/**
 * Outbound state transfer task. Pushes data segments to another cluster member on request. Instances of
 * OutboundTransferTask are created and managed by StateTransferManagerImpl. There should be at most
 * one such task per destination at any time.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class OutboundTransferTask implements Runnable {

   private static final Log log = LogFactory.getLog(OutboundTransferTask.class);

   private final boolean trace = log.isTraceEnabled();

   private final Consumer<OutboundTransferTask> onCompletion;

   private final Consumer<List<StateChunk>> onChunkReplicated;

   private final BiFunction<InternalCacheEntry, InternalEntryFactory, InternalCacheEntry> mapEntryFromDataContainer;

   private final BiFunction<MarshalledEntry, InternalEntryFactory, InternalCacheEntry> mapEntryFromStore;

   private final int topologyId;

   private final Address destination;

   private final Set<Integer> segments = new CopyOnWriteArraySet<>();

   private final int chunkSize;

   private final KeyPartitioner keyPartitioner;

   private final DataContainer<Object, Object> dataContainer;

   private final PersistenceManager persistenceManager;

   private final RpcManager rpcManager;

   private final CommandsFactory commandsFactory;

   private final long timeout;

   private final String cacheName;

   private final boolean applyState;

   private final boolean pushTransfer;

   private final Map<Integer, List<InternalCacheEntry>> entriesBySegment = CollectionFactory.makeConcurrentMap();

   /**
    * The total number of entries from all segments accumulated in entriesBySegment.
    */
   private int accumulatedEntries;

   /**
    * The Future obtained from submitting this task to an executor service. This is used for cancellation.
    */
   private FutureTask<Void> runnableFuture;

   private final RpcOptions rpcOptions;

   private InternalEntryFactory entryFactory;

   public OutboundTransferTask(Address destination, Set<Integer> segments, int chunkSize,
                               int topologyId, KeyPartitioner keyPartitioner,
                               Consumer<OutboundTransferTask> onCompletion, Consumer<List<StateChunk>> onChunkReplicated,
                               BiFunction<InternalCacheEntry, InternalEntryFactory, InternalCacheEntry> mapEntryFromDataContainer,
                               BiFunction<MarshalledEntry, InternalEntryFactory, InternalCacheEntry> mapEntryFromStore, DataContainer dataContainer,
                               PersistenceManager persistenceManager, RpcManager rpcManager,
                               CommandsFactory commandsFactory, InternalEntryFactory ef, long timeout, String cacheName,
                               boolean applyState, boolean pushTransfer) {
      if (segments == null || segments.isEmpty()) {
         throw new IllegalArgumentException("Segments must not be null or empty");
      }
      if (destination == null) {
         throw new IllegalArgumentException("Destination address cannot be null");
      }
      if (chunkSize <= 0) {
         throw new IllegalArgumentException("chunkSize must be greater than 0");
      }
      this.onCompletion = onCompletion;
      this.onChunkReplicated = onChunkReplicated;
      this.mapEntryFromDataContainer = mapEntryFromDataContainer;
      this.mapEntryFromStore = mapEntryFromStore;
      this.destination = destination;
      this.segments.addAll(segments);
      this.chunkSize = chunkSize;
      this.topologyId = topologyId;
      this.keyPartitioner = keyPartitioner;
      this.dataContainer = dataContainer;
      this.persistenceManager = persistenceManager;
      this.entryFactory = ef;
      this.rpcManager = rpcManager;
      this.commandsFactory = commandsFactory;
      this.timeout = timeout;
      this.cacheName = cacheName;
      this.applyState = applyState;
      this.pushTransfer = pushTransfer;
      //the rpc options does not change in runtime. re-use the same instance
      this.rpcOptions = rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS)
            .timeout(timeout, TimeUnit.MILLISECONDS).build();
   }

   public void execute(ExecutorService executorService) {
      if (runnableFuture != null) {
         throw new IllegalStateException("This task was already submitted");
      }
      runnableFuture = new FutureTask<Void>(this, null) {
         @Override
         protected void done() {
            onCompletion.accept(OutboundTransferTask.this);
         }
      };
      executorService.submit(runnableFuture);
   }

   public Address getDestination() {
      return destination;
   }

   public Set<Integer> getSegments() {
      return segments;
   }

   public int getTopologyId() {
      return topologyId;
   }

   //todo [anistor] check thread interrupt status in loops to implement faster cancellation
   public void run() {
      try {
         // send data container entries
         for (InternalCacheEntry ice : dataContainer) {
            Object key = ice.getKey();  //todo [anistor] should we check for expired entries?
            int segmentId = keyPartitioner.getSegment(key);
            if (segments.contains(segmentId) && !ice.isL1Entry()) {
               InternalCacheEntry entry = mapEntryFromDataContainer.apply(ice, entryFactory);
               if (entry != null) {
                  sendEntry(entry, segmentId);
               }
            }
         }

         AdvancedCacheLoader<Object, Object> stProvider = persistenceManager.getStateTransferProvider();
         if (stProvider != null) {
            try {
               AdvancedCacheLoader.CacheLoaderTask task = (me, taskContext) -> {
                  int segmentId = keyPartitioner.getSegment(me.getKey());
                  if (segments.contains(segmentId)) {
                     try {
                        InternalCacheEntry entry = mapEntryFromStore.apply(me, entryFactory);
                        if (entry != null) {
                           sendEntry(entry, segmentId);
                        }
                     } catch (CacheException e) {
                        log.failedLoadingValueFromCacheStore(me.getKey(), e);
                     }
                  }
               };
               Flowable.fromPublisher(stProvider.publishEntries(k -> !dataContainer.containsKey(k), true, true))
                     .blockingForEach(me -> task.processEntry(me, null));
            } catch (CacheException e) {
               log.failedLoadingKeysFromCacheStore(e);
            }
         }

         // send the last chunk of all segments
         sendEntries(true);
      } catch (Throwable t) {
         // ignore eventual exceptions caused by cancellation (have InterruptedException as the root cause)
         if (isCancelled()) {
            if (trace) {
               log.tracef("Ignoring error in already cancelled transfer to node %s, segments %s", destination,
                          segments);
            }
         } else {
            log.failedOutBoundTransferExecution(t);
         }
      }
      if (trace) {
         log.tracef("Completed outbound transfer to node %s, segments %s", destination, segments);
      }
   }

   private void sendEntry(InternalCacheEntry ice, int segmentId) {
      // send if we have a full chunk
      if (accumulatedEntries >= chunkSize) {
         sendEntries(false);
         accumulatedEntries = 0;
      }

      List<InternalCacheEntry> entries = entriesBySegment.computeIfAbsent(segmentId, k -> new ArrayList<>());
      entries.add(ice);
      accumulatedEntries++;
   }

   private void sendEntries(boolean isLast) {
      List<StateChunk> chunks = new ArrayList<>();
      for (Map.Entry<Integer, List<InternalCacheEntry>> e : entriesBySegment.entrySet()) {
         List<InternalCacheEntry> entries = e.getValue();
         if (!entries.isEmpty() || isLast) {
            chunks.add(new StateChunk(e.getKey(), new ArrayList<>(entries), isLast));
            entries.clear();
         }
      }

      if (isLast) {
         for (int segmentId : segments) {
            List<InternalCacheEntry> entries = entriesBySegment.get(segmentId);
            if (entries == null) {
               chunks.add(new StateChunk(segmentId, Collections.emptyList(), true));
            }
         }
      }

      if (!chunks.isEmpty()) {
         if (trace) {
            if (isLast) {
               log.tracef("Sending last chunk to node %s containing %d cache entries from segments %s", destination, accumulatedEntries, segments);
            } else {
               log.tracef("Sending to node %s %d cache entries from segments %s", destination, accumulatedEntries, entriesBySegment.keySet());
            }
         }

         StateResponseCommand cmd = commandsFactory.buildStateResponseCommand(rpcManager.getAddress(), topologyId, chunks, applyState, pushTransfer);
         // send synchronously, in order. it is important that the last chunk is received last in order to correctly detect completion of the stream of chunks
         try {
            rpcManager.invokeRemotely(Collections.singleton(destination), cmd, rpcOptions);
            onChunkReplicated.accept(chunks);
         } catch (SuspectException e) {
            log.debugf("Node %s left cache %s while we were sending state to it, cancelling transfer.", destination, cacheName);
            cancel();
         } catch (Exception e) {
            if (isCancelled()) {
               log.debugf("Stopping cancelled transfer to node %s, segments %s", destination, segments);
            } else {
               log.errorf(e, "Failed to send entries to node %s: %s", destination, e.getMessage());
            }
         }
      }
   }

   /**
    * Cancel some of the segments. If all segments get cancelled then the whole task will be cancelled.
    *
    * @param cancelledSegments segments to cancel.
    */
   public void cancelSegments(Set<Integer> cancelledSegments) {
      if (segments.removeAll(cancelledSegments)) {
         if (trace) {
            log.tracef("Cancelling outbound transfer to node %s, segments %s (remaining segments %s)",
                       destination, cancelledSegments, segments);
         }
         entriesBySegment.keySet().removeAll(cancelledSegments);  // here we do not update accumulatedEntries but this inaccuracy does not cause any harm
         if (segments.isEmpty()) {
            cancel();
         }
      }
   }

   /**
    * Cancel the whole task.
    */
   public void cancel() {
      if (runnableFuture != null && !runnableFuture.isCancelled()) {
         log.debugf("Cancelling outbound transfer to node %s, segments %s", destination, segments);
         runnableFuture.cancel(true);
      }
   }

   public boolean isCancelled() {
      return runnableFuture != null && runnableFuture.isCancelled();
   }

   @Override
   public String toString() {
      return "OutboundTransferTask{" +
            "topologyId=" + topologyId +
            ", destination=" + destination +
            ", segments=" + new SmallIntSet(segments) +
            ", chunkSize=" + chunkSize +
            ", timeout=" + timeout +
            ", cacheName='" + cacheName + '\'' +
            '}';
   }

   public static InternalCacheEntry defaultMapEntryFromDataContainer(InternalCacheEntry ice, InternalEntryFactory entryFactory) {
      return ice;
   }

   public static InternalCacheEntry defaultMapEntryFromStore(MarshalledEntry me, InternalEntryFactory entryFactory) {
      return entryFactory.create(me.getKey(), me.getValue(), me.getMetadata());
   }
}
