package org.infinispan.statetransfer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.statetransfer.StateResponseCommand;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.reactive.publisher.impl.SegmentPublisherSupplier;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.SingleResponseCollector;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Flowable;

/**
 * Outbound state transfer task. Pushes data segments to another cluster member on request. Instances of
 * OutboundTransferTask are created and managed by StateTransferManagerImpl. There should be at most
 * one such task per destination at any time.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class OutboundTransferTask {

   private static final Log log = LogFactory.getLog(OutboundTransferTask.class);

   private final Consumer<Collection<StateChunk>> onChunkReplicated;

   private final int topologyId;

   private final Address destination;

   private final IntSet segments;

   private final int chunkSize;

   private final RpcManager rpcManager;

   private final CommandsFactory commandsFactory;

   private final long timeout;

   private final String cacheName;

   private final boolean applyState;

   private final RpcOptions rpcOptions;

   private volatile boolean cancelled;

   public OutboundTransferTask(Address destination, IntSet segments, int segmentCount, int chunkSize, int topologyId,
                               Consumer<Collection<StateChunk>> onChunkReplicated, RpcManager rpcManager,
                               CommandsFactory commandsFactory, long timeout, String cacheName, boolean applyState) {
      if (segments == null || segments.isEmpty()) {
         throw new IllegalArgumentException("Segments must not be null or empty");
      }
      if (destination == null) {
         throw new IllegalArgumentException("Destination address cannot be null");
      }
      if (chunkSize <= 0) {
         throw new IllegalArgumentException("chunkSize must be greater than 0");
      }
      this.onChunkReplicated = onChunkReplicated;
      this.destination = destination;
      this.segments = IntSets.concurrentCopyFrom(segments, segmentCount);
      this.chunkSize = chunkSize;
      this.topologyId = topologyId;
      this.rpcManager = rpcManager;
      this.commandsFactory = commandsFactory;
      this.timeout = timeout;
      this.cacheName = cacheName;
      this.applyState = applyState;

      this.rpcOptions = new RpcOptions(DeliverOrder.NONE, timeout, TimeUnit.MILLISECONDS);
   }

   public Address getDestination() {
      return destination;
   }

   public IntSet getSegments() {
      return segments;
   }

   public int getTopologyId() {
      return topologyId;
   }

   /**
    * Starts sending entries from the data container and the first loader with fetch persistent data enabled
    * to the target node.
    *
    * @return a completion stage that completes when all the entries have been sent.
    * @param notifications a {@code Flowable} with all the entries that need to be sent
    */
   public CompletionStage<Void> execute(Flowable<SegmentPublisherSupplier.Notification<InternalCacheEntry<?, ?>>> notifications) {
      return notifications
            .buffer(chunkSize)
            .takeUntil(batch -> cancelled)
            // Here we receive a batch of notifications, a list with size up to chunkSize.
            // Although the notification list has the chunkSize the list contains not only data segments.
            // The notification contains data segments which hold values; lost and completed segments. This means that
            // although we are batching the data, our final chunk can be smaller than chunkSize.
            // This could be improved.
            .concatMapCompletable(batch -> {
               Map<Integer, StateChunk> chunks = new HashMap<>();
               for(SegmentPublisherSupplier.Notification<InternalCacheEntry<?, ?>> notification: batch) {
                  if (notification.isValue()) {
                     StateChunk chunk = chunks.computeIfAbsent(
                           notification.valueSegment(), segment -> new StateChunk(segment, new ArrayList<>(), false));
                     chunk.getCacheEntries().add(notification.value());
                  }

                  // If the notification identify the segment is completed we mark a chunk as a last chunk.
                  if (notification.isSegmentComplete()) {
                     int segment = notification.completedSegment();
                     chunks.compute(segment, (s, previous) -> previous == null
                           ? new StateChunk(s, Collections.emptyList(), true)
                           : new StateChunk(segment, previous.getCacheEntries(), true));
                  }
               }

               return Completable.fromCompletionStage(sendChunks(chunks));
            }, 1)
            .toCompletionStage(null);
   }

   private CompletionStage<Void> sendChunks(Map<Integer, StateChunk> chunks) {
      if (chunks.isEmpty())
         return CompletableFutures.completedNull();

      if (log.isTraceEnabled()) {
         long entriesSize = chunks.values().stream().mapToInt(v -> v.getCacheEntries().size()).sum();
         log.tracef("Sending to node %s %d cache entries from segments %s", destination, entriesSize, chunks.keySet());
      }

      StateResponseCommand cmd = commandsFactory.buildStateResponseCommand(topologyId, chunks.values(), applyState);
      try {
         return rpcManager.invokeCommand(destination, cmd, SingleResponseCollector.validOnly(), rpcOptions)
                          .handle((response, throwable) -> {
                             if (throwable == null) {
                                onChunkReplicated.accept(chunks.values());
                                return null;
                             }

                             logSendException(throwable);
                             cancel();
                             return null;
                          });
      } catch (IllegalLifecycleStateException e) {
         // Manager is shutting down, ignore the error
         cancel();
      } catch (Exception e) {
         logSendException(e);
         cancel();
      }
      return CompletableFutures.completedNull();
   }

   private void logSendException(Throwable throwable) {
      Throwable t = CompletableFutures.extractException(throwable);
      if (t instanceof SuspectException) {
         log.debugf("Node %s left cache %s while we were sending state to it, cancelling transfer.",
                    destination, cacheName);
      } else if (isCancelled()) {
         log.debugf("Stopping cancelled transfer to node %s, segments %s", destination, segments);
      } else {
         log.errorf(t, "Failed to send entries to node %s: %s", destination, t.getMessage());
      }
   }

   /**
    * Cancel some of the segments. If all segments get cancelled then the whole task will be cancelled.
    *
    * @param cancelledSegments segments to cancel.
    */
   void cancelSegments(IntSet cancelledSegments) {
      if (segments.removeAll(cancelledSegments)) {
         if (log.isTraceEnabled()) {
            log.tracef("Cancelling outbound transfer to node %s, segments %s (remaining segments %s)",
                       destination, cancelledSegments, segments);
         }
         if (segments.isEmpty()) {
            cancel();
         }
      }
   }

   /**
    * Cancel the whole task.
    */
   public void cancel() {
      if (!cancelled) {
         log.debugf("Cancelling outbound transfer to node %s, segments %s", destination, segments);
         cancelled = true;
      }
   }

   public boolean isCancelled() {
      return cancelled;
   }

   @Override
   public String toString() {
      return "OutboundTransferTask{" +
            "topologyId=" + topologyId +
            ", destination=" + destination +
            ", segments=" + segments +
            ", chunkSize=" + chunkSize +
            ", timeout=" + timeout +
            ", cacheName='" + cacheName + '\'' +
            '}';
   }
}
