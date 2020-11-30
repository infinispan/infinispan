package org.infinispan.statetransfer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.statetransfer.StateResponseCommand;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.SingleResponseCollector;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.CompletableObserver;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.disposables.Disposable;

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

   private final KeyPartitioner keyPartitioner;

   private final RpcManager rpcManager;

   private final CommandsFactory commandsFactory;

   private final long timeout;

   private final String cacheName;

   private final boolean applyState;

   private final boolean pushTransfer;


   private final RpcOptions rpcOptions;

   private volatile boolean cancelled;

   public OutboundTransferTask(Address destination, IntSet segments, int segmentCount, int chunkSize,
                               int topologyId, KeyPartitioner keyPartitioner,
                               Consumer<Collection<StateChunk>> onChunkReplicated,
                               RpcManager rpcManager,
                               CommandsFactory commandsFactory, long timeout, String cacheName,
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
      this.onChunkReplicated = onChunkReplicated;
      this.destination = destination;
      this.segments = IntSets.concurrentCopyFrom(segments, segmentCount);
      this.chunkSize = chunkSize;
      this.topologyId = topologyId;
      this.keyPartitioner = keyPartitioner;
      this.rpcManager = rpcManager;
      this.commandsFactory = commandsFactory;
      this.timeout = timeout;
      this.cacheName = cacheName;
      this.applyState = applyState;
      this.pushTransfer = pushTransfer;

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
    * @param entries a {@code Flowable} with all the entries that need to be sent
    */
   public CompletionStage<Void> execute(Flowable<InternalCacheEntry<Object, Object>> entries) {
      CompletableFuture<Void> taskFuture = new CompletableFuture<>();
      try {
         AtomicReference<List<InternalCacheEntry<Object, Object>>> batchRef =
            new AtomicReference<>(Collections.emptyList());
         entries.buffer(chunkSize)
                .takeUntil(batch -> cancelled)
                .concatMapCompletable(batch -> {
                   // Send the previous batch, not the current one
                   // This allows us to mark all the segments as finished in the same RPC with the
                   // last batch
                   List<InternalCacheEntry<Object, Object>> previousBatch = batchRef.getAndSet(batch);
                   if (previousBatch.isEmpty())
                      return Completable.complete();

                   return Completable.fromCompletionStage(sendEntries(previousBatch, false));
                }, 1)
                .subscribe(new CompletableObserver() {
                   @Override
                   public void onSubscribe(Disposable d) {
                   }

                   @Override
                   public void onComplete() {
                      // Send the remaining entries and mark all the segments as finished
                      List<InternalCacheEntry<Object, Object>> previousBatch = batchRef.get();
                      sendEntries(previousBatch, true)
                         .whenComplete((ignored, throwable) -> {
                            if (throwable == null) {
                               taskFuture.complete(null);
                            } else {
                               taskFuture.completeExceptionally(throwable);
                            }
                         });
                   }

                   @Override
                   public void onError(Throwable e) {
                      taskFuture.completeExceptionally(e);
                   }
                });
      } catch (Throwable t) {
         taskFuture.completeExceptionally(t);
      }
      return taskFuture;
   }

   private CompletionStage<Void> sendEntries(List<InternalCacheEntry<Object, Object>> entries, boolean isLast) {
      Map<Integer, StateChunk> chunks = new HashMap<>();
      for (InternalCacheEntry<Object, Object> ice : entries) {
         int segmentId = keyPartitioner.getSegment(ice.getKey());
         if (segments.contains(segmentId)) {
            StateChunk chunk = chunks.computeIfAbsent(
               segmentId, segment -> new StateChunk(segment, new ArrayList<>(), isLast));
            chunk.getCacheEntries().add(ice);
         }
      }

      if (isLast) {
         for (PrimitiveIterator.OfInt iter = segments.iterator(); iter.hasNext(); ) {
            int segmentId = iter.nextInt();
            chunks.computeIfAbsent(
               segmentId, segment -> new StateChunk(segment, Collections.emptyList(), true));
         }
      }

      if (chunks.isEmpty())
         return CompletableFutures.completedNull();

      if (log.isTraceEnabled()) {
         if (isLast) {
            log.tracef("Sending last chunk to node %s containing %d cache entries from segments %s", destination,
                       entries.size(), segments);
         } else {
            log.tracef("Sending to node %s %d cache entries from segments %s", destination, entries.size(),
                       chunks.keySet());
         }
      }

      StateResponseCommand cmd = commandsFactory.buildStateResponseCommand(topologyId,
                                                                           chunks.values(), applyState, pushTransfer);
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
