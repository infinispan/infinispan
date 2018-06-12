package org.infinispan.statetransfer;

import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import net.jcip.annotations.GuardedBy;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.SmallIntSet;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.SingleResponseCollector;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Inbound state transfer task. Fetches multiple data segments from a remote source node and applies them to local
 * cache. Instances of InboundTransferTask are created and managed by StateTransferManagerImpl. StateTransferManagerImpl
 * must have zero or one such task for each segment.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class InboundTransferTask {

   private static final Log log = LogFactory.getLog(InboundTransferTask.class);
   private static final boolean trace = log.isTraceEnabled();

   @GuardedBy("segments")
   private final SmallIntSet segments;

   @GuardedBy("segments")
   private final SmallIntSet unfinishedSegments;

   private final Address source;

   private volatile boolean isCancelled = false;

   /**
    * This latch is counted down when all segments are completely received or in case of task cancellation.
    */
   private final CompletableFuture<Void> completionFuture = new CompletableFuture<>();

   private final int topologyId;

   private final RpcManager rpcManager;

   private final CommandsFactory commandsFactory;

   private final long timeout;

   private final String cacheName;

   private final boolean applyState;

   private final RpcOptions rpcOptions;

   public InboundTransferTask(Set<Integer> segments, Address source, int topologyId, RpcManager rpcManager,
                              CommandsFactory commandsFactory, long timeout, String cacheName, boolean applyState) {
      if (segments == null || segments.isEmpty()) {
         throw new IllegalArgumentException("segments must not be null or empty");
      }
      if (source == null) {
         throw new IllegalArgumentException("Source address cannot be null");
      }

      this.segments = new SmallIntSet(segments);
      this.unfinishedSegments = new SmallIntSet(segments);
      this.source = source;
      this.topologyId = topologyId;
      this.rpcManager = rpcManager;
      this.commandsFactory = commandsFactory;
      this.timeout = timeout;
      this.cacheName = cacheName;
      this.applyState = applyState;
      this.rpcOptions = new RpcOptions(DeliverOrder.NONE, timeout, TimeUnit.MILLISECONDS);
   }

   public SmallIntSet getSegments() {
      synchronized (segments) {
         return new SmallIntSet(segments);
      }
   }

   public SmallIntSet getUnfinishedSegments() {
      synchronized (segments) {
         return new SmallIntSet(unfinishedSegments);
      }
   }

   public Address getSource() {
      return source;
   }

   /**
    * Send START_STATE_TRANSFER request to source node.
    *
    * @return a {@code CompletableFuture} that completes when the transfer is done.
    */
   public CompletableFuture<Void> requestSegments() {
      return startTransfer(applyState ? StateRequestCommand.Type.START_STATE_TRANSFER : StateRequestCommand.Type.START_CONSISTENCY_CHECK);
   }

   public CompletableFuture<Void> requestKeys() {
      return startTransfer(StateRequestCommand.Type.START_KEYS_TRANSFER);
   }

   private CompletableFuture<Void> startTransfer(StateRequestCommand.Type type) {
      if (!isCancelled) {
         Set<Integer> segmentsCopy = getSegments();
         if (segmentsCopy.isEmpty()) {
            log.tracef("Segments list is empty, skipping source %s", source);
            completionFuture.complete(null);
            return completionFuture;
         }
         if (trace) {
            log.tracef("Requesting state (%s) from node %s for segments %s", type, source, segmentsCopy);
         }
         // start transfer of cache entries
         try {
            StateRequestCommand cmd = commandsFactory.buildStateRequestCommand(type, rpcManager.getAddress(), topologyId, segmentsCopy);
            Response response = rpcManager.blocking(rpcManager.invokeCommand(source, cmd,
                                                                             SingleResponseCollector.validOnly(),
                                                                             rpcOptions));
            if (response instanceof SuccessfulResponse) {
               if (trace) {
                  log.tracef("Successfully requested state (%s) from node %s for segments %s", type, source, segmentsCopy);
               }
               return completionFuture;
            } else {
               Exception e = new CacheException(String.valueOf(response));
               log.failedToRequestSegments(cacheName, source, segmentsCopy, e);
               completionFuture.completeExceptionally(e);
            }
         } catch (Exception e) {
            if (!isCancelled) {
               log.failedToRequestSegments(cacheName, source, segmentsCopy, e);
               completionFuture.completeExceptionally(e);
            }
         }
      }
      return completionFuture;
   }

   /**
    * Cancels a set of segments and marks them as finished.
    *
    * If all segments are cancelled then the whole task is cancelled, as if {@linkplain #cancel()} was called.
    *
    * @param cancelledSegments the segments to be cancelled
    */
   public void cancelSegments(Set<Integer> cancelledSegments) {
      if (isCancelled) {
         throw new IllegalArgumentException("The task is already cancelled.");
      }

      if (trace) {
         log.tracef("Partially cancelling inbound state transfer from node %s, segments %s", source, cancelledSegments);
      }

      synchronized (segments) {
         // healthy paranoia
         if (!segments.containsAll(cancelledSegments)) {
            throw new IllegalArgumentException("Some of the specified segments cannot be cancelled because they were not previously requested");
         }

         unfinishedSegments.removeAll(cancelledSegments);
         if (unfinishedSegments.isEmpty()) {
            isCancelled = true;
         }
      }

      sendCancelCommand(cancelledSegments);

      if (isCancelled) {
         notifyCompletion(false);
      }
   }

   /**
    * Cancels all the segments and marks them as finished, sends a cancel command, then completes the task.
    */
   public void cancel() {
      if (!isCancelled) {
         isCancelled = true;

         Set<Integer> segmentsCopy = getUnfinishedSegments();
         unfinishedSegments.clear();
         if (trace) {
            log.tracef("Cancelling inbound state transfer from %s with unfinished segments %s", source, segmentsCopy);
         }

         sendCancelCommand(segmentsCopy);

         notifyCompletion(false);
      }
   }

   public boolean isCancelled() {
      return isCancelled;
   }

   private void sendCancelCommand(Set<Integer> cancelledSegments) {
      StateRequestCommand.Type requestType = applyState ? StateRequestCommand.Type.CANCEL_STATE_TRANSFER : StateRequestCommand.Type.CANCEL_CONSISTENCY_CHECK;
      StateRequestCommand cmd = commandsFactory.buildStateRequestCommand(requestType, rpcManager.getAddress(),
            topologyId, cancelledSegments);
      try {
         rpcManager.sendTo(source, cmd, DeliverOrder.NONE);
      } catch (Exception e) {
         // Ignore exceptions here, the worst that can happen is that the provider will send some extra state
         log.debugf("Caught an exception while cancelling state transfer from node %s for segments %s",
                    source, cancelledSegments);
      }
   }

   public void onStateReceived(int segmentId, boolean isLastChunk) {
      if (!isCancelled && isLastChunk) {
         boolean isCompleted = false;
         synchronized (segments) {
            if (segments.contains(segmentId)) {
               unfinishedSegments.remove(segmentId);
               if (unfinishedSegments.isEmpty()) {
                  log.debugf("Finished receiving state for segments %s", segments);
                  isCompleted = true;
               }
            }
         }
         if (isCompleted) {
            notifyCompletion(true);
         }
      }
   }

   private void notifyCompletion(boolean success) {
      if (success) {
         completionFuture.complete(null);
      } else {
         completionFuture.completeExceptionally(new CancellationException("Inbound transfer was cancelled"));
      }
   }

   public boolean isCompletedSuccessfully() {
      return completionFuture.isDone() && !completionFuture.isCompletedExceptionally();
   }

   /**
    * Terminate abruptly regardless if the segments were received or not. This is used when the source node
    * is no longer alive.
    */
   public void terminate() {
      notifyCompletion(false);
   }

   @Override
   public String toString() {
      synchronized (segments) {
         return "InboundTransferTask{" +
               "segments=" + segments +
               ", unfinishedSegments=" + unfinishedSegments +
               ", source=" + source +
               ", isCancelled=" + isCancelled +
               ", completionFuture=" + completionFuture +
               ", topologyId=" + topologyId +
               ", timeout=" + timeout +
               ", cacheName=" + cacheName +
               '}';
      }
   }
}
