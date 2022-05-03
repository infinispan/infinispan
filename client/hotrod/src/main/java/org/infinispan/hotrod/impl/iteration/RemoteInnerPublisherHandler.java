package org.infinispan.hotrod.impl.iteration;

import java.lang.invoke.MethodHandles;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.commons.reactive.AbstractAsyncPublisherHandler;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.logging.TraceException;
import org.infinispan.hotrod.exceptions.RemoteIllegalLifecycleStateException;
import org.infinispan.hotrod.exceptions.TransportException;
import org.infinispan.hotrod.impl.logging.Log;
import org.infinispan.hotrod.impl.logging.LogFactory;
import org.infinispan.hotrod.impl.operations.IterationNextResponse;
import org.infinispan.hotrod.impl.operations.IterationStartResponse;

import io.netty.channel.Channel;

class RemoteInnerPublisherHandler<K, E> extends AbstractAsyncPublisherHandler<Map.Entry<SocketAddress, IntSet>,
      CacheEntry<K, E>, IterationStartResponse, IterationNextResponse<K, E>> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   protected final RemotePublisher<K, E> publisher;

   // Need to be volatile since cancel can come on a different thread
   protected volatile Channel channel;
   private volatile byte[] iterationId;
   private AtomicBoolean cancelled = new AtomicBoolean();

   protected RemoteInnerPublisherHandler(RemotePublisher<K, E> parent, int batchSize,
         Supplier<Map.Entry<SocketAddress, IntSet>> supplier, Map.Entry<SocketAddress, IntSet> firstTarget) {
      super(batchSize, supplier, firstTarget);
      this.publisher = parent;
   }

   private String iterationId() {
      return publisher.iterationId(iterationId);
   }

   @Override
   protected void sendCancel(Map.Entry<SocketAddress, IntSet> target) {
      if (!cancelled.getAndSet(true)) {
         actualCancel();
      }
   }

   private void actualCancel() {
      if (iterationId != null && channel != null) {
         // Just let cancel complete asynchronously
         publisher.sendCancel(iterationId, channel);
      }
   }

   @Override
   protected CompletionStage<IterationStartResponse> sendInitialCommand(
         Map.Entry<SocketAddress, IntSet> target, int batchSize) {
      SocketAddress address = target.getKey();
      IntSet segments = target.getValue();
      log.tracef("Starting iteration with segments %s", segments);
      return publisher.newIteratorStartOperation(address, segments, batchSize);
   }

   @Override
   protected CompletionStage<IterationNextResponse<K, E>> sendNextCommand(Map.Entry<SocketAddress, IntSet> target, int batchSize) {
      return publisher.newIteratorNextOperation(iterationId, channel);
   }

   @Override
   protected long handleInitialResponse(IterationStartResponse startResponse, Map.Entry<SocketAddress, IntSet> target) {
      this.channel = startResponse.getChannel();
      this.iterationId = startResponse.getIterationId();
      if (log.isDebugEnabled()) {
         log.iterationTransportObtained(channel.remoteAddress(), iterationId());
         log.startedIteration(iterationId());
      }

      // We could have been cancelled while the initial response was sent
      if (cancelled.get()) {
         actualCancel();
      }
      return 0;
   }

   @Override
   protected long handleNextResponse(IterationNextResponse<K, E> nextResponse, Map.Entry<SocketAddress, IntSet> target) {
      if (!nextResponse.hasMore()) {
         // server doesn't clean up when complete
         sendCancel(target);
         publisher.completeSegments(target.getValue());
         targetComplete();
      }
      IntSet completedSegments = nextResponse.getCompletedSegments();
      if (completedSegments != null && log.isTraceEnabled()) {
         IntSet targetSegments = target.getValue();
         if (targetSegments != null) {
            targetSegments.removeAll(completedSegments);
         }
      }
      publisher.completeSegments(completedSegments);
      List<CacheEntry<K, E>> entries = nextResponse.getEntries();
      for (CacheEntry<K, E> entry : entries) {
         if (!onNext(entry)) {
            break;
         }
      }
      return entries.size();
   }

   @Override
   protected void handleThrowableInResponse(Throwable t, Map.Entry<SocketAddress, IntSet> target) {
      if (t instanceof TransportException || t instanceof RemoteIllegalLifecycleStateException) {
         log.throwableDuringPublisher(t);
         if (log.isTraceEnabled()) {
            IntSet targetSegments = target.getValue();
            if (targetSegments != null) {
               log.tracef("There are still outstanding segments %s that will need to be retried", targetSegments);
            }
         }
         publisher.erroredServer(target.getKey());
         // Try next target if possible
         targetComplete();

         accept(0);
      } else {
         t.addSuppressed(new TraceException());
         super.handleThrowableInResponse(t, target);
      }

   }
}
