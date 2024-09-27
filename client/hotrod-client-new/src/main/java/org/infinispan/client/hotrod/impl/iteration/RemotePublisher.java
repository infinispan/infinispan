package org.infinispan.client.hotrod.impl.iteration;

import static org.infinispan.client.hotrod.logging.Log.HOTROD;

import java.lang.invoke.MethodHandles;
import java.net.SocketAddress;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.client.hotrod.impl.operations.CacheOperationsFactory;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.operations.IterationEndResponse;
import org.infinispan.client.hotrod.impl.operations.IterationNextResponse;
import org.infinispan.client.hotrod.impl.operations.IterationStartResponse;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.OperationDispatcher;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.reactive.RxJavaInterop;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import io.reactivex.rxjava3.core.Flowable;

public class RemotePublisher<K, E> implements Publisher<Map.Entry<K, E>> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private final CacheOperationsFactory operationsFactory;

   private final OperationDispatcher dispatcher;
   private final String filterConverterFactory;
   private final byte[][] filterParams;
   private final IntSet segments;
   private final int batchSize;
   private final boolean metadata;
   private final KeyTracker segmentKeyTracker;

   private final Set<SocketAddress> failedServers = ConcurrentHashMap.newKeySet();

   public RemotePublisher(CacheOperationsFactory operationsFactory, OperationDispatcher dispatcher, String filterConverterFactory,
                          Object[] filterParams, Set<Integer> segments, int batchSize, boolean metadata, DataFormat dataFormat) {
      this.operationsFactory = operationsFactory;
      this.dispatcher = dispatcher;
      this.filterConverterFactory = filterConverterFactory;
      this.filterParams = operationsFactory.marshallParams(filterParams);
      String cacheName = operationsFactory.getRemoteCache().getName();
      SegmentConsistentHash segmentConsistentHash = (SegmentConsistentHash) dispatcher.getConsistentHash(cacheName);
      if (segments == null) {
         if (segmentConsistentHash != null) {
            int maxSegment = segmentConsistentHash.getNumSegments();
            this.segments = IntSets.concurrentSet(maxSegment);
            for (int i = 0; i < maxSegment; ++i) {
               this.segments.set(i);
            }
         } else {
            this.segments = null;
         }
      } else {
         this.segments = IntSets.concurrentCopyFrom(IntSets.from(segments), Collections.max(segments) + 1);
      }
      this.batchSize = batchSize;
      this.metadata = metadata;
      this.segmentKeyTracker = KeyTrackerFactory.create(dataFormat, segmentConsistentHash,
            dispatcher.getTopologyId(cacheName), segments);
   }

   @Override
   public void subscribe(Subscriber<? super Map.Entry<K, E>> subscriber) {
      // Segments can be null if we weren't provided any and we don't have a ConsistentHash
      if (segments == null) {
         AtomicBoolean shouldRetry = new AtomicBoolean(true);

         RemoteInnerPublisherHandler<K, E> innerHandler = new RemoteInnerPublisherHandler<K, E>(this,
               batchSize, () -> {
            // Note that this publisher will continue to return empty entries until it has completed a given
            // target without encountering a Throwable
            if (shouldRetry.getAndSet(false)) {
               return new AbstractMap.SimpleImmutableEntry<>(null, null);
            }
            return null;
         }, null) {
            @Override
            protected void handleThrowableInResponse(Throwable t, Map.Entry<SocketAddress, IntSet> target) {
               // Let it retry again if necessary
               shouldRetry.set(true);
               super.handleThrowableInResponse(t, target);
            }
         };
         innerHandler.startPublisher().subscribe(subscriber);
         return;
      }
      Flowable.just(segments)
            .map(segments -> {
               Map<SocketAddress, Set<Integer>> segmentsByAddress = dispatcher.getPrimarySegmentsByAddress(
                     operationsFactory.getRemoteCache().getName());
               Map<SocketAddress, IntSet> actualTargets = new HashMap<>(segmentsByAddress.size());
               for (Map.Entry<SocketAddress, Set<Integer>> entry : segmentsByAddress.entrySet()) {
                  SocketAddress targetAddress = entry.getKey();
                  if (failedServers.contains(targetAddress)) {
                     targetAddress = null;
                  }
                  IntSet segmentsNeeded = null;
                  Set<Integer> targetSegments = entry.getValue();
                  for (int targetSegment : targetSegments) {
                     if (segments.contains(targetSegment)) {
                        if (segmentsNeeded == null) {
                           segmentsNeeded = IntSets.mutableEmptySet();
                        }
                        segmentsNeeded.set(targetSegment);
                     }
                  }
                  if (segmentsNeeded != null) {
                     actualTargets.put(targetAddress, segmentsNeeded);
                  }
               }
               // If no addresses could handle the segments directly - then just send to any node all segments
               if (actualTargets.isEmpty()) {
                  actualTargets.put(null, segments);
               }
               return actualTargets;
            }).flatMap(actualTargets -> {
               int batchSize = (this.batchSize / actualTargets.size()) + 1;
               return Flowable.fromIterable(actualTargets.entrySet())
                     .map(entry -> {
                        log.tracef("Requesting next for: %s", entry);
                        RemoteInnerPublisherHandler<K, E> innerHandler = new RemoteInnerPublisherHandler<>(this,
                              batchSize, () -> null, entry);
                        return innerHandler.startPublisher();
                     }).flatMap(RxJavaInterop.identityFunction(), actualTargets.size());
            })
            .repeatUntil(() -> {
               log.tracef("Segments left to process are %s", segments);
               return segments.isEmpty();
            }).subscribe(subscriber);
   }

   void erroredServer(SocketAddress socketAddress) {
      // socketAddress is null if we don't have a CH or if the CH contains an address that is already in failedServers
      if (socketAddress != null) {
         failedServers.add(socketAddress);
      }
   }

   CompletionStage<Void> sendCancel(byte[] iterationId, SocketAddress socketAddress) {
      CompletionStage<IterationEndResponse> endResponseStage = dispatcher.executeOnSingleAddress(
            operationsFactory.newIterationEndOperation(iterationId), socketAddress);
      return endResponseStage.handle((endResponse, t) -> {
         if (t != null) {
            HOTROD.ignoringErrorDuringIterationClose(iterationId(iterationId), t);
         } else {
            short status = endResponse.getStatus();

            if (HotRodConstants.isSuccess(status) && HOTROD.isDebugEnabled()) {
               HOTROD.iterationClosed(iterationId(iterationId));
            }
            if (HotRodConstants.isInvalidIteration(status)) {
               throw HOTROD.errorClosingIteration(iterationId(iterationId));
            }
         }
         return null;
      });
   }

   String iterationId(byte[] iterationId) {
      return new String(iterationId, HotRodConstants.HOTROD_STRING_CHARSET);
   }

   void completeSegments(IntSet completedSegments) {
      if (segments != null) {
         segments.removeAll(completedSegments);
      }
   }

   CompletionStage<IterationStartResponse> newIteratorStartOperation(SocketAddress address, IntSet segments,
         int batchSize) {
      HotRodOperation<IterationStartResponse> op = operationsFactory.newIterationStartOperation(filterConverterFactory,
            filterParams, segments, batchSize, metadata);
      if (address == null) {
         return dispatcher.execute(op);
      }
      return dispatcher.executeOnSingleAddress(op, address);
   }

   CompletionStage<IterationNextResponse<K, E>> newIteratorNextOperation(byte[] iterationId, SocketAddress socketAddress) {
      HotRodOperation<IterationNextResponse<K, E>> op = operationsFactory.newIterationNextOperation(iterationId,
            segmentKeyTracker);
      return dispatcher.executeOnSingleAddress(op, socketAddress);
   }
}
