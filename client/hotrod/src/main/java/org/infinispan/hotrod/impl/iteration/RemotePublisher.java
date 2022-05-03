package org.infinispan.hotrod.impl.iteration;

import static org.infinispan.hotrod.impl.logging.Log.HOTROD;

import java.lang.invoke.MethodHandles;
import java.net.SocketAddress;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.commons.reactive.RxJavaInterop;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.hotrod.impl.logging.Log;
import org.infinispan.hotrod.impl.logging.LogFactory;
import org.infinispan.hotrod.impl.operations.CacheOperationsFactory;
import org.infinispan.hotrod.impl.operations.IterationEndResponse;
import org.infinispan.hotrod.impl.operations.IterationNextOperation;
import org.infinispan.hotrod.impl.operations.IterationNextResponse;
import org.infinispan.hotrod.impl.operations.IterationStartOperation;
import org.infinispan.hotrod.impl.operations.IterationStartResponse;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import io.netty.channel.Channel;
import io.reactivex.rxjava3.core.Flowable;

public class RemotePublisher<K, E> implements Publisher<CacheEntry<K, E>> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private final CacheOperationsFactory cacheOperationsFactory;
   private final String filterConverterFactory;
   private final byte[][] filterParams;
   private final IntSet segments;
   private final int batchSize;
   private final boolean metadata;
   private final DataFormat dataFormat;
   private final KeyTracker segmentKeyTracker;

   private final Set<SocketAddress> failedServers = ConcurrentHashMap.newKeySet();

   public RemotePublisher(CacheOperationsFactory cacheOperationsFactory, String filterConverterFactory,
                          byte[][] filterParams, Set<Integer> segments, int batchSize, boolean metadata, DataFormat dataFormat) {
      this.cacheOperationsFactory = cacheOperationsFactory;
      this.filterConverterFactory = filterConverterFactory;
      this.filterParams = filterParams;
      SegmentConsistentHash segmentConsistentHash = (SegmentConsistentHash) cacheOperationsFactory.getConsistentHash();
      if (segments == null) {
         if (segmentConsistentHash != null) {
            int maxSegment = segmentConsistentHash.getNumSegments();
            this.segments = IntSets.mutableEmptySet(maxSegment);
            for (int i = 0; i < maxSegment; ++i) {
               this.segments.set(i);
            }
         } else {
            this.segments = null;
         }
      } else {
         this.segments = IntSets.mutableCopyFrom(segments);
      }
      this.batchSize = batchSize;
      this.metadata = metadata;
      this.dataFormat = dataFormat;
      this.segmentKeyTracker = KeyTrackerFactory.create(dataFormat, segmentConsistentHash,
            cacheOperationsFactory.getTopologyId(), segments);
   }

   @Override
   public void subscribe(Subscriber<? super CacheEntry<K, E>> subscriber) {
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
               Map<SocketAddress, Set<Integer>> segmentsByAddress = cacheOperationsFactory.getPrimarySegmentsByAddress();
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
      failedServers.add(socketAddress);
   }

   CompletionStage<Void> sendCancel(byte[] iterationId, Channel channel) {
      CompletionStage<IterationEndResponse> endResponseStage = cacheOperationsFactory.newIterationEndOperation(iterationId, CacheOptions.DEFAULT, channel).execute();
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
      IterationStartOperation iterationStartOperation = cacheOperationsFactory.newIterationStartOperation(filterConverterFactory,
            filterParams, segments, batchSize, metadata, CacheOptions.DEFAULT, dataFormat, address);
      return iterationStartOperation.execute();
   }

   CompletionStage<IterationNextResponse<K, E>> newIteratorNextOperation(byte[] iterationId, Channel channel) {
      IterationNextOperation<K, E> iterationNextOperation = cacheOperationsFactory.newIterationNextOperation(iterationId,
            channel, segmentKeyTracker, CacheOptions.DEFAULT, dataFormat);
      return iterationNextOperation.execute();
   }
}
