package org.infinispan.reactive.publisher.impl;

import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.CacheSet;
import org.infinispan.commons.util.IntSet;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.reactive.RxJavaInterop;
import org.infinispan.reactive.publisher.impl.commands.reduction.PublisherResult;
import org.infinispan.reactive.publisher.impl.commands.reduction.SegmentPublisherResult;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;

/**
 * LocalPublisherManager that handles cases when a non segmented store are present. In this case we optimize
 * the retrieval to not use individual segments as this would cause multiple full scans of the underlying
 * store. In this case we submit a task for all segments requested and process the results concurrently if
 * requested.
 * @author wburns
 * @since 10.0
 */
@Scope(Scopes.NAMED_CACHE)
public class NonSegmentedLocalPublisherManagerImpl<K, V> extends LocalPublisherManagerImpl<K, V> {
   static final int PARALLEL_BATCH_SIZE = 1024;

   @Override
   protected <I, R> Flowable<R> exactlyOnceParallel(CacheSet<I> set,
         Set<K> keysToExclude, Function<I, K> toKeyFunction, IntSet segments,
         Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
         SegmentListener listener, IntSet concurrentSegments) {
      Flowable<I> flowable = Flowable.fromPublisher(set.localPublisher(segments));

      if (keysToExclude != null) {
         flowable = flowable.filter(i -> !keysToExclude.contains(toKeyFunction.apply(i)));
      }

      return combineStages(flowable.buffer(PARALLEL_BATCH_SIZE)
            .parallel()
            .runOn(asyncScheduler)
            .map(buffer -> transformer.apply(Flowable.fromIterable(buffer)))
            .sequential(), true);
   }

   @Override
   protected <I, R> Flowable<R> exactlyOnceSequential(CacheSet<I> set,
         Set<K> keysToExclude, Function<I, K> toKeyFunction, IntSet segments,
         Function<? super Publisher<I>, ? extends CompletionStage<R>> transformer,
         SegmentListener listener, IntSet concurrentSegments) {
      Flowable<I> flowable = Flowable.fromPublisher(set.localPublisher(segments));

      if (keysToExclude != null) {
         flowable = flowable.filter(i -> !keysToExclude.contains(toKeyFunction.apply(i)));
      }
      return RxJavaInterop.completionStageToMaybe(transformer.apply(flowable))
            .toFlowable();
   }

   @Override
   protected <R> CompletionStage<PublisherResult<R>> exactlyOnceHandleLostSegments(CompletionStage<R> finalValue, SegmentListener listener) {
      return finalValue.thenApply(value -> {
         IntSet lostSegments = listener.segmentsLost;
         if (lostSegments.isEmpty()) {
            return LocalPublisherManagerImpl.<R>ignoreSegmentsFunction().apply(value);
         } else {
            // We treat all segments as being lost if any are lost in ours
            // NOTE: we never remove any segments from this set at all - so it will contain all requested segments
            return new SegmentPublisherResult<R>(listener.segments, null);
         }
      }).whenComplete((u, t) -> changeListener.remove(listener));
   }
}
