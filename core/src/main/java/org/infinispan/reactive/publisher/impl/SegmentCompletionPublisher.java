package org.infinispan.reactive.publisher.impl;

import java.util.function.IntConsumer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * A {@link Publisher} that also notifies in a thread safe manner when a segment has sent all values upstream. To more
 * specifically detail the guarantee, the {@code accept} method of the provided {@link IntConsumer} will be invoked
 * serially inline with {@code onNext}, {@code onError}, {@code onComplete} and will only be invoked after all values
 * from the given segment have already been notified via {@code onNext). Note that there is no guarantee that the previous
 * values was from the given segment, only that all have been notified prior.
 * <p>
 * If segment completion is not needed, use the {@link Publisher#subscribe(Subscriber)} or provided
 * {@link #EMPTY_CONSUMER} as the argument to the {@link #subscribe(Subscriber, IntConsumer)} method. This allows
 * implementors to optimize for the case when segment completion is not needed as this may require additional overhead.
 * @param <R> value type
 */
@FunctionalInterface
public interface SegmentCompletionPublisher<R> extends Publisher<R> {
   IntConsumer EMPTY_CONSUMER = v -> { };

   /**
    * Same as {@link org.reactivestreams.Publisher#subscribe(Subscriber)}, except that we also can notify a listener
    * when a segment has published all of its entries
    * @param s subscriber to be notified of values and completion
    * @param completedSegmentConsumer segment notifier to notify
    */
   void subscribe(Subscriber<? super R> s, IntConsumer completedSegmentConsumer);

   @Override
   default void subscribe(Subscriber<? super R> s) {
      subscribe(s, EMPTY_CONSUMER);
   }
}
