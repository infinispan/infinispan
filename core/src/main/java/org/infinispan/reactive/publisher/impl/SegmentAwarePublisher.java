package org.infinispan.reactive.publisher.impl;

import java.util.function.IntConsumer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * This is the same as {@link SegmentCompletionPublisher} except that it also allows listening for when a segment is
 * lost. The lost segment provides the same notification guarantees as the segment completion of the parent interface.
 * <p>
 * This interface is normally just for internal Infinispan usage as users shouldn't normally have to care about retrying.
 * <p>
 * If segment completion is not needed, use the {@link Publisher#subscribe(Subscriber)} or provided
 * {@link #EMPTY_CONSUMER} as the argument to both of the arguments in the
 * {@link #subscribe(Subscriber, IntConsumer, IntConsumer)} method. This allows implementors to optimize for the case
 * when segment completion/loss is not needed as this may require additional overhead.
 * @param <R> value type
 */
@FunctionalInterface
public interface SegmentAwarePublisher<R> extends SegmentCompletionPublisher<R> {

   /**
    * Same as {@link SegmentCompletionPublisher#subscribe(Subscriber, IntConsumer)}, except that we also can notify a
    * listener when a segment has been lost before publishing all its entries
    * @param s subscriber to be notified of values and completion
    * @param completedSegmentConsumer segment notifier to notify
    * @param lostSegmentConsumer segment notifier to notify of lost segments
    */
   void subscribe(Subscriber<? super R> s, IntConsumer completedSegmentConsumer, IntConsumer lostSegmentConsumer);

   @Override
   default void subscribe(Subscriber<? super R> s, IntConsumer completedSegmentConsumer) {
      subscribe(s, completedSegmentConsumer, EMPTY_CONSUMER);
   }
}
