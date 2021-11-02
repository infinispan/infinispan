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
public interface SegmentPublisherSupplier<R> {
   /**
    * Wrapper around an element returned that can either be a value or a segment completion. Note that the user
    * should invoke {@link #isSegmentComplete()} or {@link #isValue()} to determine which type it is.
    *
    * @param <R> the value type if present
    */
   interface Notification<R> {
      /**
       * Whether this notification contains a value, always non null if so
       *
       * @return true if a value is present
       */
      boolean isValue();

      /**
       * Whether this notification is for a completed segmented
       *
       * @return true if a segment is complete
       */
      boolean isSegmentComplete();

      /**
       * The value when present for this notification
       *
       * @return the value
       * @throws IllegalStateException if this notification is segment complete
       */
      R value();

      /**
       * The segment that was complete for this notification
       *
       * @return the segment
       * @throws IllegalStateException if this notification contains a value
       */
      int completedSegment();
   }

   Publisher<R> publisherWithoutSegments();

   Publisher<Notification<R>> publisherWithSegments();
}
