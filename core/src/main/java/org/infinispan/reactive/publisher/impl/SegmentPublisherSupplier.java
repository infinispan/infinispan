package org.infinispan.reactive.publisher.impl;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * A {@link Publisher} that also allows receiving segment completion information if desired. If segment information
 * is needed use {@link #publisherWithSegments()} which publishes the {@link Notification} instances that can either
 * contain a value or a segment completion notification. In that latter case the notification the notification should be
 * checked if it is for a value {@link Notification#isValue()} or a segment completion {@link Notification#isSegmentComplete()}.
 * <p>
 * After a segment complete notification has been published no other values from that segment will be published again before
 * the publisher completes.
 * Also note that it is possible for a segment to have no values, so there is no guarantee a prior value mapped to a
 * given segment.
 * <p>
 * If segment completion is not needed, use the {@link Publisher#subscribe(Subscriber)}. This allows
 * implementors to optimize for the case when segment completion is not needed as this may require additional overhead.
 * @param <R>
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
      default R value() {
         throw new IllegalStateException("Notification does not contain a value, please check with isValue first!");
      }

      /**
       * The segment that maps for the value of this notification
       *
       * @return the segment
       * @throws IllegalStateException if this notification is segment complete
       */
      default int valueSegment() {
         throw new IllegalStateException("Notification does not contain a value segment, please check with isValue first!");
      }

      /**
       * The segment that was complete for this notification
       *
       * @return the segment
       * @throws IllegalStateException if this notification contains a value
       */
      default int completedSegment() {
         throw new IllegalStateException("Notification does not contain a completed segment, please check with isSegmentComplete first!");
      }
   }

   Publisher<R> publisherWithoutSegments();

   Publisher<Notification<R>> publisherWithSegments();
}
