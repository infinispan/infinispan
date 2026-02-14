package org.infinispan.reactive.publisher.impl;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/**
 * A {@link Publisher} that also allows receiving segment completion information if desired. If segment information
 * is needed use {@link #publisherWithSegments()} which publishes the {@link Notification} instances that can either
 * contain a value or a segment completion notification. In that latter case the notification should be
 * checked if it is for a value {@link Notification#isValue()} or a segment completion {@link Notification#isSegmentComplete()}.
 * <p>
 * After a segment complete notification has been published no other values from that segment will be published again before
 * the publisher completes.
 * Also note that it is possible for a segment to have no values, so there is no guarantee a prior value mapped to a
 * given segment.
 * <p>
 * If segment completion is not needed, use the {@link Publisher#subscribe(Subscriber)}. This allows
 * implementors to optimize for the case when segment completion is not needed as this may require additional overhead.
 *
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

   /**
    * Returns a publisher that can be used to subscribe to the values available.
    *
    * @return a Publisher that publishes the resulting values without corresponding segment information
    */
   Publisher<R> publisherWithoutSegments();

   /**
    * A {@link Publisher} that will publish entries as originally configured from possibly remote sources. The published
    * items will be wrapped in a {@link Notification} which can be either an item or segment completion notification.
    * The type can be verified by first invoking {@link Notification#isValue()} or {@link Notification#isSegmentComplete()}
    * after which the value or segment information should be retrieved. Note that each value will also have a segment
    * attributed to it which can be access by invoking {@link Notification#valueSegment()}.
    * <p>
    * Note that segment completion can be interwoven with values and some segments may have no items present.
    * However, once a segment complete notification is encountered for a given segment no additional values will be
    * published to the same subscriber for the given segment.
    * <p>
    * If segment information is not required, please use {@link #publisherWithoutSegments()} as implementations
    * may have additional optimizations in place for when this information is not required.
    *
    * @return a Publisher that publishes the resulting values with segment information
    */
   Publisher<Notification<R>> publisherWithSegments();
}
