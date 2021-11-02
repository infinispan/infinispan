package org.infinispan.reactive.publisher.impl;

import org.reactivestreams.Publisher;

/**
 * This is the same as {@link SegmentPublisherSupplier} except that it also allows listening for when a segment is
 * lost. The lost segment provides the same notification guarantees as the segment completion of the parent interface.
 * <p>
 * This interface is normally just for internal Infinispan usage as users shouldn't normally have to care about retrying.
 * <p>
 * Implementors of this do not do retries and instead notify of lost segments instead of retrying, which implementors
 * of {@link SegmentPublisherSupplier} normally do.
 *
 * @param <R> value type
 */
public interface SegmentAwarePublisherSupplier<R> extends SegmentPublisherSupplier<R> {

   /**
    * Notification that can also contains lost segments. Note that the lost segments are mutually exclusive with
    * value and completed segments.
    *
    * @param <R> the value type if present
    */
   interface NotificationWithLost<R> extends SegmentPublisherSupplier.Notification<R> {
      /**
       * Whether this notification is for a lost segment
       *
       * @return true if a segment was lost
       */
      boolean isLostSegment();

      /**
       * The segment that was complete for this notification
       *
       * @return the segment
       * @throws IllegalStateException if this notification contains a value or has a completed segment
       */
      int lostSegment();
   }

   /**
    * When this method is used the {@link DeliveryGuarantee} is ignored as the user isn't listening to completion or
    * lost segments
    */
   Publisher<R> publisherWithoutSegments();

   /**
    * Same as {@link SegmentPublisherSupplier#publisherWithSegments()} , except that we also can notify a
    * listener when a segment has been lost before publishing all its entries
    */
   Publisher<NotificationWithLost<R>> publisherWithLostSegments();
}
