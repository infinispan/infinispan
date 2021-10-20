package org.infinispan.reactive.publisher.impl;

import org.reactivestreams.Subscriber;

/**
 * This is the same as {@link SegmentCompletionPublisher} except that it also allows listening for when a segment is
 * lost. The lost segment provides the same notification guarantees as the segment completion of the parent interface.
 * <p>
 * This interface is normally just for internal Infinispan usage as users shouldn't normally have to care about retrying.
 * <p>
 * Implementors of this do not do retries and instead notify of lost segments instead of retrying, which implementors
 * of {@link SegmentCompletionPublisher} normally do.
 *
 * @param <R> value type
 */
public interface SegmentAwarePublisher<R> extends SegmentCompletionPublisher<R> {

   /**
    * Notification that can also contains lost segments. Note that the lost segments are mutually exclusive with
    * value and completed segments.
    *
    * @param <R> the value type if present
    */
   interface NotificationWithLost<R> extends SegmentCompletionPublisher.Notification<R> {
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
    * Same as {@link SegmentCompletionPublisher#subscribeWithSegments(Subscriber)} , except that we also can notify a
    * listener when a segment has been lost before publishing all its entries
    *
    * @param subscriber subscriber to be notified of values, segment completion and segment lost
    */
   void subscribeWithLostSegments(Subscriber<? super NotificationWithLost<R>> subscriber);

   /**
    * When this method is used the {@link DeliveryGuarantee} is ignored as the user isn't listening to completion or
    * lost segments
    *
    * @param s
    */
   @Override
   void subscribe(Subscriber<? super R> s);
}
