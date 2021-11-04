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
      default boolean isLostSegment() {
         return false;
      }

      /**
       * The segment that was complete for this notification
       *
       * @return the segment
       * @throws IllegalStateException if this notification contains a value or has a completed segment
       */
      default int lostSegment() {
         throw new IllegalStateException("Notification does not contain a lost segment, please check with isLostSegment first!");
      }
   }

   /**
    * When this method is used the {@link DeliveryGuarantee} is ignored as the user isn't listening to completion or
    * lost segments
    */
   Publisher<R> publisherWithoutSegments();

   /**
    * Same as {@link SegmentPublisherSupplier#publisherWithSegments()} , except that we also can notify a
    * listener when a segment has been lost before publishing all its entries.
    * <p>
    * The provided {@link DeliveryGuarantee} when creating this <i>SegmentAwarePublisherSupplier</i> will control
    * how a lost segment notification is raised {@link NotificationWithLost#isLostSegment()}.
    * <h4>Summary of Delivery Guarantee Effects</h4>
    * <table border="1" cellpadding="1" cellspacing="1" summary="Summary of Delivery Guarantee Effects">
    *    <tr>
    *       <th bgcolor="#CCCCFF" align="left">Delivery Guarantee</th>
    *       <th bgcolor="#CCCCFF" align="left">Effect</th>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link DeliveryGuarantee#AT_MOST_ONCE}</td>
    *       <td valign="top">A segment is lost only if this node is not the read owner when starting to read it.
    *       If ths segment is no longer readable after the publisher started, no more entries are returned
    *       and the segment is completed normally.</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link DeliveryGuarantee#AT_LEAST_ONCE}</td>
    *       <td valign="top">Same as {@link DeliveryGuarantee#EXACTLY_ONCE}.</td>
    *    </tr>
    *    <tr>
    *       <td valign="top">{@link DeliveryGuarantee#EXACTLY_ONCE}</td>
    *       <td valign="top">A segment is lost if at any point while reading entries from it,
    *       this node is no longer a read owner of the given segment.
    *       Therefore if the segment is complete, the publisher is guaranteed
    *       to include all values for the given segment.</td>
    *    </tr>
    * </table>
    * <p>
    * If the cache is LOCAL only the {@link DeliveryGuarantee#AT_MOST_ONCE} should be used as there is no difference
    * between the different guarantees, and it is more performant.
    */
   default Publisher<NotificationWithLost<R>> publisherWithLostSegments() {
      return publisherWithLostSegments(false);
   }

   /**
    * Same as {@link SegmentPublisherSupplier#publisherWithSegments()} , except that we also can notify a
    * listener when a segment has been lost before publishing all its entries
    * <p>
    * If <b>reuseNotifications</b> parameter is true then the returned Notifications can be the same object containing
    * different results. This means any consumer must not store the Notification or process them asynchronously
    * or else you could find incorrect values. This parameter is solely for memory and performance uses when it is known
    * that the returned Publisher will be consumed synchronously and process the values and segments immediately.
    *
    * @param reuseNotifications If the returned Publisher can reuse notification objects to save memory
    */
   Publisher<NotificationWithLost<R>> publisherWithLostSegments(boolean reuseNotifications);
}
