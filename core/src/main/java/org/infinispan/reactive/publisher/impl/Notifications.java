package org.infinispan.reactive.publisher.impl;

import java.util.Objects;

public class Notifications {
   private Notifications() {
   }

   public interface NotificationBuilder<R> {
      SegmentAwarePublisherSupplier.NotificationWithLost<R> value(R value, int segment);

      SegmentAwarePublisherSupplier.NotificationWithLost<R> segmentComplete(int segment);

      SegmentAwarePublisherSupplier.NotificationWithLost<R> segmentLost(int segment);
   }

   public static <R> NotificationBuilder<R> reuseBuilder() {
      return new ReuseNotificationBuilder<>();
   }

   public static <R> NotificationBuilder<R> newBuilder() {
      return new NewBuilder<>();
   }

   private static class NewBuilder<R> implements NotificationBuilder<R> {

      @Override
      public SegmentAwarePublisherSupplier.NotificationWithLost<R> value(R value, int segment) {
         return Notifications.value(value, segment);
      }

      @Override
      public SegmentAwarePublisherSupplier.NotificationWithLost<R> segmentComplete(int segment) {
         return Notifications.segmentComplete(segment);
      }

      @Override
      public SegmentAwarePublisherSupplier.NotificationWithLost<R> segmentLost(int segment) {
         return Notifications.segmentLost(segment);
      }
   }

   static class ReuseNotificationBuilder<R> implements SegmentAwarePublisherSupplier.NotificationWithLost<R>, NotificationBuilder<R> {
      // value != null => value and segment
      // value == null && segment >= 0 => segment completed
      // value == null && segment < 0 => -segment-1 lost
      R value;
      int segment;

      @Override
      public SegmentAwarePublisherSupplier.NotificationWithLost<R> value(R value, int segment) {
         this.value = value;
         if (segment < 0) {
            throw new IllegalArgumentException("Segment must be 0 or greater");
         }
         this.segment = segment;
         return this;
      }

      @Override
      public SegmentAwarePublisherSupplier.NotificationWithLost<R> segmentComplete(int segment) {
         this.value = null;
         this.segment = segment;
         return this;
      }

      @Override
      public SegmentAwarePublisherSupplier.NotificationWithLost<R> segmentLost(int segment) {
         this.value = null;
         this.segment = -segment - 1;
         return this;
      }

      @Override
      public boolean isLostSegment() {
         return value == null && segment < 0;
      }

      @Override
      public int lostSegment() {
         if (!isLostSegment()) {
            return SegmentAwarePublisherSupplier.NotificationWithLost.super.lostSegment();
         }
         return -segment - 1;
      }

      @Override
      public boolean isValue() {
         return value != null;
      }

      @Override
      public boolean isSegmentComplete() {
         return value == null && segment >= 0;
      }

      @Override
      public R value() {
         if (!isValue()) {
            return SegmentAwarePublisherSupplier.NotificationWithLost.super.value();
         }
         return value;
      }

      @Override
      public int valueSegment() {
         if (!isValue()) {
            return SegmentAwarePublisherSupplier.NotificationWithLost.super.valueSegment();
         }
         return segment;
      }

      @Override
      public int completedSegment() {
         if (!isSegmentComplete()) {
            return SegmentAwarePublisherSupplier.NotificationWithLost.super.completedSegment();
         }
         return segment;
      }

      @Override
      public String toString() {
         return "ReuseNotificationBuilder{" +
               (value != null ? "value=" + value : "") +
               (value != null ? ", segment=" : (segment > 0 ? "completed segment=" : "lost segment")) + segment +
               '}';
      }
   }

   public static <R> SegmentAwarePublisherSupplier.NotificationWithLost<R> value(R value, int segment) {
      return new ValueNotification<>(value, segment);
   }

   public static <R> SegmentAwarePublisherSupplier.NotificationWithLost<R> segmentComplete(int segment) {
      return new ValueNotification<>(segment, true);
   }

   public static <R> SegmentAwarePublisherSupplier.NotificationWithLost<R> segmentLost(int segment) {
      return new ValueNotification<>(segment, false);
   }

   static class ValueNotification<R> implements SegmentAwarePublisherSupplier.NotificationWithLost<R> {
      // value != null => value and segment
      // value == null && segment >= 0 => segment completed
      // value == null && segment < 0 => -segment-1 lost
      protected final R value;
      protected final int segment;

      public ValueNotification(R value, int segment) {
         this.value = value;
         if (segment < 0) {
            throw new IllegalArgumentException("Segment must be 0 or greater");
         }
         this.segment = segment;
      }

      public ValueNotification(int segment, boolean segmentComplete) {
         this.value = null;
         this.segment = segmentComplete ? segment : -segment - 1;
      }

      @Override
      public boolean isLostSegment() {
         return segment < 0;
      }

      @Override
      public boolean isValue() {
         return value != null;
      }

      @Override
      public boolean isSegmentComplete() {
         return value == null && segment >= 0;
      }

      @Override
      public R value() {
         if (value != null)
            return value;

         return SegmentAwarePublisherSupplier.NotificationWithLost.super.value();
      }

      @Override
      public int valueSegment() {
         if (value != null)
            return segment;

         return SegmentAwarePublisherSupplier.NotificationWithLost.super.valueSegment();
      }

      @Override
      public int completedSegment() {
         if (value == null && segment >= 0)
            return segment;

         return SegmentAwarePublisherSupplier.NotificationWithLost.super.completedSegment();
      }

      @Override
      public int lostSegment() {
         if (segment < 0)
            return -segment - 1;

         return SegmentAwarePublisherSupplier.NotificationWithLost.super.lostSegment();
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         ValueNotification<?> that = (ValueNotification<?>) o;
         return segment == that.segment && Objects.equals(value, that.value);
      }

      @Override
      public int hashCode() {
         return segment * 31 + Objects.hashCode(value);
      }


      @Override
      public String toString() {
         return "ValueNotification{" +
               (value != null ? "value=" + value : "") +
               (value != null ? ", segment=" : (segment > 0 ? "completed segment=" : "lost segment")) + segment +
               '}';
      }
   }
}
