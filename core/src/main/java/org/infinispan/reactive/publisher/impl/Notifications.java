package org.infinispan.reactive.publisher.impl;

import java.util.Objects;

public class Notifications {
   private Notifications() {
   }

   public static <R> SegmentAwarePublisher.NotificationWithLost<R> value(R value) {
      return new ValueNotification<>(value);
   }

   public static <R> SegmentAwarePublisher.NotificationWithLost<R> segmentComplete(int segment) {
      return new SegmentNotification<>(segment, true);
   }

   public static <R> SegmentAwarePublisher.NotificationWithLost<R> segmentLost(int segment) {
      return new SegmentNotification<>(segment, false);
   }

   private static class SegmentNotification<R> implements SegmentAwarePublisher.NotificationWithLost<R> {
      private final int segment;
      private final boolean complete;

      public SegmentNotification(int segment, boolean complete) {
         this.segment = segment;
         this.complete = complete;
      }

      @Override
      public boolean isLostSegment() {
         return !complete;
      }

      @Override
      public int lostSegment() {
         if (complete) {
            throw new IllegalStateException("This contains a completed segment, not a lost segment!");
         }
         return segment;
      }

      @Override
      public boolean isValue() {
         return false;
      }

      @Override
      public boolean isSegmentComplete() {
         return complete;
      }

      @Override
      public R value() {
         throw new IllegalStateException("This contains a " + (complete ? "completed" : "lost") + ", not a value!");
      }

      @Override
      public int completedSegment() {
         if (!complete) {
            throw new IllegalStateException("This contains a lost segment, not a completed segment!");
         }
         return segment;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         SegmentNotification that = (SegmentNotification) o;
         return segment == that.segment && complete == that.complete;
      }

      @Override
      public int hashCode() {
         return Objects.hash(segment, complete);
      }

      @Override
      public String toString() {
         return "SegmentNotification{" +
               "segment=" + segment +
               ", complete=" + complete +
               '}';
      }
   }

   private static class ValueNotification<R> implements SegmentAwarePublisher.NotificationWithLost<R> {
      private final R value;

      public ValueNotification(R value) {
         this.value = value;
      }


      @Override
      public boolean isLostSegment() {
         return false;
      }

      @Override
      public int lostSegment() {
         throw new IllegalStateException("This contains a value, not a lost segment!");
      }

      @Override
      public boolean isValue() {
         return true;
      }

      @Override
      public boolean isSegmentComplete() {
         return false;
      }

      @Override
      public R value() {
         return value;
      }

      @Override
      public int completedSegment() {
         throw new IllegalStateException("This contains a value, not a completed segment!");
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         ValueNotification<?> that = (ValueNotification<?>) o;
         return Objects.equals(value, that.value);
      }

      @Override
      public int hashCode() {
         return Objects.hash(value);
      }

      @Override
      public String toString() {
         return "ValueNotification{" +
               "value=" + value +
               '}';
      }
   }

}
