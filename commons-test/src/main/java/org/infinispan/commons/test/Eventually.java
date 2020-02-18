package org.infinispan.commons.test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.junit.ComparisonFailure;

public class Eventually {

   public static final int DEFAULT_TIMEOUT_MILLIS = 10000;
   public static final int DEFAULT_POLL_INTERVAL_MILLIS = 100;

   @FunctionalInterface
   public interface Condition {
      boolean isSatisfied() throws Exception;
   }

   public static void eventually(Supplier<AssertionError> assertionErrorSupplier, Condition ec, long timeout,
                                 long pollInterval, TimeUnit unit) {
      if (pollInterval <= 0) {
         throw new IllegalArgumentException("Check interval must be positive");
      }
      try {
         long expectedEndTime = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeout, unit);
         long sleepMillis = MILLISECONDS.convert(pollInterval, unit);
         do {
            if (ec.isSatisfied()) return;

            Thread.sleep(sleepMillis);
         } while (expectedEndTime - System.nanoTime() > 0);

         throw assertionErrorSupplier.get();
      } catch (Exception e) {
         throw new RuntimeException("Unexpected!", e);
      }
   }

   public static void eventually(String message, Condition ec, long timeout, long pollInterval, TimeUnit unit) {
      eventually(() -> new AssertionError(message), ec, timeout, pollInterval, unit);
   }

   public static void eventually(Condition ec) {
      eventually(ec, DEFAULT_TIMEOUT_MILLIS);
   }

   public static void eventually(Condition ec, long timeoutMillis) {
      eventually(ec, timeoutMillis, MILLISECONDS);
   }

   public static void eventually(String message, Condition ec) {
      eventually(message, ec, DEFAULT_TIMEOUT_MILLIS, DEFAULT_POLL_INTERVAL_MILLIS, MILLISECONDS);
   }

   public static void eventually(Condition ec, long timeout, TimeUnit unit) {
      eventually(() -> new AssertionError(), ec, unit.toMillis(timeout), DEFAULT_POLL_INTERVAL_MILLIS, MILLISECONDS);
   }

   public static void eventually(Condition ec, long timeout, long pollInterval, TimeUnit unit) {
      eventually(() -> new AssertionError(), ec, timeout, pollInterval, unit);
   }

   public static <T> void eventuallyEquals(T expected, Supplier<T> supplier, long timeout, long pollInterval,
                                           TimeUnit unit) {
      if (pollInterval <= 0) {
         throw new IllegalArgumentException("Check interval must be positive");
      }
      try {
         long expectedEndTime = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeout, unit);
         long sleepMillis = MILLISECONDS.convert(pollInterval, unit);
         T value;
         do {
            value = supplier.get();
            if (Objects.equals(expected, value)) return;

            Thread.sleep(sleepMillis);
         } while (expectedEndTime - System.nanoTime() > 0);

         if (expected instanceof String && value instanceof String) {
            throw new ComparisonFailure(null, (String) expected, (String) value);
         } else {
            String expectedClass = expected != null ? expected.getClass().getSimpleName() : "";
            String valueClass = value != null ? value.getClass().getSimpleName() : "";

            throw new AssertionError(
                  String.format("expected: %s<%s>, but was %s<%s>", expectedClass, expected, valueClass, value));
         }
      } catch (Exception e) {
         throw new RuntimeException("Unexpected!", e);
      }
   }

   public static <T> void eventuallyEquals(T expected, Supplier<T> supplier, long timeout, TimeUnit unit) {
      eventuallyEquals(expected, supplier, unit.toMillis(timeout), DEFAULT_POLL_INTERVAL_MILLIS, MILLISECONDS);
   }

   public static <T> void eventuallyEquals(T expected, Supplier<T> supplier) {
      eventuallyEquals(expected, supplier, DEFAULT_TIMEOUT_MILLIS, DEFAULT_POLL_INTERVAL_MILLIS, MILLISECONDS);
   }
}
