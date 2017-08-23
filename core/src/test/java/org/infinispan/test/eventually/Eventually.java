package org.infinispan.test.eventually;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

public class Eventually {

   public static final long DEFAULT_POLL_INTERVAL = 500;
   public static final long DEFAULT_TIMEOUT_MS = 10000;
   public static final TimeUnit DEFAULT_UNIT = TimeUnit.MILLISECONDS;

   public static void eventually(Condition ec) {
      eventually(ec, DEFAULT_TIMEOUT_MS);
   }

   public static void eventually(Condition ec, long timeoutMillis) {
      eventually(ec, timeoutMillis, TimeUnit.MILLISECONDS);
   }

   public static void eventually(Condition ec, long timeout, TimeUnit unit) {
      eventually(() -> null, ec, unit.toMillis(timeout), DEFAULT_POLL_INTERVAL, DEFAULT_UNIT);
   }

   public static void eventually(Condition ec, long timeout, long pollInterval, TimeUnit unit) {
      eventually(() -> null, ec, timeout, pollInterval, unit);
   }

   public static void eventually(String message, Condition ec) {
      eventually(() -> message, ec, DEFAULT_TIMEOUT_MS, DEFAULT_POLL_INTERVAL, DEFAULT_UNIT);
   }

   public static void eventually(Supplier<String> messageSupplier, BooleanSupplier condition) {
      eventually(messageSupplier, () -> condition.getAsBoolean(), DEFAULT_TIMEOUT_MS, DEFAULT_POLL_INTERVAL, DEFAULT_UNIT);
   }

   public static void eventually(String message, Condition ec, long timeout, long pollInterval, TimeUnit unit) {
      eventually(() -> message, ec, timeout, pollInterval, unit);
   }

   public static void eventually(Supplier<String> message, Condition ec, long timeout, long pollInterval, TimeUnit unit) {
      if (pollInterval <= 0) {
         throw new IllegalArgumentException("Check interval must be positive");
      }
      long expectedEndTime = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeout, unit);
      long sleepNanos = TimeUnit.NANOSECONDS.convert(pollInterval, unit);
      try {
         while (expectedEndTime - System.nanoTime() > 0) {
         if (ec.isSatisfied()) return;
            LockSupport.parkNanos(sleepNanos);
         }
         if (!ec.isSatisfied()) {
            //throwing AssertionError to avoid coupling with TestNG and JUnit. This makes it much easier to use
            //in other modules (such as Hibernate).
            throw new AssertionError(message);
         }
      } catch (Exception e) {
         throw new RuntimeException("Unexpected!");
      }
   }
}
