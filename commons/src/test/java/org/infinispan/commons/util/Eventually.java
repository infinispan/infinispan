package org.infinispan.commons.util;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class Eventually {

   @FunctionalInterface
   public interface Condition {
      boolean isSatisfied() throws Exception;
   }

   public static void eventually(Supplier<AssertionError> assertionErrorSupplier, Condition ec, long timeout, long pollInterval, TimeUnit unit) {
      if (pollInterval <= 0) {
         throw new IllegalArgumentException("Check interval must be positive");
      }
      try {
         long expectedEndTime = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeout, unit);
         long sleepMillis = TimeUnit.MILLISECONDS.convert(pollInterval, unit);
         while (expectedEndTime - System.nanoTime() > 0) {
            if (ec.isSatisfied()) return;
            Thread.sleep(sleepMillis);
         }
         if (!ec.isSatisfied()) {
            throw assertionErrorSupplier.get();
         }
      } catch (Exception e) {
         throw new RuntimeException("Unexpected!", e);
      }
   }

   public static void eventually(String message, Condition ec, long timeout, long pollInterval, TimeUnit unit) {
      eventually(() -> new AssertionError(message), ec, timeout, pollInterval, unit);
   }

   public static void eventually(Condition ec) {
      eventually(ec, 10000);
   }

   public static void eventually(Condition ec, long timeoutMillis) {
      eventually(ec, timeoutMillis, TimeUnit.MILLISECONDS);
   }

   public static void eventually(String message, Condition ec) {
      eventually(message, ec, 10000, 500, TimeUnit.MILLISECONDS);
   }

   public static void eventually(Condition ec, long timeout, TimeUnit unit) {
      eventually(() -> new AssertionError(), ec, unit.toMillis(timeout), 500, TimeUnit.MILLISECONDS);
   }

   public static void eventually(Condition ec, long timeout, long pollInterval, TimeUnit unit) {
      eventually(() -> new AssertionError(), ec, timeout, pollInterval, unit);
   }
}
