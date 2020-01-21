package org.infinispan.server.test;

import static org.junit.Assert.assertEquals;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Test utilities
 *
 * @since 11.0
 */
public class Util {
   public static <T> void eventuallyEquals(T expected, Supplier<T> supplier) {
      long endNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (System.nanoTime() - endNanos < 0) {
         if (Objects.equals(expected, supplier.get()))
            return;
      }
      assertEquals(expected, supplier.get());
   }
}
