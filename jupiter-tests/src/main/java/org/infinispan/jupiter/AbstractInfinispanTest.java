package org.infinispan.jupiter;

import static org.junit.jupiter.api.Assertions.fail;

import java.lang.invoke.MethodHandles;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;

import org.infinispan.commons.test.ExceptionRunnable;
import org.infinispan.commons.time.TimeService;
import org.infinispan.util.EmbeddedTimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.junit.jupiter.api.extension.RegisterExtension;

// OrderByInstance.class is used to update method names based upon parameters to avoid duplicate test names
// Shouldn't be required for @ParameterizedTest
// TODO Is it better to use @ParameterizedTest everywhere, or is there a way to create a Factory of JUnit 5 tests with
// contructor args?
public abstract class AbstractInfinispanTest {

   private static final Class<?> testClass = MethodHandles.lookup().lookupClass();
   protected static final Log log = LogFactory.getLog(testClass);

   @RegisterExtension
   protected static final TestThreadExtension threadExt = new TestThreadExtension(testClass);
   public static final TimeService TIME_SERVICE = new EmbeddedTimeService();

   protected interface Condition {
      boolean isSatisfied() throws Exception;
   }

   protected <T> void eventuallyEquals(T expected, Supplier<T> supplier) {
      eventually(() -> "expected:<" + expected + ">, got:<" + supplier.get() + ">",
            () -> Objects.equals(expected, supplier.get()));
   }

   protected static <T> void eventuallyEquals(String message, T expected, Supplier<T> supplier) {
      eventually(() -> message + " expected:<" + expected + ">, got:<" + supplier.get() + ">",
            () -> Objects.equals(expected, supplier.get()));
   }

   protected static void eventually(Supplier<String> messageSupplier, Condition condition) {
      eventually(messageSupplier, condition, 30, TimeUnit.SECONDS);
   }

   protected static void eventually(Supplier<String> messageSupplier, Condition condition, long timeout,
                                    TimeUnit timeUnit) {
      try {
         long timeoutNanos = timeUnit.toNanos(timeout);
         // We want the sleep time to increase in arithmetic progression
         // 30 loops with the default timeout of 30 seconds means the initial wait is ~ 65 millis
         int loops = 30;
         int progressionSum = loops * (loops + 1) / 2;
         long initialSleepNanos = timeoutNanos / progressionSum;
         long sleepNanos = initialSleepNanos;
         long expectedEndTime = System.nanoTime() + timeoutNanos;
         while (expectedEndTime - System.nanoTime() > 0) {
            if (condition.isSatisfied())
               return;
            LockSupport.parkNanos(sleepNanos);
            sleepNanos += initialSleepNanos;
         }
         if (!condition.isSatisfied()) {
            fail(messageSupplier.get());
         }
      } catch (Exception e) {
         throw new RuntimeException("Unexpected!", e);
      }
   }

   protected static void eventually(Condition ec, long timeoutMillis) {
      eventually(ec, timeoutMillis, TimeUnit.MILLISECONDS);
   }

   protected static void eventually(Condition ec, long timeout, TimeUnit unit) {
      eventually(() -> "Condition is still false after " + timeout + " " + unit, ec, timeout, unit);
   }

   protected void eventually(String message, Condition ec, long timeout, TimeUnit unit) {
      eventually(() -> message, ec, unit.toMillis(timeout), TimeUnit.MILLISECONDS);
   }

   protected static void eventually(Condition ec) {
      eventually(ec, 10000, TimeUnit.MILLISECONDS);
   }

   protected void eventually(String message, Condition ec) {
      eventually(message, ec, 10000, TimeUnit.MILLISECONDS);
   }

   protected Future<Void> fork(ExceptionRunnable r) {
      return threadExt.submit(new CallableWrapper<>(() -> {
         r.run();
         return null;
      }));
   }

   protected <T> Future<T> fork(Callable<T> c) {
      return threadExt.submit(new CallableWrapper<>(c));
   }


   /**
    * This should normally not be used, use the {@code fork(Runnable|Callable|ExceptionRunnable)}
    * method when an executor is required.
    *
    * Although if you want a limited set of threads this could still be useful for something like
    * {@link java.util.concurrent.Executors#newFixedThreadPool(int, java.util.concurrent.ThreadFactory)} or
    * {@link java.util.concurrent.Executors#newSingleThreadExecutor(java.util.concurrent.ThreadFactory)}
    *
    * @param prefix The prefix starting for the thread factory
    * @return A thread factory that will use the same naming schema as the other methods
    */
   public ThreadFactory getTestThreadFactory(final String prefix) {
      return threadExt.getTestThreadFactory(prefix);
   }

   private static class CallableWrapper<T> implements Callable<T> {
      private final Callable<? extends T> c;

      CallableWrapper(Callable<? extends T> c) {
         this.c = c;
      }

      @Override
      public T call() throws Exception {
         try {
            log.trace("Started fork callable..");
            T result = c.call();
            log.debug("Exiting fork callable.");
            return result;
         } catch (Exception e) {
            log.warn("Exiting fork callable due to exception", e);
            throw e;
         }
      }
   }
}
