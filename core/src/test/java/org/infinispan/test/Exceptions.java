package org.infinispan.test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

/**
 * Utility methods for testing expected exceptions.
 *
 * @author Dan Berindei
 * @since 8.2
 */
public class Exceptions {
   public interface ExceptionRunnable {
      void run() throws Exception;
   }

   public static void assertException(Class<? extends Throwable> exceptionClass, Throwable t) {
      if (t == null) {
         throw new AssertionError("Should have thrown an " + exceptionClass, null);
      }
      if (t.getClass() != exceptionClass) {
         throw new AssertionError(
               "Wrong exception thrown: expected:<" + exceptionClass + ">, actual:<" + t.getClass() + ">", t);
      }
   }

   public static void assertException(Class<? extends Throwable> exceptionClass, String messageRegex,
         Throwable t) {
      assertException(exceptionClass, t);
      Pattern pattern = Pattern.compile(messageRegex);
      if (!pattern.matcher(t.getMessage()).matches()) {
         throw new AssertionError(
               "Wrong exception message: expected:<" + messageRegex + ">, actual:<" + t.getMessage() + ">",
               t);
      }
   }

   public static void assertExceptionNonStrict(Class<? extends Throwable> exceptionClass, Throwable t) {
      if (t == null) {
         throw new AssertionError("Should have thrown an " + exceptionClass, null);
      }
      if (!exceptionClass.isInstance(t)) {
         throw new AssertionError(
               "Wrong exception thrown: expected:<" + exceptionClass + ">, actual:<" + t.getClass() + ">", t);
      }
   }

   public static void assertException(Class<? extends Throwable> wrapperExceptionClass,
         Class<? extends Throwable> exceptionClass, Throwable t) {
      assertException(wrapperExceptionClass, t);
      assertException(exceptionClass, t.getCause());
   }

   public static void assertException(Class<? extends Throwable> wrapperExceptionClass,
         Class<? extends Throwable> exceptionClass, String messageRegex, Throwable t) {
      assertException(wrapperExceptionClass, t);
      assertException(exceptionClass, messageRegex, t.getCause());
   }

   public static void assertException(Class<? extends Throwable> wrapperExceptionClass2,
                                      Class<? extends Throwable> wrapperExceptionClass,
                                      Class<? extends Throwable> exceptionClass, Throwable t) {
      assertException(wrapperExceptionClass2, t);
      assertException(wrapperExceptionClass, t.getCause());
      assertException(exceptionClass, t.getCause().getCause());
   }

   public static void assertException(Class<? extends Throwable> wrapperExceptionClass3,
                                      Class<? extends Throwable> wrapperExceptionClass2,
                                      Class<? extends Throwable> wrapperExceptionClass,
                                      Class<? extends Throwable> exceptionClass, Throwable t) {
      assertException(wrapperExceptionClass3, t);
      assertException(wrapperExceptionClass2, t.getCause());
      assertException(wrapperExceptionClass, t.getCause().getCause());
      assertException(exceptionClass, t.getCause().getCause().getCause());
   }

   public static void expectException(Class<? extends Throwable> exceptionClass, String messageRegex,
         ExceptionRunnable runnable) {
      Throwable t = extractException(runnable);
      assertException(exceptionClass, messageRegex, t);
   }

   public static void expectException(Class<? extends Throwable> wrapperExceptionClass,
         Class<? extends Throwable> exceptionClass,
         String messageRegex, ExceptionRunnable runnable) {
      Throwable t = extractException(runnable);
      assertException(wrapperExceptionClass, exceptionClass, messageRegex, t);
   }

   public static void expectException(Class<? extends Throwable> wrapperExceptionClass2,
         Class<? extends Throwable> wrapperExceptionClass1, Class<? extends Throwable> exceptionClass,
         ExceptionRunnable runnable) {
      Throwable t = extractException(runnable);
      assertException(wrapperExceptionClass2, wrapperExceptionClass1, exceptionClass, t);
   }

   public static void expectException(Class<? extends Throwable> exceptionClass, ExceptionRunnable runnable) {
      Throwable t = extractException(runnable);
      assertException(exceptionClass, t);
   }

   public static void expectException(Class<? extends Throwable> wrapperExceptionClass,
         Class<? extends Throwable> exceptionClass, ExceptionRunnable runnable) {
      Throwable t = extractException(runnable);
      assertException(wrapperExceptionClass, t);
      assertException(exceptionClass, t.getCause());
   }

   public static void expectExceptionNonStrict(Class<? extends Throwable> we2, Class<? extends Throwable> we1, Class<? extends Throwable> e, ExceptionRunnable runnable) {
      Throwable t = extractException(runnable);
      assertExceptionNonStrict(we2, t);
      assertExceptionNonStrict(we1, t.getCause());
      assertExceptionNonStrict(e, t.getCause().getCause());
   }


   public static void expectExecutionException(Class<? extends Throwable> exceptionClass, String messageRegex,
         Future<?> future) {
      expectExecutionException(exceptionClass, messageRegex, future, 10, TimeUnit.SECONDS);
   }

   public static void expectExecutionException(Class<? extends Throwable> exceptionClass, String messageRegex, Future<?> future, long timeout, TimeUnit unit) {
      Throwable t = extractException(() -> future.get(timeout, unit));
      assertException(ExecutionException.class, t);
      assertException(exceptionClass, messageRegex, t.getCause());
   }

   public static void expectExecutionException(Class<? extends Throwable> wrapperExceptionClass,
         Class<? extends Throwable> exceptionClass, String messageRegex, Future<?> future) {
      Throwable t = extractException(() -> future.get(10, TimeUnit.SECONDS));
      assertException(ExecutionException.class, t);
      assertException(wrapperExceptionClass, t.getCause());
      assertException(exceptionClass, messageRegex, t.getCause().getCause());
   }

   public static void expectExecutionException(Class<? extends Throwable> wrapperExceptionClass2,
         Class<? extends Throwable> wrapperExceptionClass, Class<? extends Throwable> exceptionClass,
         String messageRegex, Future<?> future) {
      Throwable t = extractException(() -> future.get(10, TimeUnit.SECONDS));
      assertException(ExecutionException.class, t);
      assertException(wrapperExceptionClass2, t.getCause());
      assertException(wrapperExceptionClass, exceptionClass, messageRegex, t.getCause().getCause());
   }

   public static void expectExecutionException(Class<? extends Throwable> exceptionClass, Future<?> future) {
      Throwable t = extractException(() -> future.get(10, TimeUnit.SECONDS));
      assertException(ExecutionException.class, t);
      assertException(exceptionClass, t.getCause());
   }

   public static void expectExecutionException(Class<? extends Throwable> wrapperExceptionClass,
         Class<? extends Throwable> exceptionClass, Future<?> future) {
      Throwable t = extractException(() -> future.get(10, TimeUnit.SECONDS));
      assertException(ExecutionException.class, t);
      assertException(wrapperExceptionClass, t.getCause());
      assertException(exceptionClass, t.getCause().getCause());
   }

   public static void expectExecutionException(Class<? extends Throwable> wrapperExceptionClass2,
         Class<? extends Throwable> wrapperExceptionClass, Class<? extends Throwable> exceptionClass,
         Future<?> future) {
      Throwable t = extractException(() -> future.get(10, TimeUnit.SECONDS));
      assertException(ExecutionException.class, t);
      assertException(wrapperExceptionClass2, t.getCause());
      assertException(wrapperExceptionClass, exceptionClass, t.getCause().getCause());
   }

   private static Throwable extractException(ExceptionRunnable runnable) {
      Throwable exception = null;
      try {
         runnable.run();
      } catch (Throwable t) {
         exception = t;
      }
      return exception;
   }
}
