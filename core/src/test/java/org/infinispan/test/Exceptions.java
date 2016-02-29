package org.infinispan.test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.fail;

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
      assertNotNull("Should have thrown an " + exceptionClass, t);
      assertEquals("Wrong exception: " + t, exceptionClass, t.getClass());
   }

   public static void assertException(Class<? extends Throwable> exceptionClass, String messageRegex,
         Throwable t) {
      assertException(exceptionClass, t);
      Pattern pattern = Pattern.compile(messageRegex);
      if (!pattern.matcher(t.getMessage()).matches()) {
         fail("Wrong exception message: " + t.getMessage());
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

   public static void expectException(Class<? extends Throwable> exceptionClass, String messageRegex,
         ExceptionRunnable runnable) {
      Throwable t = extractException(runnable);
      assertException(exceptionClass, messageRegex, t);
   }

   public static void expectException(Class<? extends Throwable> wrapperExceptionClass,
         Class<? extends Throwable> exceptionClass,
         String messageRegex, ExceptionRunnable runnable) {
      Throwable t = extractException(runnable);
      assertException(wrapperExceptionClass, t);
      assertException(exceptionClass, messageRegex, t.getCause());
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

   public static void expectExecutionException(Class<? extends Throwable> exceptionClass, String messageRegex,
         Future<?> future) {
      Throwable t = extractException(() -> future.get(10, TimeUnit.SECONDS));
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
