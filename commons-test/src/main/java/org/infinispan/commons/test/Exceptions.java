package org.infinispan.commons.test;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletionStage;
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

   public static void assertException(Class<? extends Throwable> exceptionClass, Throwable t) {
      if (t == null) {
         throw new AssertionError("Should have thrown an " + exceptionClass.getName(), null);
      }
      if (t.getClass() != exceptionClass) {
         throw new AssertionError(
               "Wrong exception thrown: expected:<" + exceptionClass.getName() + ">, actual:<" + t.getClass().getName() + ">", t);

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

   /**
    * Expect an exception of class {@code exceptionClass} or its subclasses.
    */
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

   public static void expectExceptionNonStrict(Class<? extends Throwable> e, ExceptionRunnable runnable) {
      Throwable t = extractException(runnable);
      assertExceptionNonStrict(e, t);
   }

   public static void expectExceptionNonStrict(Class<? extends Throwable> we1, Class<? extends Throwable> e, ExceptionRunnable runnable) {
      Throwable t = extractException(runnable);
      assertExceptionNonStrict(we1, t);
      assertExceptionNonStrict(e, t.getCause());
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

   public static void expectCompletionException(Class<? extends Throwable> exceptionClass, String messageRegex,
                                               CompletionStage<?> stage) {
      expectCompletionException(exceptionClass, messageRegex, stage, 10, TimeUnit.SECONDS);
   }

   public static void expectCompletionException(Class<? extends Throwable> exceptionClass, String messageRegex,
                                                CompletionStage<?> stage, long timeout, TimeUnit unit) {
      // Need to use get() for the timeout, but that converts the exception to an ExecutionException
      Throwable t = extractException(() -> stage.toCompletableFuture().get(timeout, unit));
      assertException(ExecutionException.class, t);
      assertException(exceptionClass, messageRegex, t.getCause());
   }

   public static void expectCompletionException(Class<? extends Throwable> wrapperExceptionClass,
                                               Class<? extends Throwable> exceptionClass, String messageRegex,
                                                CompletionStage<?> stage) {
      // Need to use get() for the timeout, but that converts the exception to an ExecutionException
      Throwable t = extractException(() -> stage.toCompletableFuture().get(10, TimeUnit.SECONDS));
      assertException(ExecutionException.class, t);
      assertException(wrapperExceptionClass, t.getCause());
      assertException(exceptionClass, messageRegex, t.getCause().getCause());
   }

   public static void expectCompletionException(Class<? extends Throwable> wrapperExceptionClass2,
                                               Class<? extends Throwable> wrapperExceptionClass, Class<? extends Throwable> exceptionClass,
                                               String messageRegex, CompletionStage<?> stage) {
      // Need to use get() for the timeout, but that converts the exception to an ExecutionException
      Throwable t = extractException(() -> stage.toCompletableFuture().get(10, TimeUnit.SECONDS));
      assertException(ExecutionException.class, t);
      assertException(wrapperExceptionClass2, t.getCause());
      assertException(wrapperExceptionClass, exceptionClass, messageRegex, t.getCause().getCause());
   }

   public static void expectCompletionException(Class<? extends Throwable> exceptionClass, CompletionStage<?> stage) {
      // Need to use get() for the timeout, but that converts the exception to an ExecutionException
      Throwable t = extractException(() -> stage.toCompletableFuture().get(10, TimeUnit.SECONDS));
      assertException(ExecutionException.class, t);
      assertException(exceptionClass, t.getCause());
   }

   public static void expectCompletionException(Class<? extends Throwable> wrapperExceptionClass,
                                               Class<? extends Throwable> exceptionClass, CompletionStage<?> stage) {
      // Need to use get() for the timeout, but that converts the exception to an ExecutionException
      Throwable t = extractException(() -> stage.toCompletableFuture().get(10, TimeUnit.SECONDS));
      assertException(ExecutionException.class, t);
      assertException(wrapperExceptionClass, t.getCause());
      assertException(exceptionClass, t.getCause().getCause());
   }

   public static void expectCompletionException(Class<? extends Throwable> wrapperExceptionClass2,
                                               Class<? extends Throwable> wrapperExceptionClass, Class<? extends Throwable> exceptionClass,
                                               CompletionStage<?> stage) {
      // Need to use get() for the timeout, but that converts the exception to an ExecutionException
      Throwable t = extractException(() -> stage.toCompletableFuture().get(10, TimeUnit.SECONDS));
      assertException(ExecutionException.class, t);
      assertException(wrapperExceptionClass2, t.getCause());
      assertException(wrapperExceptionClass, exceptionClass, t.getCause().getCause());
   }

   public static Throwable extractException(ExceptionRunnable runnable) {
      Throwable exception = null;
      try {
         runnable.run();
      } catch (Throwable t) {
         exception = t;
      }
      return exception;
   }

   public static void unchecked(ExceptionRunnable runnable) {
      try {
         runnable.run();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new RuntimeException(e);
      } catch (RuntimeException e) {
         throw e;
      } catch (Throwable t) {
         throw new RuntimeException(t);
      }
   }

   public static <T> T unchecked(Callable<T> callable) {
      try {
         return callable.call();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new RuntimeException(e);
      } catch (RuntimeException e) {
         throw e;
      } catch (Throwable t) {
         throw new RuntimeException(t);
      }
   }

   public static <T> T uncheckedThrowable(ThrowableSupplier<T> supplier) {
      try {
         return supplier.get();
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new RuntimeException(e);
      } catch (RuntimeException e) {
         throw e;
      } catch (Throwable t) {
         throw new RuntimeException(t);
      }
   }
}
