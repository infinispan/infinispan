package org.infinispan.commons.util.concurrent;

import static java.util.Objects.requireNonNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.commons.CacheException;

/**
 * Utility methods connecting {@link CompletableFuture} futures.
 *
 * @author Dan Berindei
 * @since 8.0
 */
public final class CompletableFutures {
   public static final CompletableFuture[] EMPTY_ARRAY = new CompletableFuture[0];

   private static final CompletableFuture<Boolean> completedTrueFuture = CompletableFuture.completedFuture(Boolean.TRUE);
   private static final CompletableFuture<Boolean> completedFalseFuture = CompletableFuture.completedFuture(Boolean.FALSE);
   private static final CompletableFuture<?> completedEmptyMapFuture = CompletableFuture.completedFuture(Collections.emptyMap());
   private static final CompletableFuture<?> completedNullFuture = CompletableFuture.completedFuture(null);
   private static final long BIG_DELAY_NANOS = TimeUnit.DAYS.toNanos(1);
   private static final Function<?, ?> TO_NULL = o -> null;
   private static final Function<?, Boolean> TO_TRUE_FUNCTION = o -> Boolean.TRUE;
   private static final Function<?, ?> identity = t -> t;

   private CompletableFutures() {
   }

   @SuppressWarnings("unchecked")
   public static <K, V> CompletableFuture<Map<K, V>> completedEmptyMap() {
      return (CompletableFuture<Map<K, V>>) completedEmptyMapFuture;
   }

   @SuppressWarnings("unchecked")
   public static <T> CompletableFuture<T> completedNull() {
      return (CompletableFuture<T>) completedNullFuture;
   }

   public static CompletableFuture<Boolean> completedTrue() {
      return completedTrueFuture;
   }

   public static CompletableFuture<Boolean> completedFalse() {
      return completedFalseFuture;
   }

   public static CompletionStage<Boolean> booleanStage(boolean trueOrFalse) {
      return trueOrFalse ? completedTrueFuture : completedFalseFuture;
   }

   public static <T> CompletableFuture<List<T>> sequence(List<CompletableFuture<T>> futures) {
      CompletableFuture<Void> all = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
      return all.thenApply(v -> futures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
   }

   public static <T> CompletableFuture<T> completedExceptionFuture(Throwable ex) {
      CompletableFuture<T> future = new CompletableFuture<>();
      future.completeExceptionally(ex);
      return future;
   }

   /**
    * It waits until the {@link CompletableFuture} is completed.
    * <p>
    * It ignore if the {@link CompletableFuture} is completed normally or exceptionally.
    *
    * @param future the {@link CompletableFuture} to test.
    * @param time   the timeout.
    * @param unit   the timeout unit.
    * @return {@code true} if completed, {@code false} if timed out.
    * @throws InterruptedException if interrupted while waiting.
    * @throws NullPointerException if {@code future} or {@code unit} is {@code null}.
    */
   public static boolean await(CompletableFuture<?> future, long time, TimeUnit unit) throws InterruptedException {
      try {
         requireNonNull(future, "Completable Future must be non-null.").get(time, requireNonNull(unit, "Time Unit must be non-null"));
         return true;
      } catch (ExecutionException e) {
         return true;
      } catch (java.util.concurrent.TimeoutException e) {
         return false;
      }
   }

   /**
    * Same as {@link #await(CompletableFuture, long, TimeUnit)} but wraps checked exceptions in {@link CacheException}
    *
    * @param future the {@link CompletableFuture} to test.
    * @param time   the timeout.
    * @param unit   the timeout unit.
    * @return {@code true} if completed, {@code false} if timed out.
    * @throws NullPointerException if {@code future} or {@code unit} is {@code null}.
    */
   public static boolean uncheckedAwait(CompletableFuture<?> future, long time, TimeUnit unit) {
      try {
         requireNonNull(future, "Completable Future must be non-null.").get(time, requireNonNull(unit, "Time Unit must be non-null"));
         return true;
      } catch (ExecutionException e) {
         return true;
      } catch (java.util.concurrent.TimeoutException e) {
         return false;
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      }
   }

   /**
    * Wait for a long time until the {@link CompletableFuture} is completed.
    *
    * @param future the {@link CompletableFuture}.
    * @param <T>    the return type.
    * @return the result value.
    * @throws ExecutionException   if the {@link CompletableFuture} completed exceptionally.
    * @throws InterruptedException if the current thread was interrupted while waiting.
    */
   public static <T> T await(CompletableFuture<T> future) throws ExecutionException, InterruptedException {
      try {
         return Objects.requireNonNull(future, "Completable Future must be non-null.").get(BIG_DELAY_NANOS, TimeUnit.NANOSECONDS);
      } catch (java.util.concurrent.TimeoutException e) {
         throw new IllegalStateException("This should never happen!", e);
      }
   }

   /**
    * Same as {@link #await(CompletableFuture)} but wraps checked exceptions in {@link CacheException}
    *
    * @param future the {@link CompletableFuture}.
    * @param <T>    the return type.
    * @return the result value.
    */
   public static <T> T uncheckedAwait(CompletableFuture<T> future) {
      try {
         return Objects.requireNonNull(future, "Completable Future must be non-null.").get(BIG_DELAY_NANOS, TimeUnit.NANOSECONDS);
      } catch (java.util.concurrent.TimeoutException e) {
         throw new IllegalStateException("This should never happen!", e);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new CacheException(e);
      } catch (ExecutionException e) {
         throw new CacheException(e);
      }
   }

   public static CompletionException asCompletionException(Throwable t) {
      if (t instanceof CompletionException) {
         return ((CompletionException) t);
      } else {
         return new CompletionException(t);
      }
   }

   public static void rethrowExceptionIfPresent(Throwable t) {
      if (t != null) {
         throw asCompletionException(t);
      }
   }

   public static Throwable extractException(Throwable t) {
      Throwable cause = t.getCause();
      if (cause != null && t instanceof CompletionException) {
         return cause;
      }
      return t;
   }

   public static <T, R> Function<T, R> toNullFunction() {
      //noinspection unchecked
      return (Function<T, R>) TO_NULL;
   }

   public static <T> Function<T, Boolean> toTrueFunction() {
      //noinspection unchecked
      return (Function<T, Boolean>) TO_TRUE_FUNCTION;
   }

   public static <T> Function<T, T> identity() {
      //noinspection unchecked
      return (Function<T, T>) identity;
   }
}
