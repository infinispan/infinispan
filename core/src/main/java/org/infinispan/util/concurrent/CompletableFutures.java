package org.infinispan.util.concurrent;

import org.infinispan.commons.util.concurrent.NotifyingNotifiableFuture;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * Utility methods connecting {@link CompletableFuture} futures and our {@link NotifyingNotifiableFuture} futures.
 *
 * @author Dan Berindei
 * @since 8.0
 */
public class CompletableFutures {
   public static <T> void connect(NotifyingNotifiableFuture<T> sink, CompletableFuture<T> source) {
      CompletableFuture<T> compoundSource = source.whenComplete((value, throwable) -> {
         if (throwable == null) {
            sink.notifyDone(value);
         } else {
            sink.notifyException(throwable);
         }
      });
      sink.setFuture(compoundSource);
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
}
