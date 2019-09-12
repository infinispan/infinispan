package org.infinispan.util.concurrent;

import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.infinispan.commons.IllegalLifecycleStateException;

/**
 * A mixture between a {@link CompletableFuture} and a {@link java.util.concurrent.locks.Condition}.
 *
 * @since 10.1
 * @author Dan Berindei
 */
public class ConditionFuture<T> {
   private final Map<Predicate<T>, Data> futures = new IdentityHashMap<>();
   private final ScheduledExecutorService timeoutExecutor;
   private volatile T lastValue;
   private volatile boolean running = true;

   public ConditionFuture(ScheduledExecutorService timeoutExecutor) {
      this.timeoutExecutor = timeoutExecutor;
   }

   /**
    * Create a new {@link CompletionStage} that completes after the first {@link #update(Object)} call
    * with a value satisfying the {@code test} predicate.
    *
    * @param test The predicate.
    * @param timeout Maximum time to wait for a value satisfying the predicate.
    * @param timeUnit Timeout time unit.
    */
   public CompletionStage<Void> newConditionStage(Predicate<T> test, long timeout, TimeUnit timeUnit) {
      Objects.requireNonNull(test);

      if (!running) {
         return CompletableFutures.completedExceptionFuture(new IllegalLifecycleStateException());
      }

      Data data = new Data();
      data.cancelFuture = timeoutExecutor.schedule(() -> {
         data.completeExceptionally(new TimeoutException());
         return null;
      }, timeout, timeUnit);

      CompletableFuture<Void> previous = futures.putIfAbsent(test, data);
      if (previous != null) {
         data.cancelFuture.cancel(false);
         futures.remove(test);
         throw new IllegalStateException();
      }

      if (!running) {
         data.cancelFuture.cancel(false);
         futures.remove(test);
         data.completeExceptionally(new IllegalLifecycleStateException());
      }

      T localValue = lastValue;
      if (localValue != null && test.test(localValue)) {
         data.cancelFuture.cancel(false);
         futures.remove(test);
         data.complete(null);
      }

      return data;
   }

   /**
    * Update the value and complete any outstanding condition stages for which the value satisfies the predicate.
    */
   public void update(T value) {
      if (!running)
         throw new IllegalLifecycleStateException();

      lastValue = Objects.requireNonNull(value);
      checkConditions(value);
   }

   /**
    * Update the value and complete any outstanding condition stages for which the value satisfies the predicate.
    */
   public void updateAsync(T value, Executor executor) {
      if (!running)
         throw new IllegalLifecycleStateException();

      lastValue = Objects.requireNonNull(value);

      try {
         executor.execute(() -> checkConditions(value));
      } catch (Exception e) {
         for (Data data : futures.values()) {
            data.cancelFuture.cancel(false);
            data.completeExceptionally(e);
         }
      }
   }

   private void checkConditions(T value) {
      for (Iterator<Map.Entry<Predicate<T>, Data>> iterator = futures.entrySet().iterator(); iterator.hasNext(); ) {
         Map.Entry<Predicate<T>, Data> e = iterator.next();
         if (e.getKey().test(value)) {
            Data data = e.getValue();
            data.cancelFuture.cancel(false);
            data.complete(null);
            iterator.remove();
         }
      }
   }

   public void stop() {
      running = false;
      lastValue = null;
      IllegalLifecycleStateException exception = new IllegalLifecycleStateException();
      for (Data data : futures.values()) {
         data.cancelFuture.cancel(false);
         data.completeExceptionally(exception);
      }
      futures.clear();
   }

   private static class Data extends CompletableFuture<Void> {
      volatile Future<Void> cancelFuture;
   }
}
