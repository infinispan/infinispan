package org.infinispan.util.concurrent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.infinispan.commons.IllegalLifecycleStateException;

/**
 * A mixture between a {@link CompletableFuture} and a {@link java.util.concurrent.locks.Condition}.
 *
 * @since 10.1
 * @author Dan Berindei
 */
public class ConditionFuture<T> {
   private final Map<Data, Predicate<T>> futures = Collections.synchronizedMap(new IdentityHashMap<>());
   private final ScheduledExecutorService timeoutExecutor;
   private volatile T lastValue;
   private volatile boolean running = true;

   public ConditionFuture(ScheduledExecutorService timeoutExecutor) {
      this.timeoutExecutor = timeoutExecutor;
   }

   public CompletionStage<Void> newConditionStage(Predicate<T> test, long timeout, TimeUnit timeUnit) {
      return newConditionStage(test, TimeoutException::new, timeout, timeUnit);
   }

   /**
    * Create a new {@link CompletionStage} that completes after the first {@link #update(Object)} call
    * with a value satisfying the {@code test} predicate.
    *
    * @param test The predicate.
    * @param exceptionGenerator Exception generator for timeout errors.
    * @param timeout Maximum time to wait for a value satisfying the predicate.
    * @param timeUnit Timeout time unit.
    */
   public CompletionStage<Void> newConditionStage(Predicate<T> test, Supplier<Exception> exceptionGenerator, long timeout, TimeUnit timeUnit) {
      Objects.requireNonNull(test);

      if (!running) {
         return CompletableFuture.failedFuture(new IllegalLifecycleStateException());
      }

      Data data = new Data();
      data.cancelFuture = timeoutExecutor.schedule(() -> {
         data.completeExceptionally(exceptionGenerator.get());
         return null;
      }, timeout, timeUnit);

      Predicate<?> previous = futures.putIfAbsent(data, test);
      if (previous != null) {
         data.cancelFuture.cancel(false);
         throw new IllegalStateException("Inserting the same Data instance");
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
      } catch (Throwable t) {
         completeAllExceptionally(t);
      }
   }

   private void completeAllExceptionally(Throwable t) {
      List<Data> completed;
      synchronized (futures) {
         completed = new ArrayList<>(futures.keySet());
         futures.clear();
      }
      for (Data data : completed) {
         data.cancelFuture.cancel(false);
         data.completeExceptionally(t);
      }
   }

   private void checkConditions(T value) {
      List<Data> completed;
      synchronized (futures) {
         completed = new ArrayList<>(futures.size());
         for (Iterator<Map.Entry<Data, Predicate<T>>> iterator = futures.entrySet().iterator(); iterator.hasNext(); ) {
            Map.Entry<Data, Predicate<T>> e = iterator.next();
            if (e.getValue().test(value)) {
               Data data = e.getKey();
               completed.add(data);
               iterator.remove();
            }
         }
      }
      for (Data data : completed) {
         data.cancelFuture.cancel(false);
         data.complete(null);
      }
   }

   public void stop() {
      running = false;
      lastValue = null;

      IllegalLifecycleStateException exception = new IllegalLifecycleStateException();
      completeAllExceptionally(exception);
   }

   private static class Data extends CompletableFuture<Void> {
      volatile Future<Void> cancelFuture;
   }
}
