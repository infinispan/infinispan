package org.infinispan.counter.api;

import java.util.concurrent.CompletableFuture;

/**
 * The strong consistent counter interface.
 * <p>
 * It provides atomic updates for the counter. All the operations are perform asynchronously and they complete the
 * {@link CompletableFuture} when completed.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public interface StrongCounter {

   /**
    * @return The counter name.
    */
   String getName();


   /**
    * It fetches the current value.
    * <p>
    * It may go remotely to fetch the current value.
    *
    * @return The current value.
    */
   CompletableFuture<Long> getValue();

   /**
    * Atomically increments the counter and returns the new value.
    *
    * @return The new value.
    */
   default CompletableFuture<Long> incrementAndGet() {
      return addAndGet(1L);
   }


   /**
    * Atomically decrements the counter and returns the new value
    *
    * @return The new value.
    */
   default CompletableFuture<Long> decrementAndGet() {
      return addAndGet(-1L);
   }


   /**
    * Atomically adds the given value and return the new value.
    *
    * @param delta The non-zero value to add. It can be negative.
    * @return The new value.
    */
   CompletableFuture<Long> addAndGet(long delta);

   /**
    * Resets the counter to its initial value.
    */
   CompletableFuture<Void> reset();

   /**
    * Registers a {@link CounterListener} to this counter.
    *
    * @param listener The listener to register.
    * @param <T>      The concrete type of the listener. It must implement {@link CounterListener}.
    * @return A {@link Handle} that allows to remove the listener via {@link Handle#remove()}.
    */
   <T extends CounterListener> Handle<T> addListener(T listener);

   /**
    * Atomically sets the value to the given updated value if the current value {@code ==} the expected value.
    *
    * It is the same as {@code return compareAndSwap(expect, update).thenApply(value -> value == expect);}
    *
    * @param expect the expected value
    * @param update the new value
    * @return {@code true} if successful, {@code false} otherwise.
    */
   default CompletableFuture<Boolean> compareAndSet(long expect, long update) {
      return compareAndSwap(expect, update).thenApply(value -> value == expect);
   }

   /**
    * Atomically sets the value to the given updated value if the current value {@code ==} the expected value.
    *
    * The operation is successful if the return value is equals to the expected value.
    *
    * @param expect the expected value.
    * @param update the new value.
    * @return the previous counter's value.
    */
   CompletableFuture<Long> compareAndSwap(long expect, long update);

   /**
    * @return the {@link CounterConfiguration} used by this counter.
    */
   CounterConfiguration getConfiguration();

   /**
    * It removes this counter from the cluster.
    * <p>
    * Note that it doesn't remove the counter from the {@link CounterManager}. If you want to remove the counter from
    * the {@link CounterManager} use {@link CounterManager#remove(String)}.
    *
    * @return The {@link CompletableFuture} that is completed when the counter is removed from the cluster.
    */
   CompletableFuture<Void> remove();

   /**
    * It returns a synchronous strong counter for this instance.
    *
    * @return a {@link SyncStrongCounter}.
    */
   SyncStrongCounter sync();
}
