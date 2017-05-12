package org.infinispan.counter.api;

import java.util.concurrent.CompletableFuture;

/**
 * The strong consistent counter interface.
 * <p>
 * It provides atomic updates for the counter. All the operations are perform asynchronously and they complete the
 * {@link CompletableFuture} when completed.
 * <p>
 * The implementation may support weakly consistent reads via {@link #weakGetValue()}.
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
    * @param expect the expected value
    * @param update the new value
    * @return {@code true} if successful, {@code false} otherwise.
    */
   CompletableFuture<Boolean> compareAndSet(long expect, long update);

   /**
    * @return the {@link CounterConfiguration} used by this counter.
    */
   CounterConfiguration getConfiguration();
}
