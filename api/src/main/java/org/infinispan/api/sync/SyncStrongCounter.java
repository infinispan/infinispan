package org.infinispan.api.sync;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.infinispan.api.common.events.counter.CounterEvent;
import org.infinispan.api.configuration.CounterConfiguration;

/**
 * @since 14.0
 */
public interface SyncStrongCounter {
   String name();

   /**
    * Return the container of this counter
    *
    * @return
    */
   SyncContainer container();


   /**
    * It fetches the current value.
    * <p>
    * It may go remotely to fetch the current value.
    *
    * @return The current value.
    */
   long value();

   /**
    * Atomically increments the counter and returns the new value.
    *
    * @return The new value.
    */
   default long incrementAndGet() {
      return addAndGet(1L);
   }


   /**
    * Atomically decrements the counter and returns the new value
    *
    * @return The new value.
    */
   default long decrementAndGet() {
      return addAndGet(-1L);
   }


   /**
    * Atomically adds the given value and return the new value.
    *
    * @param delta The non-zero value to add. It can be negative.
    * @return The new value.
    */
   long addAndGet(long delta);

   /**
    * Resets the counter to its initial value.
    */
   CompletableFuture<Void> reset();

   /**
    * Registers a {@link Consumer<CounterEvent>} to this counter.
    *
    * @param listener The listener to register.
    * @return A {@link AutoCloseable} that allows to remove the listener via {@link AutoCloseable#close()}.
    */
   AutoCloseable listen(Consumer<CounterEvent> listener);

   /**
    * Atomically sets the value to the given updated value if the current value {@code ==} the expected value.
    * <p>
    * It is the same as {@code return compareAndSwap(expect, update).thenApply(value -> value == expect);}
    *
    * @param expect the expected value
    * @param update the new value
    * @return {@code true} if successful, {@code false} otherwise.
    */
   default boolean compareAndSet(long expect, long update) {
      return compareAndSwap(expect, update) == expect;
   }

   /**
    * Atomically sets the value to the given updated value if the current value {@code ==} the expected value.
    * <p>
    * The operation is successful if the return value is equals to the expected value.
    *
    * @param expect the expected value.
    * @param update the new value.
    * @return the previous counter's value.
    */
   long compareAndSwap(long expect, long update);

   /**
    * @return the {@link CounterConfiguration} used by this counter.
    */
   CounterConfiguration configuration();
}
