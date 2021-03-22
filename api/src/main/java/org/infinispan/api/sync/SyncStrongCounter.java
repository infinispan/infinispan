package org.infinispan.api.sync;

import java.util.concurrent.CompletableFuture;

import org.infinispan.api.common.events.ListenerHandle;
import org.infinispan.api.common.events.counter.CounterListener;
import org.infinispan.api.configuration.CounterConfiguration;

/**
 * @since 14.0
 */
public interface SyncStrongCounter {
   String name();

   /**
    * Return the container of this counter
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
   CompletableFuture<Long> value();

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
    * @return A {@link ListenerHandle} that allows to remove the listener via {@link ListenerHandle#remove()}.
    */
   ListenerHandle<CounterListener> listen(CounterListener listener);

   /**
    * Atomically sets the value to the given updated value if the current value {@code ==} the expected value.
    * <p>
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
    * <p>
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
   CounterConfiguration configuration();
}
