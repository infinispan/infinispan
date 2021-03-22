package org.infinispan.api.async;

import java.util.concurrent.CompletionStage;

import org.infinispan.api.common.events.ListenerHandle;
import org.infinispan.api.common.events.counter.CounterListener;
import org.infinispan.api.configuration.CounterConfiguration;

/**
 * The strong consistent counter interface.
 * <p>
 * It provides atomic updates for the counter. All the operations are perform asynchronously and they complete the
 * {@link CompletionStage} when completed.
 *
 * @since 14.0
 */
public interface AsyncStrongCounter {
   /**
    * @return The counter name.
    */
   String name();

   /**
    * Retrieves the counter's configuration.
    *
    * @return this counter's configuration.
    */
   CompletionStage<CounterConfiguration> configuration();

   /**
    * Return the container of this counter
    * @return
    */
   AsyncContainer container();

   /**
    * It fetches the current value.
    * <p>
    * It may go remotely to fetch the current value.
    *
    * @return The current value.
    */
   CompletionStage<Long> value();

   /**
    * Atomically increments the counter and returns the new value.
    *
    * @return The new value.
    */
   default CompletionStage<Long> incrementAndGet() {
      return addAndGet(1L);
   }


   /**
    * Atomically decrements the counter and returns the new value
    *
    * @return The new value.
    */
   default CompletionStage<Long> decrementAndGet() {
      return addAndGet(-1L);
   }


   /**
    * Atomically adds the given value and return the new value.
    *
    * @param delta The non-zero value to add. It can be negative.
    * @return The new value.
    */
   CompletionStage<Long> addAndGet(long delta);

   /**
    * Resets the counter to its initial value.
    */
   CompletionStage<Void> reset();

   /**
    * Registers a {@link CounterListener} to this counter.
    *
    * @param listener The listener to register.
    * @return A {@link ListenerHandle} that allows to remove the listener via {@link ListenerHandle#remove()}.
    */
   CompletionStage<AutoCloseable> listen(CounterListener listener);

   /**
    * Atomically sets the value to the given updated value if the current value {@code ==} the expected value.
    * <p>
    * It is the same as {@code return compareAndSwap(expect, update).thenApply(value -> value == expect);}
    *
    * @param expect the expected value
    * @param update the new value
    * @return {@code true} if successful, {@code false} otherwise.
    */
   default CompletionStage<Boolean> compareAndSet(long expect, long update) {
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
   CompletionStage<Long> compareAndSwap(long expect, long update);
}
