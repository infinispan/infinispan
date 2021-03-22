package org.infinispan.api.mutiny;

import org.infinispan.api.common.events.ListenerHandle;
import org.infinispan.api.common.events.counter.CounterListener;
import org.infinispan.api.configuration.CounterConfiguration;

import io.smallrye.mutiny.Uni;

/**
 * The strong consistent counter interface.
 * <p>
 * It provides atomic updates for the counter. All the operations are perform asynchronously and they complete the
 * {@link Uni} when completed.
 *
 * @since 14.0
 */
public interface MutinyStrongCounter {

   /**
    * @return The counter name.
    */
   String name();

   /**
    * Return the container of this counter
    * @return
    */
   MutinyContainer container();


   /**
    * It fetches the current value.
    * <p>
    * It may go remotely to fetch the current value.
    *
    * @return The current value.
    */
   Uni<Long> value();

   /**
    * Atomically increments the counter and returns the new value.
    *
    * @return The new value.
    */
   default Uni<Long> incrementAndGet() {
      return addAndGet(1L);
   }


   /**
    * Atomically decrements the counter and returns the new value
    *
    * @return The new value.
    */
   default Uni<Long> decrementAndGet() {
      return addAndGet(-1L);
   }

   /**
    * Atomically adds the given value and return the new value.
    *
    * @param delta The non-zero value to add. It can be negative.
    * @return The new value.
    */
   Uni<Long> addAndGet(long delta);

   /**
    * Resets the counter to its initial value.
    */
   Uni<Void> reset();

   /**
    * Registers a {@link CounterListener} to this counter.
    *
    * @param listener The listener to register.
    * @return A {@link ListenerHandle} that allows to remove the listener via {@link ListenerHandle#remove()}.
    */
   Uni<ListenerHandle<CounterListener>> listen(CounterListener listener);

   /**
    * Atomically sets the value to the given updated value if the current value {@code ==} the expected value.
    * <p>
    * It is the same as {@code return compareAndSwap(expect, update).thenApply(value -> value == expect);}
    *
    * @param expect the expected value
    * @param update the new value
    * @return {@code true} if successful, {@code false} otherwise.
    */
   Uni<Boolean> compareAndSet(long expect, long update);

   /**
    * Atomically sets the value to the given updated value if the current value {@code ==} the expected value.
    * <p>
    * The operation is successful if the return value is equals to the expected value.
    *
    * @param expect the expected value.
    * @param update the new value.
    * @return the previous counter's value.
    */
   Uni<Long> compareAndSwap(long expect, long update);

   Uni<CounterConfiguration> getConfiguration();
}
