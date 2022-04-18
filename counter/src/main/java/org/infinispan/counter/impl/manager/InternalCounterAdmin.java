package org.infinispan.counter.impl.manager;

import static org.infinispan.counter.logging.Log.CONTAINER;

import java.util.concurrent.CompletionStage;

import org.infinispan.counter.api.StrongCounter;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.counter.exception.CounterException;

/**
 * Internal interface which abstract the {@link StrongCounter} and {@link WeakCounter}.
 *
 * @since 14.0
 */
public interface InternalCounterAdmin {

   /**
    * @return The {@link StrongCounter} instance or throws an {@link CounterException} if the counter is not a {@link
    * StrongCounter}.
    */
   default StrongCounter asStrongCounter() {
      throw CONTAINER.invalidCounterType(StrongCounter.class.getSimpleName(), getClass().getSimpleName());
   }

   /**
    * @return The {@link WeakCounter} instance or throws an {@link CounterException} if the counter is not a {@link
    * WeakCounter}.
    */
   default WeakCounter asWeakCounter() {
      throw CONTAINER.invalidCounterType(WeakCounter.class.getSimpleName(), getClass().getSimpleName());
   }

   /**
    * Destroys the counter.
    * <p>
    * It drops the counter's value and all listeners registered.
    *
    * @return A {@link CompletionStage} instance which is completed when the operation finishes.
    */
   CompletionStage<Void> destroy();

   /**
    * Resets the counter to its initial value.
    *
    * @return A {@link CompletionStage} instance which is completed when the operation finishes.
    */
   CompletionStage<Void> reset();

   /**
    * Returns the counter's value.
    *
    * @return A {@link CompletionStage} instance which is completed when the operation finishes.
    */
   CompletionStage<Long> value();

   /**
    * Checks if the counter is a {@link WeakCounter}.
    * <p>
    * If {@code true}, ensures {@link #asWeakCounter()} never throws an {@link CounterException}. Otherwise, it ensures
    * {@link #asStrongCounter()} never throws an {@link CounterException}.
    *
    * @return {@code true} if the counter is {@link WeakCounter}.
    */
   boolean isWeakCounter();

}
