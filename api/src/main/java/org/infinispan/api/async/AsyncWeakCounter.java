package org.infinispan.api.async;

import java.util.concurrent.CompletionStage;

import org.infinispan.api.configuration.CounterConfiguration;

/**
 * @since 14.0
 **/
public interface AsyncWeakCounter {
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
    * Retrieves this counter's value.
    *
    * @return
    */
   CompletionStage<Long> value();

   /**
    * Increments the counter.
    */
   default CompletionStage<Void> increment() {
      return add(1L);
   }

   /**
    * Decrements the counter.
    */
   default CompletionStage<Void> decrement() {
      return add(-1L);
   }

   /**
    * Adds the given value to the new value.
    *
    * @param delta the value to add.
    */
   CompletionStage<Void> add(long delta);

   /**
    * Resets the counter to its initial value.
    */
   CompletionStage<Void> reset();
}
