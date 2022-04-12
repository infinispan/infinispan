package org.infinispan.counter.impl.factory;

import java.util.concurrent.CompletionStage;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.WeakCounter;

/**
 * Factory to create and remove {@link WeakCounter}.
 *
 * @since 14.0
 */
public interface WeakCounterFactory {

   /**
    * Removes the {@link WeakCounter} state.
    *
    * @param counterName The counter's name.
    * @return A {@link CompletionStage} that is completed after the counter is removed.
    */
   CompletionStage<Void> removeWeakCounter(String counterName, CounterConfiguration configuration);


   /**
    * Creates a {@link WeakCounter}.
    *
    * @param counterName   The counter's name.
    * @param configuration The counter's configuration.
    * @return A {@link CompletionStage} that is completed after the counter is created.
    */
   CompletionStage<WeakCounter> createWeakCounter(String counterName, CounterConfiguration configuration);
}
