package org.infinispan.counter.impl.factory;

import java.util.concurrent.CompletionStage;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.StrongCounter;

/**
 * Factory to create and remove bounded and unbounded {@link StrongCounter}.
 *
 * @since 14.0
 */
public interface StrongCounterFactory {

   /**
    * Removes the {@link StrongCounter} state.
    *
    * @param counterName The counter's name.
    * @return A {@link CompletionStage} that is completed after the counter is removed.
    */
   CompletionStage<Void> removeStrongCounter(String counterName);

   /**
    * Creates a (un)bounded {@link StrongCounter}.
    *
    * @param counterName   The counter's name.
    * @param configuration The counter's configuration.
    * @return A {@link CompletionStage} that is completed after the counter is created.
    */
   CompletionStage<StrongCounter> createStrongCounter(String counterName, CounterConfiguration configuration);

}
