package org.infinispan.counter.impl.listener;

import org.infinispan.counter.api.CounterEvent;
import org.infinispan.counter.impl.entries.CounterKey;
import org.infinispan.counter.impl.entries.CounterValue;

/**
 * A interface to generate {@link CounterEvent} from the current {@link CounterValue}.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
@FunctionalInterface
public interface CounterEventGenerator {

   /**
    * It generates the {@link CounterEvent}.
    * <p>
    * The {@code value} is the new value of {@link CounterEvent}.
    *
    * @param key   The counter's key.
    * @param value The counter's most recent {@link CounterValue}.
    * @return The {@link CounterEvent} with the updated value.
    */
   CounterEvent generate(CounterKey key, CounterValue value);

}
