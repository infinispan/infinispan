package org.infinispan.commons.stat.micrometer;

import org.infinispan.commons.stat.CounterTracker;

import io.micrometer.core.instrument.Counter;

/**
 * A {@link CounterTracker} implementation that stores the value in {@link Counter}.
 *
 * @since 15.1
 */
public record MicrometerCounterTracker(Counter counter) implements CounterTracker {

   @Override
   public void increment() {
      counter.increment();
   }

   @Override
   public void increment(double amount) {
      counter.increment(amount);
   }

}
