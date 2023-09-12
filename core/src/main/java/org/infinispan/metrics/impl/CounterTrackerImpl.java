package org.infinispan.metrics.impl;

import java.util.Objects;

import org.infinispan.commons.stat.CounterTracker;

import io.micrometer.core.instrument.Counter;

/**
 * The {@link CounterTracker} that uses {@link Counter}.
 */
public class CounterTrackerImpl implements CounterTracker {

   private final Counter counter;

   CounterTrackerImpl(Counter counter) {
      this.counter = Objects.requireNonNull(counter);
   }

   @Override
   public void increment() {
      counter.increment();
   }

   @Override
   public void increment(double amount) {
      counter.increment(amount);
   }
}
