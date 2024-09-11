package org.infinispan.commons.stat;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

import io.micrometer.core.instrument.util.TimeUtils;

/**
 * A simple implementation of {@link TimerTracker} that keep tracks of events and the sum of their duration.
 *
 * @since 15.1
 */
public class SimpleTimerTracker implements TimerTracker {
   private final LongAdder counter;
   private final DoubleAdder totalTime;

   public SimpleTimerTracker() {
      counter = new LongAdder();
      totalTime = new DoubleAdder();
   }

   @Override
   public void update(Duration duration) {
      update(duration.toNanos(), TimeUnit.NANOSECONDS);
   }

   @Override
   public void update(long value, TimeUnit timeUnit) {
      counter.increment();
      totalTime.add(TimeUtils.convert(value, timeUnit, TimeUnit.NANOSECONDS));
   }

   @Override
   public long count() {
      return counter.sum();
   }

   public double totalTime() {
      return totalTime.sum();
   }
}
