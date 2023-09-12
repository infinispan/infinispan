package org.infinispan.metrics.impl;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

import org.infinispan.commons.stat.TimerTracker;

import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.util.TimeUtils;

/**
 * A {@link TimerTracker} implementation that uses {@link FunctionTimer} instead of {@link Timer}.
 *
 * @see #create(String)
 */
public class FunctionTimerTrackerImpl implements TimerTracker {

   private final LongAdder counter;
   private final DoubleAdder totalTime;
   private final TimeUnit totalTimeTimeUnit;

   FunctionTimerTrackerImpl(TimeUnit totalTimeTimeUnit) {
      this.totalTimeTimeUnit = Objects.requireNonNull(totalTimeTimeUnit);
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
      totalTime.add(TimeUtils.convert(value, timeUnit, totalTimeTimeUnit));
   }

   private long count() {
      return counter.sum();
   }

   private double totalTime() {
      return totalTime.sum();
   }

   /**
    * Creates a {@link FunctionTimer.Builder} configured to collect the data from this instance.
    *
    * @param metricName The metric name.
    * @return The builder instance.
    */
   public FunctionTimer.Builder<FunctionTimerTrackerImpl> create(String metricName) {
      return FunctionTimer.builder(metricName, this, FunctionTimerTrackerImpl::count, FunctionTimerTrackerImpl::totalTime, totalTimeTimeUnit);
   }
}
