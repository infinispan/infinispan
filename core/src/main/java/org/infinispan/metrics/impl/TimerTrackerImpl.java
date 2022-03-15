package org.infinispan.metrics.impl;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.stat.TimerTracker;

import io.micrometer.core.instrument.Timer;

/**
 * A {@link TimerTracker} implementation that updates a {@link Timer} instance.
 *
 * @author Pedro Ruivo
 * @author Fabio Massimo Ercoli
 * @since 13.0
 */
public class TimerTrackerImpl implements TimerTracker {

   private final Timer timer;

   TimerTrackerImpl(Timer timer) {
      this.timer = Objects.requireNonNull(timer, "Timer cannot be null.");
   }

   @Override
   public void update(Duration duration) {
      timer.record(duration);
   }

   @Override
   public void update(long value, TimeUnit timeUnit) {
      timer.record(Duration.ofNanos(timeUnit.toNanos(value)));
   }
}
