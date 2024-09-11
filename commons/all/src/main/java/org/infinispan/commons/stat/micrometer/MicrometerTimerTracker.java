package org.infinispan.commons.stat.micrometer;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.stat.TimerTracker;

import io.micrometer.core.instrument.Timer;

/**
 * A {@link TimerTracker} implementation that stores the value in {@link Timer}.
 *
 * @since 15.1
 */
public record MicrometerTimerTracker(Timer timer) implements TimerTracker {

   @Override
   public void update(Duration duration) {
      timer.record(duration);
   }

   @Override
   public void update(long value, TimeUnit timeUnit) {
      timer.record(value, timeUnit);
   }

   @Override
   public long count() {
      return timer.count();
   }
}
