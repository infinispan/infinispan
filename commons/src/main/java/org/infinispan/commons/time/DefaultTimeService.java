package org.infinispan.commons.time;

import java.time.Clock;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * The default implementation of {@link TimeService}. It does not perform any optimization and relies on {@link
 * System#currentTimeMillis()} and {@link System#nanoTime()}.
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class DefaultTimeService implements TimeService {

   public static final DefaultTimeService INSTANCE = new DefaultTimeService();

   private final Clock clock;

   public DefaultTimeService() {
      this.clock = Clock.systemUTC();
   }

   @Override
   public long wallClockTime() {
      return System.currentTimeMillis();
   }

   @Override
   public long time() {
      return System.nanoTime();
   }

   @Override
   public Instant instant() {
      return clock.instant();
   }

   @Override
   public long timeDuration(long startTimeNanos, TimeUnit outputTimeUnit) {
      return timeDuration(startTimeNanos, time(), outputTimeUnit);
   }

   @Override
   public long timeDuration(long startTimeNanos, long endTimeNanos, TimeUnit outputTimeUnit) {
      long remaining = endTimeNanos - startTimeNanos;
      if (remaining <= 0) {
         return 0;
      }
      return outputTimeUnit.convert(remaining, TimeUnit.NANOSECONDS);
   }

   @Override
   public boolean isTimeExpired(long endTimeNanos) {
      return time() - endTimeNanos >= 0;
   }

   @Override
   public long remainingTime(long endTimeNanos, TimeUnit outputTimeUnit) {
      long remaining = endTimeNanos - time();
      return remaining <= 0 ? 0 : outputTimeUnit.convert(remaining, TimeUnit.NANOSECONDS);
   }

   @Override
   public long expectedEndTime(long duration, TimeUnit inputTimeUnit) {
      if (duration <= 0) {
         return time();
      }
      return time() + inputTimeUnit.toNanos(duration);
   }
}
