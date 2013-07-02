package org.infinispan.util;

import java.util.concurrent.TimeUnit;

/**
 * The default implementation of {@link TimeService}. It does not perform any optimization and relies on {@link
 * System#currentTimeMillis()} and {@link System#nanoTime()}.
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class DefaultTimeService implements TimeService {

   @Override
   public long wallClockTime() {
      return System.currentTimeMillis();
   }

   @Override
   public long time() {
      return System.nanoTime();
   }

   @Override
   public long timeDuration(long startTime, TimeUnit outputTimeUnit) {
      return timeDuration(startTime, time(), outputTimeUnit);
   }

   @Override
   public long timeDuration(long startTime, long endTime, TimeUnit outputTimeUnit) {
      if (startTime < 0 || endTime < 0 || startTime >= endTime) {
         return 0;
      }
      return outputTimeUnit.convert(endTime - startTime, TimeUnit.NANOSECONDS);
   }

   @Override
   public boolean isTimeExpired(long endTime) {
      return time() >= endTime;
   }

   @Override
   public long remainingTime(long endTime, TimeUnit outputTimeUnit) {
      if (endTime <= 0) {
         return 0;
      }
      long now = time();
      return now > endTime ? 0 : outputTimeUnit.convert(endTime - now, TimeUnit.NANOSECONDS);
   }

   @Override
   public long expectedEndTime(long duration, TimeUnit inputTimeUnit) {
      if (duration <= 0) {
         return time();
      }
      return time() + inputTimeUnit.toNanos(duration);
   }
}
