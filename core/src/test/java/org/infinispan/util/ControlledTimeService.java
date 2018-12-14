package org.infinispan.util;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * TimeService that allows for wall clock time to be adjust manually.
 */
public class ControlledTimeService extends EmbeddedTimeService {
   protected long currentMillis;

   public ControlledTimeService() {
      this.currentMillis = 1_000_000L;
   }

   @Override
   public long wallClockTime() {
      return currentMillis;
   }

   @Override
   public long time() {
      return TimeUnit.MILLISECONDS.toNanos(currentMillis);
   }

   @Override
   public Instant instant() {
      return Instant.ofEpochMilli(currentMillis);
   }

   public void advance(long time) {
      if (time <= 0) {
         throw new IllegalArgumentException("Argument must be greater than 0");
      }
      currentMillis += time;
   }
}
