package org.infinispan.commons.time;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * TimeService that allows for wall clock time to be adjust manually.
 */
public class ControlledTimeService extends DefaultTimeService {
   protected long currentMillis;

   public ControlledTimeService() {
      this(System.currentTimeMillis());
   }

   public ControlledTimeService(long currentMillis) {
      this.currentMillis = currentMillis;
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
