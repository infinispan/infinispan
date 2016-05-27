package org.infinispan.util;

import java.time.Instant;

/**
 * TimeService that allows for wall clock time to be adjust manually.
 */
public class ControlledTimeService extends DefaultTimeService {
   protected long currentMillis;

   public ControlledTimeService(long currentMillis) {
      this.currentMillis = currentMillis;
   }

   @Override
   public long wallClockTime() {
      return currentMillis;
   }

   @Override
   public long time() {
      return currentMillis * 1000000;
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
