package org.infinispan.util;

/**
 * TimeService that allows for wall clock time to be adjust manually.
 */
public class ControlledTimeService extends DefaultTimeService {
   private long currentMillis;

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

   public void advance(long time) {
      currentMillis += time;
   }
}
