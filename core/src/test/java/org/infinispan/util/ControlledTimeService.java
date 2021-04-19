package org.infinispan.util;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * TimeService that allows for wall clock time to be adjust manually.
 */
public class ControlledTimeService extends EmbeddedTimeService {
   private static final Log log = LogFactory.getLog(ControlledTimeService.class);

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
      log.debugf("Current time is now %d", currentMillis);
   }
}
