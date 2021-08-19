package org.infinispan.commons.time;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

/**
 * TimeService that allows for wall clock time to be adjust manually.
 */
public class ControlledTimeService extends DefaultTimeService {
   private static final Log log = LogFactory.getLog(ControlledTimeService.class);

   private final Object id;
   protected volatile long currentMillis;

   public ControlledTimeService() {
      this(null);
   }

   public ControlledTimeService(Object id) {
      this(id, 1_000_000L);
   }

   public ControlledTimeService(Object id, ControlledTimeService other) {
      this(id, other.currentMillis);
   }

   private ControlledTimeService(Object id, long currentMillis) {
      this.id = id;
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

   public void advance(long delta, TimeUnit timeUnit) {
      advance(timeUnit.toMillis(delta));
   }

   public synchronized void advance(long deltaMillis) {
      if (deltaMillis <= 0) {
         throw new IllegalArgumentException("Argument must be greater than 0");
      }
      currentMillis += deltaMillis;
      log.tracef("Current time for %s is now %d", id, currentMillis);
   }
}
