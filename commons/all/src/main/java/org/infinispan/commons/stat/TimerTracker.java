package org.infinispan.commons.stat;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Tracks a timer metric.
 *
 * @author Pedro Ruivo
 * @since 13.0
 */
public interface TimerTracker {

   TimerTracker NO_OP = new TimerTracker() {
      @Override
      public void update(Duration duration) {
         //no-op
      }

      @Override
      public void update(long value, TimeUnit timeUnit) {
         //no-op
      }
   };

   /**
    * Adds a record.
    *
    * @param duration The duration value.
    */
   void update(Duration duration);

   /**
    * Adds a record.
    *
    * @param value    The value.
    * @param timeUnit The {@link TimeUnit} of the value.
    */
   void update(long value, TimeUnit timeUnit);

}
