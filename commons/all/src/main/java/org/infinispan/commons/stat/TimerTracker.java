package org.infinispan.commons.stat;

import java.util.concurrent.TimeUnit;

/**
 * Tracks a timer metric.
 *
 * @author Pedro Ruivo
 * @since 13.0
 */
public interface TimerTracker {

   /**
    * Adds a record.
    *
    * @param value    The value.
    * @param timeUnit The {@link TimeUnit} of the value.
    */
   void update(long value, TimeUnit timeUnit);

}
