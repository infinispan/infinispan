package org.infinispan.commons.stat;

import java.util.concurrent.TimeUnit;

/**
 * A {@link SimpleStat} implementation that also updates a {@link TimerTracker} object.
 *
 * @author Pedro Ruivo
 * @since 13.0
 */
public class SimpleStateWithTimer extends DefaultSimpleStat {

   private volatile TimerTracker timerTracker;

   @Override
   public void record(long value) {
      super.record(value);
      TimerTracker local = this.timerTracker;
      if (local != null) {
         local.update(value, TimeUnit.NANOSECONDS);
      }
   }

   @Override
   public void setTimer(TimerTracker timer) {
      this.timerTracker = timer;
   }
}
