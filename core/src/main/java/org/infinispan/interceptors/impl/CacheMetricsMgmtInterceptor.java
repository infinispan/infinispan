package org.infinispan.interceptors.impl;

import java.util.concurrent.TimeUnit;

import org.eclipse.microprofile.metrics.Timer;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.Units;

/**
 * Captures cache related statistics to be exposed as JMX attributes and/or microprofile metrics, depending on
 * configuration.
 *
 * @author anistor@redhat.com
 * @since 11.0.1
 */
@MBean(objectName = "Statistics", description = "General statistics such as timings, hit/miss ratio, etc.")
public final class CacheMetricsMgmtInterceptor extends CacheMgmtInterceptor {

   private Timer hitTimes;
   private Timer missTimes;
   private Timer storeTimes;
   private Timer removeTimes;

   @ManagedAttribute(description = "Hit Times", displayName = "Hit Times", dataType = DataType.TIMER, units = Units.NANOSECONDS)
   public void setHitTimes(Timer hitTimes) {
      this.hitTimes = hitTimes;
   }

   @ManagedAttribute(description = "Miss times", displayName = "Miss times", dataType = DataType.TIMER, units = Units.NANOSECONDS)
   public void setMissTimes(Timer missTimes) {
      this.missTimes = missTimes;
   }

   @ManagedAttribute(description = "Store Times", displayName = "Store Times", dataType = DataType.TIMER, units = Units.NANOSECONDS)
   public void setStoreTimes(Timer storeTimes) {
      this.storeTimes = storeTimes;
   }

   @ManagedAttribute(description = "Remove Times", displayName = "Remove Times", dataType = DataType.TIMER, units = Units.NANOSECONDS)
   public void setRemoveTimes(Timer removeTimes) {
      this.removeTimes = removeTimes;
   }

   @Override
   protected void accumulateHitStats(long intervalNanos, int count) {
      super.accumulateHitStats(intervalNanos, count);
      hitTimes.update(intervalNanos, TimeUnit.NANOSECONDS);
   }

   @Override
   protected void accumulateHitStats(long intervalNanos) {
      super.accumulateHitStats(intervalNanos);
      hitTimes.update(intervalNanos, TimeUnit.NANOSECONDS);
   }

   @Override
   protected void accumulateMissStats(long intervalNanos, int count) {
      super.accumulateMissStats(intervalNanos, count);
      missTimes.update(intervalNanos, TimeUnit.NANOSECONDS);
   }

   @Override
   protected void accumulateMissStats(long intervalNanos) {
      super.accumulateMissStats(intervalNanos);
      missTimes.update(intervalNanos, TimeUnit.NANOSECONDS);
   }

   @Override
   protected void accumulateStoreStats(long intervalNanos, int count) {
      super.accumulateStoreStats(intervalNanos, count);
      storeTimes.update(intervalNanos, TimeUnit.NANOSECONDS);
   }

   @Override
   protected void accumulateStoreStats(long intervalNanos) {
      super.accumulateStoreStats(intervalNanos);
      storeTimes.update(intervalNanos, TimeUnit.NANOSECONDS);
   }

   @Override
   protected void accumulateRemoveStats(long intervalNanos, int count) {
      super.accumulateRemoveStats(intervalNanos, count);
      removeTimes.update(intervalNanos, TimeUnit.NANOSECONDS);
   }

   @Override
   protected void accumulateRemoveStats(long intervalNanos) {
      super.accumulateRemoveStats(intervalNanos);
      removeTimes.update(intervalNanos, TimeUnit.NANOSECONDS);
   }
}
