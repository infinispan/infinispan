package org.infinispan.stats.impl;

import java.util.concurrent.TimeUnit;

import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.Units;
import org.infinispan.util.logging.Log;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
public abstract class AbstractClusterStats extends AbstractStats {

   AbstractClusterStats(Log log) {
      super(log);
   }

   abstract void updateStats() throws Exception;

   @ManagedAttribute(description = "Gets the threshold for cluster wide stats refresh (milliseconds)",
         displayName = "Stale Stats Threshold",
         dataType = DataType.TRAIT,
         writable = true,
         clusterWide = true)
   public long getStaleStatsThreshold() {
      return staleStatsThreshold;
   }

   @ManagedAttribute(
         description = "Number of seconds since the cluster-wide statistics were last reset",
         displayName = "Seconds since cluster-wide statistics were reset",
         units = Units.SECONDS,
         clusterWide = true
   )
   public long getTimeSinceReset() {
      long result = -1;
      if (isStatisticsEnabled()) {
         result = timeService.timeDuration(resetNanoseconds.get(), TimeUnit.SECONDS);
      }
      return result;
   }

   @ManagedAttribute(description = "Enables or disables the gathering of statistics by this component",
         displayName = "Statistics enabled",
         dataType = DataType.TRAIT,
         writable = true,
         clusterWide = true)
   public boolean isStatisticsEnabled() {
      return getStatisticsEnabled();
   }
}
