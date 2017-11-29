package org.infinispan.stats.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.commons.CacheException;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.jmx.JmxStatisticsExposer;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.DisplayType;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.Units;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
public abstract class AbstractClusterStats implements JmxStatisticsExposer {

   public static final long DEFAULT_STALE_STATS_THRESHOLD = 3000;

   private TimeService timeService;
   private volatile long staleStatsThreshold = DEFAULT_STALE_STATS_THRESHOLD;
   private volatile long statsUpdateTimestamp = 0;
   volatile boolean statisticsEnabled = false;

   private final Log log;
   private final AtomicLong resetNanoseconds = new AtomicLong(0);
   final HashMap<String, Number> statsMap = new HashMap<>();

   AbstractClusterStats(Log log) {
      this.log = log;
   }

   abstract void updateStats() throws Exception;

   @Inject
   void init(TimeService timeService) {
      this.timeService = timeService;
   }

   @Start
   void start() {
      setStatisticsEnabled(statisticsEnabled);
   }

   public void reset() {
      statsMap.clear();
   }

   @ManagedAttribute(description = "Gets the threshold for cluster wide stats refresh (milliseconds)",
         displayName = "Stale Stats Threshold",
         dataType = DataType.TRAIT,
         writable = true)
   public long getStaleStatsThreshold() {
      return staleStatsThreshold;
   }

   public void setStaleStatsThreshold(long staleStatsThreshold) {
      this.staleStatsThreshold = staleStatsThreshold;
   }

   @Override
   @ManagedOperation(description = "Resets statistics gathered by this component", displayName = "Reset statistics")
   public void resetStatistics() {
      if (isStatisticsEnabled()) {
         reset();
         resetNanoseconds.set(timeService.time());
      }
   }

   @ManagedAttribute(
         description = "Number of seconds since the cluster-wide statistics were last reset",
         displayName = "Seconds since cluster-wide statistics were reset",
         units = Units.SECONDS,
         displayType = DisplayType.SUMMARY
   )
   public long getTimeSinceReset() {
      long result = -1;
      if (isStatisticsEnabled()) {
         result = timeService.timeDuration(resetNanoseconds.get(), TimeUnit.SECONDS);
      }
      return result;
   }

   @Override
   public void setStatisticsEnabled(boolean enabled) {
      this.statisticsEnabled = enabled;
      if (enabled) {
         //yes technically we do not reset stats but we initialize them
         resetNanoseconds.set(timeService.time());
      }
   }

   @Override
   public boolean getStatisticsEnabled() {
      return statisticsEnabled;
   }

   @ManagedAttribute(description = "Enables or disables the gathering of statistics by this component",
         displayName = "Statistics enabled",
         dataType = DataType.TRAIT,
         writable = true)
   public boolean isStatisticsEnabled() {
      return getStatisticsEnabled();
   }

   synchronized void fetchClusterWideStatsIfNeeded() {
      long duration = timeService.timeDuration(statsUpdateTimestamp, timeService.time(), TimeUnit.MILLISECONDS);
      if (duration > DEFAULT_STALE_STATS_THRESHOLD) {
         try {
            updateStats();
         } catch (Exception e) {
            log.error("Could not execute cluster wide cache stats operation ", e);
            throw new CacheException("Could not execute cluster wide cache stats operation", e);
         } finally {
            statsUpdateTimestamp = timeService.time();
         }
      }
   }

   long addLongAttributes(List<Map<String, Number>> responseList, String attribute) {
      long total = 0;
      for (Map<String, Number> m : responseList) {
         Number value = m.get(attribute);
         long longValue = value.longValue();
         if (longValue > -1) {
            total += longValue;
         }
      }
      return total;
   }

   double addDoubleAttributes(List<Map<String, Number>> responseList, String attribute) {
      double total = 0;
      for (Map<String, Number> m : responseList) {
         Number value = m.get(attribute);
         double doubleValue = value.doubleValue();
         if (doubleValue > -1) {
            total += doubleValue;
         }
      }
      return total;
   }

   int addIntAttributes(List<Map<String, Number>> responseList, String attribute) {
      int total = 0;
      for (Map<String, Number> m : responseList) {
         Number value = m.get(attribute);
         int intValue = value.intValue();
         if (intValue > -1) {
            total += intValue;
         }
      }
      return total;
   }

   int maxIntAttributes(List<Map<String, Number>> responseList, String attribute) {
      int max = 0;
      for (Map<String, Number> m : responseList) {
         Number value = m.get(attribute);
         int intValue = value.intValue();
         max = Math.max(max, intValue);
      }
      return max;
   }

   void putLongAttributesAverage(List<Map<String, Number>> responseList, String attribute) {
      long nonZeroValues = 0;
      long total = 0;
      for (Map<String, Number> m : responseList) {
         Number value = m.get(attribute);
         long longValue = value.longValue();
         if (longValue > 0) {
            total += longValue;
            nonZeroValues++;
         }
      }
      long average = nonZeroValues > 0 ? total / nonZeroValues : 0;
      statsMap.put(attribute, average);
   }

   void putLongAttributes(List<Map<String, Number>> responseList, String attribute) {
      statsMap.put(attribute, addLongAttributes(responseList, attribute));
   }

   void putIntAttributes(List<Map<String, Number>> responseList, String attribute) {
      statsMap.put(attribute, addIntAttributes(responseList, attribute));
   }

   void putIntAttributesMax(List<Map<String, Number>> responseList, String attribute) {
      statsMap.put(attribute, maxIntAttributes(responseList, attribute));
   }

   long getStatAsLong(String attribute) {
      return getStat(attribute).longValue();
   }

   int getStatAsInt(String attribute) {
      return getStat(attribute).intValue();
   }

   private Number getStat(String attribute) {
      if (isStatisticsEnabled()) {
         fetchClusterWideStatsIfNeeded();
         return statsMap.getOrDefault(attribute, 0);
      } else {
         return -1;
      }
   }
}
