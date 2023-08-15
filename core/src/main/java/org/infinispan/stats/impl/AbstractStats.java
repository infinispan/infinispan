package org.infinispan.stats.impl;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.commons.time.TimeService;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.JmxStatisticsExposer;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.util.logging.Log;

@MBean
@Scope(Scopes.NONE)
abstract class AbstractStats implements JmxStatisticsExposer {

   public static final long DEFAULT_STALE_STATS_THRESHOLD = 3000;

   protected volatile long staleStatsThreshold = DEFAULT_STALE_STATS_THRESHOLD;
   protected volatile long statsUpdateTimestamp = 0;

   @Inject
   TimeService timeService;
   volatile boolean statisticsEnabled = false;

   protected final Log log;

   final AtomicLong resetNanoseconds = new AtomicLong(0);
   final HashMap<String, Number> statsMap = new HashMap<>();

   AbstractStats(Log log) {
      this.log = log;
   }

   abstract void updateStats() throws Exception;

   @Start
   void start() {
      setStatisticsEnabled(statisticsEnabled);
   }

   public void reset() {
      statsMap.clear();
   }

   @Override
   @ManagedOperation(description = "Resets statistics gathered by this component", displayName = "Reset statistics")
   public void resetStatistics() {
      if (getStatisticsEnabled()) {
         reset();
         resetNanoseconds.set(timeService.time());
      }
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

   public void setStaleStatsThreshold(long staleStatsThreshold) {
      this.staleStatsThreshold = staleStatsThreshold;
   }

   long addLongAttributes(Collection<Map<String, Number>> responseList, String attribute) {
      long total = 0;
      for (Map<String, Number> m : responseList) {
         Number value = m.get(attribute);
         long longValue = value.longValue();
         if (longValue >= 0) {
            total += longValue;
         } else {
            total = -1;
         }
      }
      return total;
   }

   private int addIntAttributes(Collection<Map<String, Number>> responseList, String attribute) {
      int total = 0;
      for (Map<String, Number> m : responseList) {
         Number value = m.get(attribute);
         int intValue = value.intValue();
         if (intValue >= 0) {
            total += intValue;
         } else {
            total = -1;
         }
      }
      return total;
   }

   private int maxIntAttributes(Collection<Map<String, Number>> responseList, String attribute) {
      int max = -1;
      for (Map<String, Number> m : responseList) {
         Number value = m.get(attribute);
         int intValue = value.intValue();
         max = Math.max(max, intValue);
      }
      return max;
   }

   void putLongAttributesAverage(Collection<Map<String, Number>> responseList, String attribute) {
      long numValues = 0;
      long total = 0;
      for (Map<String, Number> m : responseList) {
         Number value = m.get(attribute);
         long longValue = value.longValue();
         if (longValue >= 0) {
            total += longValue;
            numValues++;
         }
      }
      if (numValues > 0) {
         long average = total / numValues;
         statsMap.put(attribute, average);
      }
   }

   void putLongAttributes(Collection<Map<String, Number>> responseList, String attribute) {
      statsMap.put(attribute, addLongAttributes(responseList, attribute));
   }

   void putIntAttributes(Collection<Map<String, Number>> responseList, String attribute) {
      statsMap.put(attribute, addIntAttributes(responseList, attribute));
   }

   void putIntAttributesMax(Collection<Map<String, Number>> responseList, String attribute) {
      statsMap.put(attribute, maxIntAttributes(responseList, attribute));
   }

   long getStatAsLong(String attribute) {
      return getStat(attribute).longValue();
   }

   int getStatAsInt(String attribute) {
      return getStat(attribute).intValue();
   }

   synchronized void fetchStatsIfNeeded() {
      long duration = timeService.timeDuration(statsUpdateTimestamp, timeService.time(), TimeUnit.MILLISECONDS);
      if (duration > staleStatsThreshold) {
         try {
            updateStats();
         } catch (Exception e) {
            log.error("Could not execute cluster wide cache stats operation ", e);
         } finally {
            statsUpdateTimestamp = timeService.time();
         }
      }
   }

   protected Number getStat(String attribute) {
      if (getStatisticsEnabled()) {
         fetchStatsIfNeeded();
         return statsMap.getOrDefault(attribute, 0);
      } else {
         return -1;
      }
   }
}
