package org.infinispan.stats.impl;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.jmx.JmxStatisticsExposer;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.DisplayType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.jmx.annotations.Units;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.stats.CacheContainerStats;
import org.infinispan.stats.Stats;
import org.infinispan.util.TimeService;


/**
 * Cache container statistics needed for admin console
 *
 * @author Vladimir Blagojevic
 * @since 7.1
 *
 */
@MBean(objectName = CacheContainerStats.OBJECT_NAME, description = "General cache container statistics such as timings, hit/miss ratio, etc.")
public class CacheContainerStatsImpl implements CacheContainerStats, JmxStatisticsExposer {

   private EmbeddedCacheManager cm;
   private final AtomicLong resetNanoseconds = new AtomicLong(0);
   private boolean statisticsEnabled = false;
   private TimeService timeService;

   public CacheContainerStatsImpl(EmbeddedCacheManager cm) {
      this.cm = cm;
      cm.getGlobalComponentRegistry().registerComponent(this, CacheContainerStats.class);
      boolean globalJmxStatsEnabled = cm.getCacheManagerConfiguration().globalJmxStatistics().enabled();
      setStatisticsEnabled(globalJmxStatsEnabled);
   }

   @Inject
   public void setDependencies(TimeService timeService) {
      this.timeService = timeService;
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

   @Override
   public void resetStatistics() {
      if (getStatisticsEnabled()) {
         for (String cn : cm.getCacheNames()) {
            if (cm.cacheExists(cn)) {
               cm.getCache(cn).getAdvancedCache().getStats().reset();
            }
         }
         resetNanoseconds.set(timeService.time());
      }
   }

   @ManagedAttribute(description = "Enables or disables the gathering of statistics by this component",
         displayName = "Statistics enabled",
         dataType = DataType.TRAIT,
         writable = true)
   public boolean isStatisticsEnabled() {
      return getStatisticsEnabled();
   }

   @ManagedAttribute(description = "Cache container total average number of milliseconds for all read operation in this cache container",
         displayName = "Cache container total average read time",
         units = Units.MILLISECONDS,
         displayType = DisplayType.SUMMARY)
   @Override
   public long getAverageReadTime() {
      long result = -1;
      if (getStatisticsEnabled()) {
         result = calculateAverageReadTime();
      }
      return result;
   }

   protected long calculateAverageReadTime() {
      long totalAverageReadTime = 0;
      int includedCacheCounter = 0;
      for (String cn : cm.getCacheNames()) {
         if (cm.cacheExists(cn)) {
            long averageReadTime = cm.getCache(cn).getAdvancedCache().getStats().getAverageReadTime();
            if (averageReadTime > 0) {
               includedCacheCounter++;
               totalAverageReadTime += averageReadTime;
            }
         }
      }
      if (includedCacheCounter > 0) {
         totalAverageReadTime = totalAverageReadTime / includedCacheCounter;
      }
      return totalAverageReadTime;
   }

   @ManagedAttribute(description = "Cache container total average number of milliseconds for all remove operation in this cache container",
         displayName = "Cache container total average remove time",
         units = Units.MILLISECONDS,
         displayType = DisplayType.SUMMARY)
   @Override
   public long getAverageRemoveTime() {
      long result = -1;
      if (getStatisticsEnabled()) {
         result = calculateAverageRemoveTime();
      }
      return result;
   }

   protected long calculateAverageRemoveTime() {
      long totalAverageRemoveTime = 0;
      int includedCacheCounter = 0;
      for (String cn : cm.getCacheNames()) {
         if (cm.cacheExists(cn)) {
            long averageRemoveTime = cm.getCache(cn).getAdvancedCache().getStats().getAverageRemoveTime();
            if (averageRemoveTime > 0) {
               includedCacheCounter++;
               totalAverageRemoveTime += averageRemoveTime;
            }
         }
      }
      if (includedCacheCounter > 0) {
         totalAverageRemoveTime = totalAverageRemoveTime / includedCacheCounter;
      }
      return totalAverageRemoveTime;
   }

   @ManagedAttribute(description = "Cache container average number of milliseconds for all write operation in this cache container",
         displayName = "Cache container average write time",
         units = Units.MILLISECONDS,
         displayType = DisplayType.SUMMARY)
   @Override
   public long getAverageWriteTime() {
      long result = -1;
      if (getStatisticsEnabled()) {
         result = calculateAverageWriteTime();
      }
      return result;
   }

   protected long calculateAverageWriteTime() {
      long totalAverageWriteTime = 0;
      int includedCacheCounter = 0;
      for (String cn : cm.getCacheNames()) {
         if (cm.cacheExists(cn)) {
            long averageWriteTime = cm.getCache(cn).getAdvancedCache().getStats().getAverageWriteTime();
            if (averageWriteTime > 0) {
               includedCacheCounter++;
               totalAverageWriteTime += averageWriteTime;
            }
         }
      }
      if (includedCacheCounter > 0) {
         totalAverageWriteTime = totalAverageWriteTime / includedCacheCounter;
      }
      return totalAverageWriteTime;
   }

   @ManagedAttribute(
         description = "Cache container total number of cache eviction operations",
         displayName = "Cache container total number of cache evictions",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   @Override
   public long getEvictions() {
      long result = -1;
      if (getStatisticsEnabled()) {
         result = calculateEvictions();
      }
      return result;
   }

   protected long calculateEvictions() {
      long totalEvictions = 0;
      for (String cn : cm.getCacheNames()) {
         if (cm.cacheExists(cn)) {
            long evictions = cm.getCache(cn).getAdvancedCache().getStats().getEvictions();
            if (evictions > 0) {
               totalEvictions += evictions;
            }
         }
      }
      return totalEvictions;
   }

   @ManagedAttribute(
         description = "Cache container total number of cache attribute hits",
         displayName = "Cache container total number of cache hits",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   @Override
   public long getHits() {
      long result = -1;
      if (getStatisticsEnabled()) {
         result = calculateHits();
      }
      return result;
   }

   protected long calculateHits() {
      long totalHits = 0;
      for (String cn : cm.getCacheNames()) {
         if (cm.cacheExists(cn)) {
            long hits = cm.getCache(cn).getAdvancedCache().getStats().getHits();
            if (hits > 0) {
               totalHits += hits;
            }
         }
      }
      return totalHits;
   }

   @ManagedAttribute(
         description = "Cache container total percentage hit/(hit+miss) ratio for this cache",
         displayName = "Cache container total hit ratio",
         units = Units.PERCENTAGE,
         displayType = DisplayType.SUMMARY
   )
   @Override
   public double getHitRatio() {
      double result = -1d;
      if (getStatisticsEnabled()) {
         result = calculateHitRatio();
      }
      return result;
   }

   protected double calculateHitRatio() {
      long totalHits = 0;
      double totalRequests = 0;
      double rwRatio = 0;
      for (String cn : cm.getCacheNames()) {
         if (cm.cacheExists(cn)) {
            Stats stats = cm.getCache(cn).getAdvancedCache().getStats();
            long requests = stats.getRetrievals();
            if (requests > 0) {
               totalHits += stats.getHits();
               totalRequests += requests;
            }
         }
      }
      if (totalRequests > 0) {
         rwRatio = totalHits / totalRequests;
      }
      return rwRatio;
   }

   @ManagedAttribute(
         description = "Cache container total number of cache attribute misses",
         displayName = "Cache container total number of cache misses",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   @Override
   public long getMisses() {
      long result = -1;
      if (getStatisticsEnabled()) {
         result = calculateMisses();
      }
      return result;
   }

   protected long calculateMisses() {
      long totalMisess = 0;
      for (String cn : cm.getCacheNames()) {
         if (cm.cacheExists(cn)) {
            long misses = cm.getCache(cn).getAdvancedCache().getStats().getMisses();
            if (misses > 0) {
               totalMisess += misses;
            }
         }
      }
      return totalMisess;
   }

   @ManagedAttribute(
         description = "Cache container total number of entries currently in all caches from this cache container",
         displayName = "Cache container total number of all cache entries",
         displayType = DisplayType.SUMMARY
   )
   public int getNumberOfEntries() {
      int result = -1;
      if (getStatisticsEnabled()) {
         result = calculateNumberOfEntries();
      }
      return result;
   }

   protected int calculateNumberOfEntries() {
      int totalNumberOfEntries = 0;
      for (String cn : cm.getCacheNames()) {
         if (cm.cacheExists(cn)) {
            int numOfEntries = cm.getCache(cn).getAdvancedCache().getStats().getCurrentNumberOfEntries();
            if (numOfEntries > 0) {
               totalNumberOfEntries += numOfEntries;
            }
         }
      }
      return totalNumberOfEntries;
   }

   @ManagedAttribute(
         description = "Cache container read/writes ratio in all caches from this cache container",
         displayName = "Cache container read/write ratio",
         units = Units.PERCENTAGE,
         displayType = DisplayType.SUMMARY
   )
   @Override
   public double getReadWriteRatio() {
      double result = -1d;
      if (getStatisticsEnabled()) {
         result = calculateReadWriteRatio();
      }
      return result;
   }

   protected double calculateReadWriteRatio() {
      long sumOfAllReads = 0;
      long sumOfAllWrites = 0;
      double rwRatio = 0;
      for (String cn : cm.getCacheNames()) {
         if (cm.cacheExists(cn)) {
            Stats stats = cm.getCache(cn).getAdvancedCache().getStats();
            long stores = stats.getStores();
            if (stores > 0) {
               sumOfAllReads += stats.getRetrievals();
               sumOfAllWrites += stores;
            }
         }
      }
      if (sumOfAllWrites > 0) {
         rwRatio = (double) sumOfAllReads / sumOfAllWrites;
      }
      return rwRatio;
   }

   @ManagedAttribute(
         description = "Cache container total number of cache removal hits",
         displayName = "Cache container total number of cache removal hits",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   @Override
   public long getRemoveHits() {
      long result = -1;
      if (getStatisticsEnabled()) {
         result = calculateRemoveHits();
      }
      return result;
   }

   protected long calculateRemoveHits() {
      long totalRemoveHits = 0;
      for (String cn : cm.getCacheNames()) {
         if (cm.cacheExists(cn)) {
            long removeHits = cm.getCache(cn).getAdvancedCache().getStats().getRemoveHits();
            if (removeHits > 0) {
               totalRemoveHits += removeHits;
            }
         }
      }
      return totalRemoveHits;
   }

   @ManagedAttribute(
         description = "Cache container total number of cache removals where keys were not found",
         displayName = "Cache container total number of cache removal misses",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   @Override
   public long getRemoveMisses() {
      long result = -1;
      if (getStatisticsEnabled()) {
         result = calculateRemoveMisses();
      }
      return result;
   }

   protected long calculateRemoveMisses() {
      long totalRemoveMisses = 0;
      for (String cn : cm.getCacheNames()) {
         if (cm.cacheExists(cn)) {
            long removeMisses = cm.getCache(cn).getAdvancedCache().getStats().getRemoveMisses();
            if (removeMisses > 0) {
               totalRemoveMisses += removeMisses;
            }
         }
      }
      return totalRemoveMisses;
   }

   @ManagedAttribute(
         description = "Cache container total number of cache attribute put operations",
         displayName = "Cache container total number of cache puts" ,
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   @Override
   public long getStores() {
      long result = -1;
      if (getStatisticsEnabled()) {
         result = calculateStores();
      }
      return result;
   }

   @Override
   @ManagedAttribute(
         description = "Number of seconds since the cache container statistics were last reset",
         displayName = "Seconds since cache container statistics were reset",
         units = Units.SECONDS,
         displayType = DisplayType.SUMMARY
   )
   public long getTimeSinceReset() {
      long result = -1;
      if (getStatisticsEnabled()) {
         result = timeService.timeDuration(resetNanoseconds.get(), TimeUnit.SECONDS);
      }
      return result;
   }

   protected long calculateStores() {
      long totalStores = 0;
      for (String cn : cm.getCacheNames()) {
         if (cm.cacheExists(cn)) {
            long stores = cm.getCache(cn).getAdvancedCache().getStats().getStores();
            if (stores > 0) {
               totalStores+= stores;
            }
         }
      }
      return totalStores;
   }

   @Override
   public long getTimeSinceStart() {
      long result = -1;
      if (getStatisticsEnabled()) {
         result = calculateTimeSinceStart();
      }
      return result;
   }

   protected long calculateTimeSinceStart() {
      long longestRunning = 0;
      for (String cn : cm.getCacheNames()) {
         if (cm.cacheExists(cn)) {
            long runningTime = cm.getCache(cn).getAdvancedCache().getStats().getTimeSinceStart();
            if (runningTime > longestRunning) {
               longestRunning = runningTime;
            }
         }
      }
      return longestRunning;
   }

   @Override
   public int getCurrentNumberOfEntries() {
      return getNumberOfEntries();
   }

   @Override
   public long getTotalNumberOfEntries() {
      return getStores();
   }

   @Override
   public long getRetrievals() {
      return getHits() + getMisses();
   }

   @Override
   public void reset() {
      resetStatistics();
   }
}
