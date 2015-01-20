package org.infinispan.stats.impl;

import org.infinispan.factories.annotations.Start;
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
   private boolean statisticsEnabled = false;

   public CacheContainerStatsImpl(EmbeddedCacheManager cm) {
      this.cm = cm;
      cm.getGlobalComponentRegistry().registerComponent(this, CacheContainerStats.class);
   }

   @Override
   public void setStatisticsEnabled(boolean enabled) {
      if (enabled) {
         // TODO we force statistics collection on all caches beloning to this cache container?
         for (String cn : cm.getCacheNames()) {
            if (cm.cacheExists(cn)) {
               cm.getCache(cn).getAdvancedCache().getStats().setStatisticsEnabled(true);
            }
         }
      }
      this.statisticsEnabled = enabled;
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
      }
   }

   @Start(priority = 9)
   private void start() {
      //TODO how to read this property i.e. if cache container stats are enabled?
      //setStatisticsEnabled();
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
      long totalAverageReadTime = 0;
      int existingCacheCounter = 0;
      for (String cn : cm.getCacheNames()) {
         if (cm.cacheExists(cn)) {
            existingCacheCounter++;
            Stats stats = cm.getCache(cn).getAdvancedCache().getStats();
            totalAverageReadTime += stats.getAverageReadTime();
         }
      }
      if (existingCacheCounter > 0) {
         return totalAverageReadTime / existingCacheCounter;
      } else {
         return -1;
      }
   }

   @ManagedAttribute(description = "Cache container total average number of milliseconds for all remove operation in this cache container",
         displayName = "Cache container total average remove time",
         units = Units.MILLISECONDS,
         displayType = DisplayType.SUMMARY)
   @Override
   public long getAverageRemoveTime() {
      long totalAverageRemoveTime = 0;
      int existingCacheCounter = 0;
      for (String cn : cm.getCacheNames()) {
         if (cm.cacheExists(cn)) {
            existingCacheCounter++;
            Stats stats = cm.getCache(cn).getAdvancedCache().getStats();
            totalAverageRemoveTime += stats.getAverageRemoveTime();
         }
      }
      if (existingCacheCounter > 0) {
         return totalAverageRemoveTime / existingCacheCounter;
      } else {
         return -1;
      }
   }

   @ManagedAttribute(description = "Cache container average number of milliseconds for all write operation in this cache container",
         displayName = "Cache container average write time",
         units = Units.MILLISECONDS,
         displayType = DisplayType.SUMMARY)
   @Override
   public long getAverageWriteTime() {
      long totalAverageWriteTime = 0;
      int existingCacheCounter = 0;
      for (String cn : cm.getCacheNames()) {
         if (cm.cacheExists(cn)) {
            existingCacheCounter++;
            Stats stats = cm.getCache(cn).getAdvancedCache().getStats();
            totalAverageWriteTime += stats.getAverageWriteTime();
         }
      }
      if (existingCacheCounter > 0) {
         return totalAverageWriteTime / existingCacheCounter;
      } else {
         return -1;
      }
   }

   @ManagedAttribute(
         description = "Cache container total number of cache eviction operations",
         displayName = "Cache container total number of cache evictions",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   @Override
   public long getEvictions() {
      long totalEvictions = 0;
      for (String cn : cm.getCacheNames()) {
         if (cm.cacheExists(cn)) {
            Stats stats = cm.getCache(cn).getAdvancedCache().getStats();
            totalEvictions += stats.getEvictions();
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
      long totalHits = 0;
      for (String cn : cm.getCacheNames()) {
         if (cm.cacheExists(cn)) {
            Stats stats = cm.getCache(cn).getAdvancedCache().getStats();
            totalHits += stats.getHits();
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
      double totalHitsOverMisses = 0;
      int existingCacheCounter = 0;
      for (String cn : cm.getCacheNames()) {
         if (cm.cacheExists(cn)) {
            existingCacheCounter++;
            Stats stats = cm.getCache(cn).getAdvancedCache().getStats();
            long misses = stats.getMisses();
            if (misses > 0) {
               totalHitsOverMisses += stats.getHits() / misses;
            }
         }
      }
      if (existingCacheCounter > 0) {
         return totalHitsOverMisses / existingCacheCounter;
      } else {
         return -1;
      }
   }

   @ManagedAttribute(
         description = "Cache container total number of cache attribute misses",
         displayName = "Cache container total number of cache misses",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   @Override
   public long getMisses() {
      long totalMisses = 0;
      for (String cn : cm.getCacheNames()) {
         if (cm.cacheExists(cn)) {
            Stats stats = cm.getCache(cn).getAdvancedCache().getStats();
            totalMisses += stats.getMisses();
         }
      }
      return totalMisses;
   }

   @ManagedAttribute(
         description = "Cache container total number of entries currently in all caches from this cache container",
         displayName = "Cache container total number of all cache entries",
         displayType = DisplayType.SUMMARY
   )
   public int getNumberOfEntries() {
      int totalEntries = 0;
      for (String cn : cm.getCacheNames()) {
         if (cm.cacheExists(cn)) {
            Stats stats = cm.getCache(cn).getAdvancedCache().getStats();
            totalEntries += stats.getCurrentNumberOfEntries();
         }
      }
      return totalEntries;
   }

   @ManagedAttribute(
         description = "Cache container read/writes ratio in all caches from this cache container",
         displayName = "Cache container read/write ratio",
         units = Units.PERCENTAGE,
         displayType = DisplayType.SUMMARY
   )
   @Override
   public double getReadWriteRatio() {
      double totalRWRatio = 0;
      int existingCacheCounter = 0;
      for (String cn : cm.getCacheNames()) {
         if (cm.cacheExists(cn)) {
            existingCacheCounter++;
            Stats stats = cm.getCache(cn).getAdvancedCache().getStats();
            long stores = stats.getStores();
            if (stores > 0) {
               totalRWRatio += (double) ((stats.getHits() + stats.getMisses()) / stores);
            }
         }
      }
      if (existingCacheCounter > 0) {
         return totalRWRatio / existingCacheCounter;
      } else {
         return -1;
      }
   }

   @ManagedAttribute(
         description = "Cache container total number of cache removal hits",
         displayName = "Cache container total number of cache removal hits",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   @Override
   public long getRemoveHits() {
      long removeHits = 0;
      for (String cn : cm.getCacheNames()) {
         if (cm.cacheExists(cn)) {
            Stats stats = cm.getCache(cn).getAdvancedCache().getStats();
            removeHits += stats.getRemoveHits();
         }
      }
      return removeHits;
   }

   @ManagedAttribute(
         description = "Cache container total number of cache removals where keys were not found",
         displayName = "Cache container total number of cache removal misses",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   @Override
   public long getRemoveMisses() {
      long removeMisses = 0;
      for (String cn : cm.getCacheNames()) {
         if (cm.cacheExists(cn)) {
            Stats stats = cm.getCache(cn).getAdvancedCache().getStats();
            removeMisses += stats.getRemoveMisses();
         }
      }
      return removeMisses;
   }

   @ManagedAttribute(
         description = "Cache container total number of cache attribute put operations",
         displayName = "Cache container total number of cache puts" ,
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY
   )
   @Override
   public long getStores() {
      long totalStores = 0;
      for (String cn : cm.getCacheNames()) {
         if (cm.cacheExists(cn)) {
            Stats stats = cm.getCache(cn).getAdvancedCache().getStats();
            totalStores += stats.getStores();
         }
      }
      return totalStores;
   }

   @Override
   public long getTimeSinceStart() {
      long longestRunning = 0;
      for (String cn : cm.getCacheNames()) {
         if (cm.cacheExists(cn)) {
            Stats stats = cm.getCache(cn).getAdvancedCache().getStats();
            long runningTime = stats.getTimeSinceStart();
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
