package org.infinispan.stats.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.JmxStatisticsExposer;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.jmx.annotations.Units;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.stats.CacheContainerStats;
import org.infinispan.stats.Stats;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;


/**
 * Cache container statistics needed for admin console
 *
 * @author Vladimir Blagojevic
 * @since 7.1
 * @deprecated Since 10.1.3. This mixes statistics across unrelated caches so the reported numbers don't have too much
 * relevance. Please use {@link org.infinispan.stats.Stats} or {@link org.infinispan.stats.ClusterCacheStats} instead.
 */
@MBean(objectName = CacheContainerStats.OBJECT_NAME, description = "General cache container statistics such as timings, hit/miss ratio, etc. for a single node.")
@Scope(Scopes.GLOBAL)
@Deprecated
public class CacheContainerStatsImpl implements CacheContainerStats, JmxStatisticsExposer {

   private static final Log log = LogFactory.getLog(CacheContainerStatsImpl.class);
   private final EmbeddedCacheManager cm;
   private final AtomicLong resetNanoseconds = new AtomicLong(0);
   private boolean statisticsEnabled = false;
   @Inject TimeService timeService;

   public CacheContainerStatsImpl(EmbeddedCacheManager cm) {
      this.cm = cm;
   }

   @Start
   void start() {
      setStatisticsEnabled(cm.getCacheManagerConfiguration().statistics());
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
         getEnabledStats().forEach(Stats::reset);
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
         units = Units.MILLISECONDS)
   @Override
   public long getAverageReadTime() {
      long result = -1;
      if (getStatisticsEnabled()) {
         result = calculateAverageReadTime();
      }
      return result;
   }

   private long calculateAverageReadTime() {
      long totalAverageReadTime = 0;
      int includedCacheCounter = 0;
      for (Stats stats : getEnabledStats()) {
         long averageReadTime = stats.getAverageReadTime();
         if (averageReadTime > 0) {
            includedCacheCounter++;
            totalAverageReadTime += averageReadTime;
         }
      }
      if (includedCacheCounter > 0) {
         totalAverageReadTime = totalAverageReadTime / includedCacheCounter;
      }
      return totalAverageReadTime;
   }

   @ManagedAttribute(description = "Cache container total average number of nanoseconds for all read operation in this cache container",
         displayName = "Cache container total average read time (ns)",
         units = Units.NANOSECONDS
   )
   @Override
   public long getAverageReadTimeNanos() {
      long result = -1;
      if (getStatisticsEnabled()) {
         result = calculateAverageReadTimeNanos();
      }
      return result;
   }

   private long calculateAverageReadTimeNanos() {
      long totalAverageReadTime = 0;
      int includedCacheCounter = 0;
      for (Stats stats : getEnabledStats()) {
         long averageReadTime = stats.getAverageReadTimeNanos();
         if (averageReadTime > 0) {
            includedCacheCounter++;
            totalAverageReadTime += averageReadTime;
         }
      }
      if (includedCacheCounter > 0) {
         totalAverageReadTime = totalAverageReadTime / includedCacheCounter;
      }
      return totalAverageReadTime;
   }

   @ManagedAttribute(description = "Required minimum number of nodes to hold current cache data",
         displayName = "Required minimum number of nodes"
   )
   @Override
   public int getRequiredMinimumNumberOfNodes() {
      int result = -1;
      for (Stats stats : getStats()) {
         result = Math.max(result, stats.getRequiredMinimumNumberOfNodes());
      }
      return result;
   }

   @ManagedAttribute(description = "Cache container total average number of milliseconds for all remove operation in this cache container",
         displayName = "Cache container total average remove time",
         units = Units.MILLISECONDS
   )
   @Override
   public long getAverageRemoveTime() {
      long result = -1;
      if (getStatisticsEnabled()) {
         result = calculateAverageRemoveTime();
      }
      return result;
   }

   private long calculateAverageRemoveTime() {
      long totalAverageRemoveTime = 0;
      int includedCacheCounter = 0;
      for (Stats stats : getEnabledStats()) {
         long averageRemoveTime = stats.getAverageRemoveTime();
         if (averageRemoveTime > 0) {
            includedCacheCounter++;
            totalAverageRemoveTime += averageRemoveTime;
         }
      }
      if (includedCacheCounter > 0) {
         totalAverageRemoveTime = totalAverageRemoveTime / includedCacheCounter;
      }
      return totalAverageRemoveTime;
   }

   @ManagedAttribute(description = "Cache container total average number of nanoseconds for all remove operation in this cache container",
         displayName = "Cache container total average remove time (ns)",
         units = Units.NANOSECONDS
   )
   @Override
   public long getAverageRemoveTimeNanos() {
      long result = -1;
      if (getStatisticsEnabled()) {
         result = calculateAverageRemoveTimeNanos();
      }
      return result;
   }

   private long calculateAverageRemoveTimeNanos() {
      long totalAverageRemoveTime = 0;
      int includedCacheCounter = 0;
      for (Stats stats : getEnabledStats()) {
         long averageRemoveTime = stats.getAverageRemoveTimeNanos();
         if (averageRemoveTime > 0) {
            includedCacheCounter++;
            totalAverageRemoveTime += averageRemoveTime;
         }
      }
      if (includedCacheCounter > 0) {
         totalAverageRemoveTime = totalAverageRemoveTime / includedCacheCounter;
      }
      return totalAverageRemoveTime;
   }

   @ManagedAttribute(description = "Cache container average number of milliseconds for all write operation in this cache container",
         displayName = "Cache container average write time",
         units = Units.MILLISECONDS
   )
   @Override
   public long getAverageWriteTime() {
      long result = -1;
      if (getStatisticsEnabled()) {
         result = calculateAverageWriteTime();
      }
      return result;
   }

   private long calculateAverageWriteTime() {
      long totalAverageWriteTime = 0;
      int includedCacheCounter = 0;
      for (Stats stats : getEnabledStats()) {
         long averageWriteTime = stats.getAverageWriteTime();
         if (averageWriteTime > 0) {
            includedCacheCounter++;
            totalAverageWriteTime += averageWriteTime;
         }
      }
      if (includedCacheCounter > 0) {
         totalAverageWriteTime = totalAverageWriteTime / includedCacheCounter;
      }
      return totalAverageWriteTime;
   }

   @ManagedAttribute(description = "Cache container average number of nanoseconds for all write operation in this cache container",
         displayName = "Cache container average write time (ns)",
         units = Units.MILLISECONDS
   )
   @Override
   public long getAverageWriteTimeNanos() {
      long result = -1;
      if (getStatisticsEnabled()) {
         result = calculateAverageWriteTimeNanos();
      }
      return result;
   }

   private long calculateAverageWriteTimeNanos() {
      long totalAverageWriteTime = 0;
      int includedCacheCounter = 0;
      for (Stats stats : getEnabledStats()) {
         long averageWriteTime = stats.getAverageWriteTimeNanos();
         if (averageWriteTime > 0) {
            includedCacheCounter++;
            totalAverageWriteTime += averageWriteTime;
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
         measurementType = MeasurementType.TRENDSUP
   )
   @Override
   public long getEvictions() {
      long result = -1;
      if (getStatisticsEnabled()) {
         result = calculateEvictions();
      }
      return result;
   }

   private long calculateEvictions() {
      long totalEvictions = 0;
      for (Stats stats : getEnabledStats()) {
         long evictions = stats.getEvictions();
         if (evictions > 0) {
            totalEvictions += evictions;
         }
      }
      return totalEvictions;
   }

   @ManagedAttribute(
         description = "Cache container total number of cache attribute hits",
         displayName = "Cache container total number of cache hits",
         measurementType = MeasurementType.TRENDSUP
   )
   @Override
   public long getHits() {
      long result = -1;
      if (getStatisticsEnabled()) {
         result = calculateHits();
      }
      return result;
   }

   private long calculateHits() {
      long totalHits = 0;
      for (Stats stats : getEnabledStats()) {
         long hits = stats.getHits();
         if (hits > 0) {
            totalHits += hits;
         }
      }
      return totalHits;
   }

   @ManagedAttribute(
         description = "Cache container total percentage hit/(hit+miss) ratio for this cache",
         displayName = "Cache container total hit ratio",
         units = Units.PERCENTAGE
   )
   @Override
   public double getHitRatio() {
      double result = -1d;
      if (getStatisticsEnabled()) {
         result = calculateHitRatio();
      }
      return result;
   }

   private double calculateHitRatio() {
      long totalHits = 0;
      double totalRequests = 0;
      double rwRatio = 0;
      for (Stats stats : getEnabledStats()) {
         long requests = stats.getRetrievals();
         if (requests > 0) {
            totalHits += stats.getHits();
            totalRequests += requests;
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
         measurementType = MeasurementType.TRENDSUP
   )
   @Override
   public long getMisses() {
      long result = -1;
      if (getStatisticsEnabled()) {
         result = calculateMisses();
      }
      return result;
   }

   private long calculateMisses() {
      long totalMisess = 0;
      for (Stats stats : getEnabledStats()) {
         long misses = stats.getMisses();
         if (misses > 0) {
            totalMisess += misses;
         }
      }
      return totalMisess;
   }

   @ManagedAttribute(
         description = "Cache container total number of entries currently in all caches from this cache container",
         displayName = "Cache container total number of all cache entries"
   )
   public int getNumberOfEntries() {
      int result = statisticsEnabled ? 0 : -1;
      if (statisticsEnabled) {
         for (Stats stats : getEnabledStats()) {
            int numOfEntries = stats.getCurrentNumberOfEntries();
            if (numOfEntries > 0) {
               result += numOfEntries;
            }
         }
      }
      return result;
   }

   @ManagedAttribute(
         description = "Cache container total number of entries currently in-memory for all caches in this cache container",
         displayName = "Cache container total number of in-memory cache entries"
   )
   public int getCurrentNumberOfEntriesInMemory() {
      int result = statisticsEnabled ? 0 : -1;
      if (statisticsEnabled) {
         for (Stats stats : getEnabledStats()) {
            int numOfEntries = stats.getCurrentNumberOfEntriesInMemory();
            if (numOfEntries > 0) {
               result += numOfEntries;
            }
         }
      }
      return result;
   }

   @ManagedAttribute(
         description = "Cache container read/writes ratio in all caches from this cache container",
         displayName = "Cache container read/write ratio",
         units = Units.PERCENTAGE
   )
   @Override
   public double getReadWriteRatio() {
      double result = -1d;
      if (getStatisticsEnabled()) {
         result = calculateReadWriteRatio();
      }
      return result;
   }

   private double calculateReadWriteRatio() {
      long sumOfAllReads = 0;
      long sumOfAllWrites = 0;
      double rwRatio = 0;
      for (Stats stats : getEnabledStats()) {
         long stores = stats.getStores();
         if (stores > 0) {
            sumOfAllReads += stats.getRetrievals();
            sumOfAllWrites += stores;
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
         measurementType = MeasurementType.TRENDSUP
   )
   @Override
   public long getRemoveHits() {
      long result = -1;
      if (getStatisticsEnabled()) {
         result = calculateRemoveHits();
      }
      return result;
   }

   private long calculateRemoveHits() {
      long totalRemoveHits = 0;
      for (Stats stats : getEnabledStats()) {
         long removeHits = stats.getRemoveHits();
         if (removeHits > 0) {
            totalRemoveHits += removeHits;
         }
      }
      return totalRemoveHits;
   }

   @ManagedAttribute(
         description = "Cache container total number of cache removals where keys were not found",
         displayName = "Cache container total number of cache removal misses",
         measurementType = MeasurementType.TRENDSUP
   )
   @Override
   public long getRemoveMisses() {
      long result = -1;
      if (getStatisticsEnabled()) {
         result = calculateRemoveMisses();
      }
      return result;
   }

   private long calculateRemoveMisses() {
      long totalRemoveMisses = 0;
      for (Stats stats : getEnabledStats()) {
         long removeMisses = stats.getRemoveMisses();
         if (removeMisses > 0) {
            totalRemoveMisses += removeMisses;
         }
      }
      return totalRemoveMisses;
   }

   @ManagedAttribute(
         description = "Cache container total number of cache put operations",
         displayName = "Cache container total number of cache puts",
         measurementType = MeasurementType.TRENDSUP
   )
   @Override
   public long getStores() {
      long result = -1;
      if (getStatisticsEnabled()) {
         result = calculateStores();
      }
      return result;
   }

   @ManagedAttribute(
         description = "Number of seconds since the cache container statistics were last reset",
         displayName = "Seconds since cache container statistics were reset",
         units = Units.SECONDS
   )
   @Override
   public long getTimeSinceReset() {
      long result = -1;
      if (getStatisticsEnabled()) {
         result = timeService.timeDuration(resetNanoseconds.get(), TimeUnit.SECONDS);
      }
      return result;
   }

   private long calculateStores() {
      long totalStores = 0;
      for (Stats stats : getEnabledStats()) {
         long stores = stats.getStores();
         if (stores > 0) {
            totalStores += stores;
         }
      }
      return totalStores;
   }

   @ManagedAttribute(
         description = "Number of seconds since cache started",
         displayName = "Seconds since cache started",
         units = Units.SECONDS,
         measurementType = MeasurementType.TRENDSUP
   )
   @Override
   public long getTimeSinceStart() {
      long result = -1;
      if (getStatisticsEnabled()) {
         result = calculateTimeSinceStart();
      }
      return result;
   }

   private long calculateTimeSinceStart() {
      long longestRunning = 0;
      for (Stats stats : getEnabledStats()) {
         long runningTime = stats.getTimeSinceStart();
         if (runningTime > longestRunning) {
            longestRunning = runningTime;
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

   @ManagedAttribute(
         description = "Amount in bytes of memory used in a given cache container for entries with eviction",
         displayName = "Container memory used by eviction"
   )
   @Override
   public long getDataMemoryUsed() {
      return calculateDataMemoryUsed();
   }

   private long calculateDataMemoryUsed() {
      long totalMemoryUsed = 0;
      for (Stats stats : getEnabledStats()) {
         long memoryUsed = stats.getDataMemoryUsed();
         if (memoryUsed > 0) {
            totalMemoryUsed += memoryUsed;
         }
      }
      return totalMemoryUsed;
   }

   @ManagedAttribute(
         description = "Amount in bytes of off-heap memory used by this cache container",
         displayName = "Off-Heap memory used"
   )
   @Override
   public long getOffHeapMemoryUsed() {
      return calculateOffHeapUsed();
   }

   private long calculateOffHeapUsed() {
      long totalOffHeapUsed = 0;
      for (Stats stats : getEnabledStats()) {
         long offHeapUsed = stats.getOffHeapMemoryUsed();
         if (offHeapUsed > 0) {
            totalOffHeapUsed += offHeapUsed;
         }
      }
      return totalOffHeapUsed;
   }

   @Override
   public long getRetrievals() {
      return getHits() + getMisses();
   }

   @Override
   public void reset() {
      resetStatistics();
   }

   private List<Stats> getStats() {
      List<Stats> stats = new ArrayList<>();
      for (String cn : cm.getCacheNames()) {
         if (cm.cacheExists(cn)) {
            AdvancedCache<?, ?> cache = getCache(cn);
            if (cache != null) {
               stats.add(cache.getStats());
            }
         }
      }
      return stats;
   }

   private AdvancedCache<?, ?> getCache(String cacheName) {
      try {
         return SecurityActions.getUnwrappedCache(cm.getCache(cacheName)).getAdvancedCache();
      } catch (CacheException t) {
         log.cannotObtainFailedCache(cacheName, t);
      }
      return null;
   }

   private List<Stats> getEnabledStats() {
      List<Stats> stats = new ArrayList<>();
      for (String cn : cm.getCacheNames()) {
         if (cm.cacheExists(cn)) {
            AdvancedCache<?, ?> cache = getCache(cn);
            if (cache != null) {
               Configuration cfg = cache.getCacheConfiguration();
               if (cfg.statistics().enabled()) {
                  stats.add(cache.getStats());
               }
            }
         }
      }
      return stats;
   }

   @Override
   public Json toJson() {
      return Json.object()
            .set("statistics_enabled", statisticsEnabled)
            .set("number_of_entries", getNumberOfEntries())
            .set("hit_ratio", getHitRatio())
            .set("read_write_ratio", getReadWriteRatio())
            .set("time_since_start", getTimeSinceStart())
            .set("time_since_reset", getTimeSinceReset())
            .set("current_number_of_entries", getCurrentNumberOfEntries())
            .set("current_number_of_entries_in_memory", getCurrentNumberOfEntriesInMemory())
            .set("total_number_of_entries", getTotalNumberOfEntries())
            .set("off_heap_memory_used", getOffHeapMemoryUsed())
            .set("data_memory_used", getDataMemoryUsed())
            .set("stores", getStores())
            .set("retrievals", getRetrievals())
            .set("hits", getHits())
            .set("misses", getMisses())
            .set("remove_hits", getRemoveHits())
            .set("remove_misses", getRemoveMisses())
            .set("evictions", getEvictions())
            .set("average_read_time", getAverageReadTime())
            .set("average_read_time_nanos", getAverageReadTimeNanos())
            .set("average_write_time", getAverageWriteTime())
            .set("average_write_time_nanos", getAverageWriteTimeNanos())
            .set("average_remove_time", getAverageRemoveTime())
            .set("average_remove_time_nanos", getAverageRemoveTimeNanos())
            .set("required_minimum_number_of_nodes", getRequiredMinimumNumberOfNodes());
   }
}
