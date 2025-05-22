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
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.stats.CacheContainerStats;
import org.infinispan.stats.Stats;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;


/**
 * Cache container statistics
 *
 * @author Vladimir Blagojevic
 * @since 7.1
 */
@MBean(objectName = CacheContainerStats.OBJECT_NAME, description = "General cache container statistics for a single node.")
@Scope(Scopes.GLOBAL)
public class CacheContainerStatsImpl implements CacheContainerStats, JmxStatisticsExposer {

   private static final Log log = LogFactory.getLog(CacheContainerStatsImpl.class);
   private final EmbeddedCacheManager cm;
   private final AtomicLong resetNanoseconds = new AtomicLong(0);
   private boolean statisticsEnabled = false;
   @Inject TimeService timeService;

   private volatile StatsHolder enabledStats;

   public CacheContainerStatsImpl(EmbeddedCacheManager cm) {
      this.cm = cm;
   }

   @Start
   void start() {
      setStatisticsEnabled(SecurityActions.getCacheManagerConfiguration(cm).statistics());
   }

   @Override
   public void setStatisticsEnabled(boolean enabled) {
      this.statisticsEnabled = enabled;
      if (enabled) {
         // technically we do not reset stats, but we initialize them
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

   @ManagedAttribute(description = "Required minimum number of nodes to hold current cache data",
         displayName = "Required minimum number of nodes"
   )
   @Override
   public int getRequiredMinimumNumberOfNodes() {
      int result = -1;
      for (Stats stats : getEnabledStats()) {
         result = Math.max(result, stats.getRequiredMinimumNumberOfNodes());
      }
      return result;
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
   public void reset() {
      resetStatistics();
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
      if (enabledStats != null && !enabledStats.isExpired())
         return enabledStats.stats;

      List<Stats> stats = new ArrayList<>();
      for (String cn : cm.getCacheNames()) {
         if (cm.isRunning(cn)) {
            AdvancedCache<?, ?> cache = getCache(cn);
            if (cache != null) {
               Configuration cfg = cache.getCacheConfiguration();
               if (cfg.statistics().enabled()) {
                  stats.add(cache.getStats());
               }
            }
         }
      }
      this.enabledStats = new StatsHolder(stats);
      return stats;
   }

   @Override
   public Json toJson() {
      return Json.object()
            .set("statistics_enabled", statisticsEnabled)
            .set("off_heap_memory_used", getOffHeapMemoryUsed())
            .set("data_memory_used", getDataMemoryUsed())
            .set("required_minimum_number_of_nodes", getRequiredMinimumNumberOfNodes());
   }

   private final class StatsHolder {
      final long expiration;
      final List<Stats> stats;

      StatsHolder(List<Stats> stats) {
         this.expiration = timeService.expectedEndTime(1, TimeUnit.SECONDS);
         this.stats = stats;
      }

      boolean isExpired() {
         return timeService.isTimeExpired(expiration);
      }
   }
}
