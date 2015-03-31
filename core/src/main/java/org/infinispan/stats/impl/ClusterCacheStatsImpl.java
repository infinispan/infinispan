package org.infinispan.stats.impl;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.eviction.ActivationManager;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.ActivationInterceptor;
import org.infinispan.interceptors.CacheWriterInterceptor;
import org.infinispan.interceptors.InvalidationInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.jmx.JmxStatisticsExposer;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.DisplayType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.jmx.annotations.Parameter;
import org.infinispan.jmx.annotations.Units;
import org.infinispan.stats.ClusterCacheStats;
import org.infinispan.stats.Stats;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

@MBean(objectName = "ClusterCacheStats", description = "General cluster statistics such as timings, hit/miss ratio, etc.")
public class ClusterCacheStatsImpl implements ClusterCacheStats, JmxStatisticsExposer {

   private static final long serialVersionUID = -7692443865184602500L;

   private static final String TIME_SINCE_START = "timeSinceStart";
   private static final String REMOVE_MISSES = "removeMisses";
   private static final String REMOVE_HITS = "removeHits";
   private static final String AVERAGE_WRITE_TIME = "averageWriteTime";
   private static final String AVERAGE_READ_TIME = "averageReadTime";
   private static final String AVERAGE_REMOVE_TIME = "averageRemoveTime";
   private static final String EVICTIONS = "evictions";
   private static final String HITS = "hits";
   private static final String MISSES = "misses";
   private static final String NUMBER_OF_ENTRIES = "numberOfEntries";
   private static final String STORES = "stores";

   //LockManager
   private static final String NUMBER_OF_LOCKS_HELD = "numberOfLocksHeld";
   private static final String NUMBER_OF_LOCKS_AVAILABLE = "numberOfLocksAvailable";

   //Invalidation/passivation/activation
   private static final String INVALIDATIONS = "invalidations";
   private static final String PASSIVATIONS = "passivations";
   private static final String ACTIVATIONS = "activations";

   //cache loaders
   private static final String CACHE_LOADER_LOADS = "cacheLoaderLoads";
   private static final String CACHE_LOADER_MISSES = "cacheLoaderMisses";
   private static final String CACHE_WRITER_STORES = "cacheWriterStores";


   public static final long DEFAULT_STALE_STATS_THRESHOLD = 3000;

   private static final Log log = LogFactory.getLog(ClusterCacheStatsImpl.class);
   private transient Cache<?, ?> cache;
   private transient DefaultExecutorService des;
   private TimeService ts;
   private boolean statisticsEnabled = false;
   private long staleStatsTreshold = DEFAULT_STALE_STATS_THRESHOLD;
   private long statsUpdateTimestamp = 0;
   private final AtomicLong resetNanoseconds = new AtomicLong(0);

   private long stores;
   private long timeSinceStart;
   private long removeMisses;
   private long removeHits;
   private long misses;
   private long hits;
   private long evictions;
   private long numberOfEntries;
   private long averageWriteTime;
   private long averageReadTime;
   private long averageRemoveTime;
   private double readWriteRatio;
   private double hitRatio;

   //LockManager
   int numberOfLocksHeld;
   int numberOfLocksAvailable;

   //invalidation, passivation activation
   long invalidations;
   long activations;
   long passivations;

   //cacheloader metrics
   long cacheLoaderLoads;
   long cacheLoaderMisses;
   long cacheWriterStores;

   @Inject
   public void injectDependencies(Cache<?, ?> cache, TimeService ts, Configuration configuration) {
      this.cache = cache;
      this.ts = ts;
      this.statisticsEnabled = configuration.jmxStatistics().enabled();
   }

   @Start
   public void start() {
      this.des = SecurityActions.getDefaultExecutorService(cache);
   }

   @Stop
   public void stop() {
      if (des != null && !des.isShutdown()) {
         des.shutdownNow();
      }
   }

   public long getStaleStatsTreshold() {
      return staleStatsTreshold;
   }

   @ManagedOperation(description = "Sets the treshold for cluster wide stats refresh (miliseconds)",
         name = "setStaleStatsTreshold",
         displayName = "Set the cluster wide stats refresh treshold")
   public void setStaleStatsTreshold(@Parameter(name = "staleStatsTreshold", description = "Stats refresh treshold in miliseconds") long staleStatsTreshold) {
      this.staleStatsTreshold = staleStatsTreshold;
   }

   // -------------------------------------------- JMX information -----------------------------------------------

   @ManagedOperation(description = "Resets statistics gathered by this component", displayName = "Reset statistics")
   public void resetStatistics() {
      if (isStatisticsEnabled()) {
         reset();
         resetNanoseconds.set(ts.time());
      }
   }

   @Override
   public void setStatisticsEnabled(boolean enabled) {
      this.statisticsEnabled = enabled;
      if (enabled) {
         //yes technically we do not reset stats but we initialize them
         resetNanoseconds.set(ts.time());
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

   @ManagedAttribute(description = "Cluster wide total average number of milliseconds for a read operation on the cache",
         displayName = "Cluster wide total average read time",
         units = Units.MILLISECONDS,
         displayType = DisplayType.SUMMARY)
   public long getAverageReadTime() {
      if (isStatisticsEnabled()) {
         fetchClusterWideStatsIfNeeded();
         return averageReadTime;
      } else {
         return -1;
      }
   }

   @ManagedAttribute(description = "Cluster wide total average number of milliseconds for a remove operation in the cache",
         displayName = "Cluster wide total average remove time",
         units = Units.MILLISECONDS,
         displayType = DisplayType.SUMMARY)
   public long getAverageRemoveTime() {
      if (isStatisticsEnabled()) {
         fetchClusterWideStatsIfNeeded();
         return averageRemoveTime;
      } else {
         return -1;
      }
   }

   @ManagedAttribute(description = "Cluster wide average number of milliseconds for a write operation in the cache",
         displayName = "Cluster wide average write time",
         units = Units.MILLISECONDS,
         displayType = DisplayType.SUMMARY)
   public long getAverageWriteTime() {
      if (isStatisticsEnabled()) {
         fetchClusterWideStatsIfNeeded();
         return averageWriteTime;
      } else {
         return -1;
      }
   }

   @ManagedAttribute(description = "Cluster wide total number of cache eviction operations",
         displayName = "Cluster wide total number of cache evictions",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   @Override
   public long getEvictions() {
      if (isStatisticsEnabled()) {
         fetchClusterWideStatsIfNeeded();
         return evictions;
      } else {
         return -1;
      }
   }

   @ManagedAttribute(description = "Cluster wide total number of cache attribute hits",
         displayName = "Cluster wide total number of cache hits",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   @Override
   public long getHits() {
      if (isStatisticsEnabled()) {
         fetchClusterWideStatsIfNeeded();
         return hits;
      } else {
         return -1;
      }
   }

   @ManagedAttribute(description = "Cluster wide total percentage hit/(hit+miss) ratio for this cache",
         displayName = "Cluster wide total hit ratio",
         units = Units.PERCENTAGE,
         displayType = DisplayType.SUMMARY)
   @Override
   public double getHitRatio() {
      if (isStatisticsEnabled()) {
         fetchClusterWideStatsIfNeeded();
         return hitRatio;
      } else {
         return -1;
      }
   }

   @ManagedAttribute(description = "Cluster wide total number of cache attribute misses",
         displayName = "Cluster wide total number of cache misses",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   public long getMisses() {
      if (isStatisticsEnabled()) {
         fetchClusterWideStatsIfNeeded();
         return misses;
      } else {
         return -1;
      }
   }

   @ManagedAttribute(description = "Cluster wide total number of entries currently in the cache",
         displayName = "Cluster wide total number of current cache entries",
         displayType = DisplayType.SUMMARY)
   public int getNumberOfEntries() {
      if (isStatisticsEnabled()) {
         fetchClusterWideStatsIfNeeded();
         return (int) numberOfEntries;
      } else {
         return -1;
      }
   }

   @ManagedAttribute(description = "Cluster wide read/writes ratio for the cache",
         displayName = "Cluster wide read/write ratio",
         units = Units.PERCENTAGE,
         displayType = DisplayType.SUMMARY)
   @Override
   public double getReadWriteRatio() {
      if (isStatisticsEnabled()) {
         fetchClusterWideStatsIfNeeded();
         return readWriteRatio;
      } else {
         return -1;
      }
   }

   @ManagedAttribute(description = "Cluster wide total number of cache removal hits",
         displayName = "Cluster wide total number of cache removal hits",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   @Override
   public long getRemoveHits() {
      if (isStatisticsEnabled()) {
         fetchClusterWideStatsIfNeeded();
         return removeHits;
      } else {
         return -1;
      }
   }

   @ManagedAttribute(description = "Cluster wide total number of cache removals where keys were not found",
         displayName = "Cluster wide total number of cache removal misses",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   @Override
   public long getRemoveMisses() {
      if (isStatisticsEnabled()) {
         fetchClusterWideStatsIfNeeded();
         return removeMisses;
      } else {
         return -1;
      }
   }

   @ManagedAttribute(description = "Cluster wide total number of cache attribute put operations",
         displayName = "Cluster wide total number of cache puts",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   @Override
   public long getStores() {
      if (isStatisticsEnabled()) {
         fetchClusterWideStatsIfNeeded();
         return stores;
      } else {
         return -1;
      }
   }

   @ManagedAttribute(description = "Number of seconds since the first cache node started",
         displayName = "Number of seconds since the first cache node started",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   @Override
   public long getTimeSinceStart() {
      if (isStatisticsEnabled()) {
         fetchClusterWideStatsIfNeeded();
         return timeSinceStart;
      } else {
         return -1;
      }
   }

   @ManagedAttribute(
         description = "Number of seconds since the cluster-wide cache statistics were last reset",
         displayName = "Seconds since cluster-wide cache statistics were reset",
         units = Units.SECONDS,
         displayType = DisplayType.SUMMARY
   )
   public long getTimeSinceReset() {
      long result = -1;
      if (isStatisticsEnabled()) {
         result = ts.timeDuration(resetNanoseconds.get(), TimeUnit.SECONDS);
      }
      return result;
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
      stores = 0;
      timeSinceStart = 0;
      removeMisses = 0;
      removeHits = 0;
      misses = 0;
      hits = 0;
      evictions = 0;
      numberOfEntries = 0;
      averageWriteTime = 0;
      averageReadTime = 0;
      averageRemoveTime = 0;
      readWriteRatio = 0;
      hitRatio = 0;

      numberOfLocksHeld = 0;
      numberOfLocksAvailable = 0;

      invalidations = 0;
      activations = 0;
      passivations = 0;

      cacheLoaderLoads = 0;
      cacheLoaderMisses = 0;
      cacheWriterStores = 0;
   }

   @ManagedAttribute(description = "Total number of exclusive locks available in the cluster",
         displayName = "Cluster wide total number of locks",
         measurementType = MeasurementType.DYNAMIC,
         displayType = DisplayType.SUMMARY)
   @Override
   public int getNumberOfLocksAvailable(){
      if (isStatisticsEnabled()) {
         fetchClusterWideStatsIfNeeded();
         return numberOfLocksAvailable;
      } else {
         return -1;
      }
   }

   @ManagedAttribute(description = "Total number of locks held in the cluster",
         displayName = "Cluster wide total number of locks held",
         measurementType = MeasurementType.DYNAMIC,
         displayType = DisplayType.SUMMARY)
   @Override
   public int getNumberOfLocksHeld(){
      if (isStatisticsEnabled()) {
         fetchClusterWideStatsIfNeeded();
         return numberOfLocksHeld;
      } else {
         return -1;
      }
   }

   @ManagedAttribute(description = "The total number of invalidations in the cluster",
         displayName = "Cluster wide total number of invalidations",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   public long getInvalidations(){
      if (isStatisticsEnabled()) {
         fetchClusterWideStatsIfNeeded();
         return invalidations;
      } else {
         return -1;
      }
   }

   @ManagedAttribute(description = "The total number of activations in the cluster",
         displayName = "Cluster wide total number of activations",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   @Override
   public long getActivations() {
      if (isStatisticsEnabled()) {
         fetchClusterWideStatsIfNeeded();
         return activations;
      } else {
         return -1;
      }
   }

   @ManagedAttribute(description = "The total number of passivations in the cluster",
         displayName = "Cluster wide total number of passivations",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   @Override
   public long getPassivations(){
      if (isStatisticsEnabled()) {
         fetchClusterWideStatsIfNeeded();
         return passivations;
      } else {
         return -1;
      }
   }

   @ManagedAttribute(description = "The total number of cacheloader load operations in the cluster",
         displayName = "Cluster wide total number of cacheloader loads",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   @Override
   public long getCacheLoaderLoads(){
      if (isStatisticsEnabled()) {
         fetchClusterWideStatsIfNeeded();
         return cacheLoaderLoads;
      } else {
         return -1;
      }
   }

   @ManagedAttribute(description = "The total number of cacheloader load misses in the cluster",
         displayName = "Cluster wide total number of cacheloader misses",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   @Override
   public long getCacheLoaderMisses(){
      if (isStatisticsEnabled()) {
         fetchClusterWideStatsIfNeeded();
         return cacheLoaderMisses;
      } else {
         return -1;
      }
   }

   @ManagedAttribute(description = "The total number of cachestore store operations in the cluster",
         displayName = "Cluster wide total number of cachestore stores",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   @Override
   public long getStoreWrites(){
      if (isStatisticsEnabled()) {
         fetchClusterWideStatsIfNeeded();
         return cacheWriterStores;
      } else {
         return -1;
      }
   }

   protected boolean launchNewDistTask() {
      long duration = ts.timeDuration(statsUpdateTimestamp, ts.time(), TimeUnit.MILLISECONDS);
      return duration > staleStatsTreshold;
   }

   protected synchronized void fetchClusterWideStatsIfNeeded() {
      if (launchNewDistTask()) {
         List<Future<Map<String, Number>>> responseList = Collections.emptyList();
         try {
            responseList = des.submitEverywhere(new DistributedCacheStatsCallable());
            updateFieldsFromResponseMap(responseList);
         } catch (Exception e) {
            log.warn("Could not execute cluster wide cache stats operation ", e);
         } finally {
            statsUpdateTimestamp = ts.time();
         }
      }
   }

   private void updateFieldsFromResponseMap(List<Future<Map<String, Number>>> responseList) throws Exception {

      averageWriteTime = addLongAttributes(responseList, AVERAGE_WRITE_TIME) / responseList.size();
      averageReadTime = addLongAttributes(responseList, AVERAGE_READ_TIME) / responseList.size();
      averageRemoveTime = addLongAttributes(responseList, AVERAGE_REMOVE_TIME) / responseList.size();
      evictions = addLongAttributes(responseList, EVICTIONS);
      hits = addLongAttributes(responseList, HITS);
      misses = addLongAttributes(responseList, MISSES);
      numberOfEntries = addLongAttributes(responseList, NUMBER_OF_ENTRIES);
      removeHits = addLongAttributes(responseList, REMOVE_HITS);
      removeMisses = addLongAttributes(responseList, REMOVE_MISSES);
      stores = addLongAttributes(responseList, STORES);
      hitRatio = updateHitRatio(responseList);
      readWriteRatio = updateReadWriteRatio(responseList);
      timeSinceStart = updateTimeSinceStart(responseList);

      numberOfLocksHeld = addIntAttributes(responseList, NUMBER_OF_LOCKS_HELD);
      numberOfLocksAvailable = addIntAttributes(responseList, NUMBER_OF_LOCKS_AVAILABLE);

      invalidations = addLongAttributes(responseList, INVALIDATIONS);
      passivations = addLongAttributes(responseList, PASSIVATIONS);
      activations = addLongAttributes(responseList, ACTIVATIONS);

      cacheLoaderLoads = addLongAttributes(responseList, CACHE_LOADER_LOADS);
      cacheLoaderMisses = addLongAttributes(responseList, CACHE_LOADER_MISSES);
      cacheWriterStores = addLongAttributes(responseList, CACHE_WRITER_STORES);
   }

   private long addLongAttributes(List<Future<Map<String, Number>>> responseList, String attribute) throws Exception {
      long total = 0;
      for (Future<Map<String, Number>> f : responseList) {
         Map<String, Number> m = f.get();
         Number value = m.get(attribute);
         long longValue = value.longValue();
         if (longValue > -1) {
            total += longValue;
         }
      }
      return total;
   }

   private int addIntAttributes(List<Future<Map<String, Number>>> responseList, String attribute) throws Exception {
      int total = 0;
      for (Future<Map<String, Number>> f : responseList) {
         Map<String, Number> m = f.get();
         Number value = m.get(attribute);
         long intValue = value.intValue();
         if (intValue > -1) {
            total += intValue;
         }
      }
      return total;
   }

   private long updateTimeSinceStart(List<Future<Map<String, Number>>> responseList) throws Exception {
      long timeSinceStartMax = 0;
      for (Future<Map<String, Number>> f : responseList) {
         Map<String, Number> m = f.get();
         Number timeSinceStart = m.get(TIME_SINCE_START);
         if (timeSinceStart.longValue() > timeSinceStartMax) {
            timeSinceStartMax = timeSinceStart.longValue();
         }
      }
      return timeSinceStartMax;
   }

   private double updateReadWriteRatio(List<Future<Map<String, Number>>> responseList) throws Exception {
      long sumOfAllReads = 0;
      long sumOfAllWrites = 0;
      double rwRatio = 0;
      for (Future<Map<String, Number>> f : responseList) {
         Map<String, Number> m = f.get();
         Number hits = m.get(HITS);
         Number misses = m.get(MISSES);
         Number stores = m.get(STORES);
         sumOfAllReads += (hits.longValue() + misses.longValue());
         sumOfAllWrites += stores.longValue();
      }
      if (sumOfAllWrites > 0) {
         rwRatio = (double) sumOfAllReads / sumOfAllWrites;
      }
      return rwRatio;
   }

   private double updateHitRatio(List<Future<Map<String, Number>>> responseList) throws Exception {
      long totalHits = 0;
      long totalRetrievals = 0;
      double hitRatio = 0;
      for (Future<Map<String, Number>> f : responseList) {
         Map<String, Number> m = f.get();
         Number hits = m.get(HITS);
         Number misses = m.get(MISSES);
         totalHits += hits.longValue();
         totalRetrievals += (hits.longValue() + misses.longValue());
      }
      if (totalRetrievals > 0) {
         hitRatio = (double) totalHits / totalRetrievals;
      }
      return hitRatio;
   }

   public static <T extends CommandInterceptor> T getFirstInterceptorWhichExtends(AdvancedCache<?,?> cache, Class<T> interceptorClass) {

      List<CommandInterceptor> interceptorChain = cache.getInterceptorChain();
      for (CommandInterceptor interceptor : interceptorChain) {
         boolean isSubclass = interceptorClass.isAssignableFrom(interceptor.getClass());
         if (isSubclass) {
            return (T) interceptor;
         }
      }
      return null;
   }

   private static class DistributedCacheStatsCallable implements
         DistributedCallable<Object, Object, Map<String, Number>>, Serializable {

      /**
       *
       */
      private static final long serialVersionUID = -8400973931071456798L;

      private transient AdvancedCache<Object, Object> remoteCache;

      @Override
      public Map<String, Number> call() throws Exception {

         Map<String, Number> map = new HashMap<String, Number>();
         Stats stats = remoteCache.getStats();
         map.put(AVERAGE_READ_TIME, stats.getAverageReadTime());
         map.put(AVERAGE_WRITE_TIME, stats.getAverageWriteTime());
         map.put(AVERAGE_REMOVE_TIME, stats.getAverageRemoveTime());
         map.put(EVICTIONS, stats.getEvictions());
         map.put(HITS, stats.getHits());
         map.put(MISSES, stats.getMisses());
         if (isDistributed()) {
            map.put(NUMBER_OF_ENTRIES, stats.getCurrentNumberOfEntries() / numOwners());
         } else {
            map.put(NUMBER_OF_ENTRIES, stats.getCurrentNumberOfEntries());
         }
         map.put(STORES, stats.getStores());
         map.put(REMOVE_HITS, stats.getRemoveHits());
         map.put(REMOVE_MISSES, stats.getRemoveMisses());
         map.put(TIME_SINCE_START, stats.getTimeSinceStart());

         LockManager lockManager = remoteCache.getLockManager();
         map.put(NUMBER_OF_LOCKS_HELD, lockManager.getNumberOfLocksHeld());

         //number of locks available is not exposed through the LockManager interface
         map.put(NUMBER_OF_LOCKS_AVAILABLE, 0);


         //invalidations
         InvalidationInterceptor invalidationInterceptor = getFirstInterceptorWhichExtends(remoteCache,
               InvalidationInterceptor.class);
         if (invalidationInterceptor != null) {
            map.put(INVALIDATIONS, invalidationInterceptor.getInvalidations());
         } else {
            map.put(INVALIDATIONS, 0);
         }

         //passivations
         PassivationManager pManager = remoteCache.getComponentRegistry().getComponent(PassivationManager.class);
         if (pManager != null) {
            map.put(PASSIVATIONS, pManager.getPassivations());
         } else {
            map.put(PASSIVATIONS, 0);
         }

         //activations
         ActivationManager aManager = remoteCache.getComponentRegistry().getComponent(ActivationManager.class);
         if (pManager != null) {
            map.put(ACTIVATIONS, aManager.getActivationCount());
         } else {
            map.put(ACTIVATIONS, 0);
         }

         //cache loaders
         ActivationInterceptor aInterceptor = getFirstInterceptorWhichExtends(remoteCache, ActivationInterceptor.class);
         if (aInterceptor != null) {
            map.put(CACHE_LOADER_LOADS, aInterceptor.getCacheLoaderLoads());
            map.put(CACHE_LOADER_MISSES, aInterceptor.getCacheLoaderMisses());
         } else {
            map.put(CACHE_LOADER_LOADS, 0);
            map.put(CACHE_LOADER_MISSES, 0);
         }
         //cache store
         CacheWriterInterceptor interceptor = getFirstInterceptorWhichExtends(remoteCache, CacheWriterInterceptor.class);
         if (interceptor != null) {
            map.put(CACHE_WRITER_STORES, interceptor.getWritesToTheStores());
         } else {
            map.put(CACHE_WRITER_STORES, 0);
         }
         return map;
      }

      @Override
      public void setEnvironment(Cache<Object, Object> cache, Set<Object> inputKeys) {
         remoteCache = cache.getAdvancedCache();
      }

      private boolean isDistributed(){
         return remoteCache.getCacheConfiguration().clustering().cacheMode().isDistributed();
      }

      private int numOwners(){
         return remoteCache.getCacheConfiguration().clustering().hash().numOwners();
      }
   }
}
