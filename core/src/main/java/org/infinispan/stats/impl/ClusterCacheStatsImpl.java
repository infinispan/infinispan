package org.infinispan.stats.impl;

import static org.infinispan.stats.impl.StatKeys.ACTIVATIONS;
import static org.infinispan.stats.impl.StatKeys.AVERAGE_READ_TIME;
import static org.infinispan.stats.impl.StatKeys.AVERAGE_REMOVE_TIME;
import static org.infinispan.stats.impl.StatKeys.AVERAGE_WRITE_TIME;
import static org.infinispan.stats.impl.StatKeys.CACHE_LOADER_LOADS;
import static org.infinispan.stats.impl.StatKeys.CACHE_LOADER_MISSES;
import static org.infinispan.stats.impl.StatKeys.CACHE_WRITER_STORES;
import static org.infinispan.stats.impl.StatKeys.EVICTIONS;
import static org.infinispan.stats.impl.StatKeys.HITS;
import static org.infinispan.stats.impl.StatKeys.INVALIDATIONS;
import static org.infinispan.stats.impl.StatKeys.MISSES;
import static org.infinispan.stats.impl.StatKeys.NUMBER_OF_ENTRIES;
import static org.infinispan.stats.impl.StatKeys.NUMBER_OF_ENTRIES_IN_MEMORY;
import static org.infinispan.stats.impl.StatKeys.NUMBER_OF_LOCKS_AVAILABLE;
import static org.infinispan.stats.impl.StatKeys.NUMBER_OF_LOCKS_HELD;
import static org.infinispan.stats.impl.StatKeys.OFF_HEAP_MEMORY_USED;
import static org.infinispan.stats.impl.StatKeys.PASSIVATIONS;
import static org.infinispan.stats.impl.StatKeys.REMOVE_HITS;
import static org.infinispan.stats.impl.StatKeys.REMOVE_MISSES;
import static org.infinispan.stats.impl.StatKeys.REQUIRED_MIN_NODES;
import static org.infinispan.stats.impl.StatKeys.STORES;
import static org.infinispan.stats.impl.StatKeys.TIME_SINCE_START;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.eviction.ActivationManager;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.impl.CacheLoaderInterceptor;
import org.infinispan.interceptors.impl.CacheWriterInterceptor;
import org.infinispan.interceptors.impl.InvalidationInterceptor;
import org.infinispan.jmx.annotations.DisplayType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.jmx.annotations.Units;
import org.infinispan.stats.ClusterCacheStats;
import org.infinispan.stats.Stats;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

@MBean(objectName = "ClusterCacheStats", description = "General cluster statistics such as timings, hit/miss ratio, etc.")
public class ClusterCacheStatsImpl extends AbstractClusterStats implements ClusterCacheStats {

   private static String[] LONG_ATTRIBUTES = new String[]{EVICTIONS, HITS, MISSES, OFF_HEAP_MEMORY_USED, REMOVE_HITS,
         REMOVE_MISSES, INVALIDATIONS, PASSIVATIONS, ACTIVATIONS, CACHE_LOADER_LOADS, CACHE_LOADER_MISSES, CACHE_WRITER_STORES,
         STORES};

   private static final Log log = LogFactory.getLog(ClusterCacheStatsImpl.class);

   private DefaultExecutorService des;
   private AdvancedCache cache;
   private double readWriteRatio;
   private double hitRatio;

   public ClusterCacheStatsImpl() {
      super(log);
   }

   @Inject
   public void injectDependencies(Cache<?, ?> cache, Configuration configuration) {
      this.cache = cache.getAdvancedCache();
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

   @Override
   void updateStats() throws Exception {
      List<Map<String, Number>> responseList = new ArrayList<>();
      List<CompletableFuture<Map<String, Number>>> responseFutures = des.submitEverywhere(new DistributedCacheStatsCallable());
      for (CompletableFuture<Map<String, Number>> cf : responseFutures)
         responseList.add(cf.get());

      for (String att : LONG_ATTRIBUTES)
         putLongAttributes(responseList, att);

      putLongAttributesAverage(responseList, AVERAGE_WRITE_TIME);
      putLongAttributesAverage(responseList, AVERAGE_READ_TIME);
      putLongAttributesAverage(responseList, AVERAGE_REMOVE_TIME);
      putLongAttributesAverage(responseList, OFF_HEAP_MEMORY_USED);

      putIntAttributes(responseList, NUMBER_OF_LOCKS_HELD);
      putIntAttributes(responseList, NUMBER_OF_LOCKS_AVAILABLE);
      putIntAttributesMax(responseList, REQUIRED_MIN_NODES);

      long numberOfEntriesInMemory = getCacheMode(cache).isReplicated() ?
            cache.getStats().getCurrentNumberOfEntriesInMemory() :
            (long) addDoubleAttributes(responseList, NUMBER_OF_ENTRIES_IN_MEMORY);
      statsMap.put(NUMBER_OF_ENTRIES_IN_MEMORY, numberOfEntriesInMemory);
      statsMap.put(NUMBER_OF_ENTRIES, cache.size());

      updateTimeSinceStart(responseList);
      updateRatios(responseList);
   }

   // -------------------------------------------- JMX information -----------------------------------------------

   @Override
   @ManagedAttribute(description = "Cluster wide total average number of milliseconds for a read operation on the cache",
         displayName = "Cluster wide total average read time",
         units = Units.MILLISECONDS,
         displayType = DisplayType.SUMMARY)
   public long getAverageReadTime() {
      return getStatAsLong(AVERAGE_READ_TIME);
   }

   @Override
   @ManagedAttribute(description = "Cluster wide total average number of milliseconds for a remove operation in the cache",
         displayName = "Cluster wide total average remove time",
         units = Units.MILLISECONDS,
         displayType = DisplayType.SUMMARY)
   public long getAverageRemoveTime() {
      return getStatAsLong(AVERAGE_REMOVE_TIME);
   }

   @Override
   public int getRequiredMinimumNumberOfNodes() {
      return getStatAsInt(REQUIRED_MIN_NODES);
   }

   @Override
   @ManagedAttribute(description = "Cluster wide average number of milliseconds for a write operation in the cache",
         displayName = "Cluster wide average write time",
         units = Units.MILLISECONDS,
         displayType = DisplayType.SUMMARY)
   public long getAverageWriteTime() {
      return getStatAsLong(AVERAGE_WRITE_TIME);
   }

   @ManagedAttribute(description = "Cluster wide total number of cache eviction operations",
         displayName = "Cluster wide total number of cache evictions",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   @Override
   public long getEvictions() {
      return getStatAsLong(EVICTIONS);
   }

   @ManagedAttribute(description = "Cluster wide total number of cache attribute hits",
         displayName = "Cluster wide total number of cache hits",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   @Override
   public long getHits() {
      return getStatAsLong(HITS);
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

   @Override
   @ManagedAttribute(description = "Cluster wide total number of cache attribute misses",
         displayName = "Cluster wide total number of cache misses",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   public long getMisses() {
      return getStatAsLong(MISSES);
   }

   @ManagedAttribute(description = "Cluster wide total number of entries currently in the cache, including passivated entries",
         displayName = "Cluster wide total number of current cache entries",
         displayType = DisplayType.SUMMARY)
   public int getNumberOfEntries() {
      return getStatAsInt(NUMBER_OF_ENTRIES);
   }

   @Override
   @ManagedAttribute(description = "Cluster wide total number of entries currently stored in-memory",
         displayName = "Cluster wide total number of in-memory cache entries",
         displayType = DisplayType.SUMMARY)
   public int getCurrentNumberOfEntriesInMemory() {
      return getStatAsInt(NUMBER_OF_ENTRIES_IN_MEMORY);
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
      return getStatAsLong(REMOVE_HITS);
   }

   @ManagedAttribute(description = "Cluster wide total number of cache removals where keys were not found",
         displayName = "Cluster wide total number of cache removal misses",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   @Override
   public long getRemoveMisses() {
      return getStatAsLong(REMOVE_MISSES);
   }

   @ManagedAttribute(description = "Cluster wide total number of cache attribute put operations",
         displayName = "Cluster wide total number of cache puts",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   @Override
   public long getStores() {
      return getStatAsLong(STORES);
   }

   @ManagedAttribute(description = "Number of seconds since the first cache node started",
         displayName = "Number of seconds since the first cache node started",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   @Override
   public long getTimeSinceStart() {
      return getStatAsLong(TIME_SINCE_START);
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
   @ManagedAttribute(
         description = "Amount in bytes of off-heap memory used across the cluster for this cache",
         displayName = "Cluster wide off-sheap memory used",
         displayType = DisplayType.SUMMARY
   )
   public long getOffHeapMemoryUsed() {
      return getStatAsLong(OFF_HEAP_MEMORY_USED);
   }

   @Override
   public long getRetrievals() {
      return getHits() + getMisses();
   }

   @Override
   public void reset() {
      super.reset();
      readWriteRatio = 0;
      hitRatio = 0;
   }

   @ManagedAttribute(description = "Total number of exclusive locks available in the cluster",
         displayName = "Cluster wide total number of locks",
         measurementType = MeasurementType.DYNAMIC,
         displayType = DisplayType.SUMMARY)
   @Override
   public int getNumberOfLocksAvailable() {
      return getStatAsInt(NUMBER_OF_LOCKS_AVAILABLE);
   }

   @ManagedAttribute(description = "Total number of locks held in the cluster",
         displayName = "Cluster wide total number of locks held",
         measurementType = MeasurementType.DYNAMIC,
         displayType = DisplayType.SUMMARY)
   @Override
   public int getNumberOfLocksHeld() {
      return getStatAsInt(NUMBER_OF_LOCKS_HELD);
   }

   @Override
   @ManagedAttribute(description = "The total number of invalidations in the cluster",
         displayName = "Cluster wide total number of invalidations",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   public long getInvalidations() {
      return getStatAsLong(INVALIDATIONS);
   }

   @ManagedAttribute(description = "The total number of activations in the cluster",
         displayName = "Cluster wide total number of activations",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   @Override
   public long getActivations() {
      return getStatAsLong(ACTIVATIONS);
   }

   @ManagedAttribute(description = "The total number of passivations in the cluster",
         displayName = "Cluster wide total number of passivations",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   @Override
   public long getPassivations() {
      return getStatAsLong(PASSIVATIONS);
   }

   @ManagedAttribute(description = "The total number of cacheloader load operations in the cluster",
         displayName = "Cluster wide total number of cacheloader loads",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   @Override
   public long getCacheLoaderLoads() {
      return getStatAsLong(CACHE_LOADER_LOADS);
   }

   @ManagedAttribute(description = "The total number of cacheloader load misses in the cluster",
         displayName = "Cluster wide total number of cacheloader misses",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   @Override
   public long getCacheLoaderMisses() {
      return getStatAsLong(CACHE_LOADER_MISSES);
   }

   @ManagedAttribute(description = "The total number of cachestore store operations in the cluster",
         displayName = "Cluster wide total number of cachestore stores",
         measurementType = MeasurementType.TRENDSUP,
         displayType = DisplayType.SUMMARY)
   @Override
   public long getStoreWrites() {
      return getStatAsLong(CACHE_WRITER_STORES);
   }

   private void updateTimeSinceStart(List<Map<String, Number>> responseList) {
      long timeSinceStartMax = 0;
      for (Map<String, Number> m : responseList) {
         Number timeSinceStart = m.get(TIME_SINCE_START);
         if (timeSinceStart.longValue() > timeSinceStartMax) {
            timeSinceStartMax = timeSinceStart.longValue();
         }
      }
      statsMap.put(TIME_SINCE_START, timeSinceStartMax);
   }

   private void updateRatios(List<Map<String, Number>> responseList) {
      long totalHits = 0;
      long totalRetrievals = 0;
      long sumOfAllReads = 0;
      long sumOfAllWrites = 0;
      for (Map<String, Number> m : responseList) {
         long hits = m.get(HITS).longValue();
         long misses = m.get(MISSES).longValue();
         totalHits += hits;
         sumOfAllReads += (totalHits + misses);
         sumOfAllWrites += m.get(STORES).longValue();
         totalRetrievals += (hits + misses);
      }
      this.hitRatio = totalRetrievals > 0 ? (double) totalHits / totalRetrievals : 0;
      this.readWriteRatio = sumOfAllWrites > 0 ? (double) sumOfAllReads / sumOfAllWrites : 0;
   }

   private static <T extends AsyncInterceptor> T getFirstInterceptorWhichExtends(AdvancedCache<?, ?> cache,
                                                                                 Class<T> interceptorClass) {
      return interceptorClass
            .cast(cache.getAsyncInterceptorChain().findInterceptorExtending(interceptorClass));
   }

   private static CacheMode getCacheMode(Cache cache) {
      return cache.getCacheConfiguration().clustering().cacheMode();
   }

   private static class DistributedCacheStatsCallable implements
         DistributedCallable<Object, Object, Map<String, Number>>, Serializable {

      private static final long serialVersionUID = -8400973931071456798L;
      private transient AdvancedCache<Object, Object> remoteCache;

      @Override
      public Map<String, Number> call() throws Exception {
         Map<String, Number> map = new HashMap<>();
         Stats stats = remoteCache.getStats();
         map.put(AVERAGE_READ_TIME, stats.getAverageReadTime());
         map.put(AVERAGE_WRITE_TIME, stats.getAverageWriteTime());
         map.put(AVERAGE_REMOVE_TIME, stats.getAverageRemoveTime());
         map.put(EVICTIONS, stats.getEvictions());
         map.put(HITS, stats.getHits());
         map.put(MISSES, stats.getMisses());

         if (!getCacheMode(remoteCache).isReplicated()) {
            double numberOfEntriesInMemory = stats.getCurrentNumberOfEntriesInMemory();
            numberOfEntriesInMemory /= remoteCache.getCacheConfiguration().clustering().hash().numOwners();
            map.put(NUMBER_OF_ENTRIES_IN_MEMORY, numberOfEntriesInMemory);
         }

         map.put(OFF_HEAP_MEMORY_USED, stats.getOffHeapMemoryUsed());
         map.put(REQUIRED_MIN_NODES, stats.getRequiredMinimumNumberOfNodes());
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
         if (aManager != null) {
            map.put(ACTIVATIONS, aManager.getActivationCount());
         } else {
            map.put(ACTIVATIONS, 0);
         }

         //cache loaders
         CacheLoaderInterceptor
               aInterceptor = getFirstInterceptorWhichExtends(remoteCache, CacheLoaderInterceptor.class);
         if (aInterceptor != null) {
            map.put(CACHE_LOADER_LOADS, aInterceptor.getCacheLoaderLoads());
            map.put(CACHE_LOADER_MISSES, aInterceptor.getCacheLoaderMisses());
         } else {
            map.put(CACHE_LOADER_LOADS, 0);
            map.put(CACHE_LOADER_MISSES, 0);
         }
         //cache store
         CacheWriterInterceptor
               interceptor = getFirstInterceptorWhichExtends(remoteCache, CacheWriterInterceptor.class);
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
   }
}
