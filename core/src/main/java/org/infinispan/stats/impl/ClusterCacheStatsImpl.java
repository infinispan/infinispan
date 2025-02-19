package org.infinispan.stats.impl;

import static org.infinispan.stats.impl.StatKeys.ACTIVATIONS;
import static org.infinispan.stats.impl.StatKeys.APPROXIMATE_ENTRIES;
import static org.infinispan.stats.impl.StatKeys.APPROXIMATE_ENTRIES_IN_MEMORY;
import static org.infinispan.stats.impl.StatKeys.APPROXIMATE_ENTRIES_UNIQUE;
import static org.infinispan.stats.impl.StatKeys.AVERAGE_READ_TIME;
import static org.infinispan.stats.impl.StatKeys.AVERAGE_READ_TIME_NANOS;
import static org.infinispan.stats.impl.StatKeys.AVERAGE_REMOVE_TIME;
import static org.infinispan.stats.impl.StatKeys.AVERAGE_REMOVE_TIME_NANOS;
import static org.infinispan.stats.impl.StatKeys.AVERAGE_WRITE_TIME;
import static org.infinispan.stats.impl.StatKeys.AVERAGE_WRITE_TIME_NANOS;
import static org.infinispan.stats.impl.StatKeys.CACHE_LOADER_LOADS;
import static org.infinispan.stats.impl.StatKeys.CACHE_LOADER_MISSES;
import static org.infinispan.stats.impl.StatKeys.CACHE_WRITER_STORES;
import static org.infinispan.stats.impl.StatKeys.DATA_MEMORY_USED;
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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.context.Flag;
import org.infinispan.eviction.impl.ActivationManager;
import org.infinispan.eviction.impl.PassivationManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.impl.CacheLoaderInterceptor;
import org.infinispan.interceptors.impl.CacheMgmtInterceptor;
import org.infinispan.interceptors.impl.CacheWriterInterceptor;
import org.infinispan.interceptors.impl.InvalidationInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.jmx.annotations.Units;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.LocalModeAddress;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.stats.ClusterCacheStats;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.function.TriConsumer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

@MBean(objectName = ClusterCacheStats.OBJECT_NAME, description = "General cluster statistics such as timings, hit/miss ratio, etc. for a cache.")
@Scope(Scopes.NAMED_CACHE)
public class ClusterCacheStatsImpl extends AbstractClusterStats implements ClusterCacheStats {

   private static final String[] LONG_ATTRIBUTES = {EVICTIONS, HITS, MISSES, OFF_HEAP_MEMORY_USED, REMOVE_HITS,
         REMOVE_MISSES, INVALIDATIONS, PASSIVATIONS, ACTIVATIONS, CACHE_LOADER_LOADS, CACHE_LOADER_MISSES, CACHE_WRITER_STORES,
         STORES, DATA_MEMORY_USED};

   private static final Log log = LogFactory.getLog(ClusterCacheStatsImpl.class);

   @Inject AdvancedCache<?, ?> cache;
   @Inject Configuration cacheConfiguration;
   @Inject GlobalConfiguration globalConfiguration;

   ClusterExecutor clusterExecutor;

   private double readWriteRatio;
   private double hitRatio;

   public ClusterCacheStatsImpl() {
      super(log);
   }

   public void start() {
      this.statisticsEnabled = cacheConfiguration.statistics().enabled();
      this.clusterExecutor = SecurityActions.getClusterExecutor(cache);
   }

   @Override
   void updateStats() {
      if (clusterExecutor == null) {
         // Attempt to retrieve cluster stats before component has been initialized
         return;
      }
      ConcurrentMap<Address, Map<String, Number>> resultMap = new ConcurrentHashMap<>();
      TriConsumer<Address, Map<String, Number>, Throwable> triConsumer = (a, v, t) -> {
         if (t != null) {
            if (t instanceof CacheConfigurationException || t instanceof IllegalLifecycleStateException) {
               log.tracef(t,"Exception encountered on %s whilst trying to calculate stats for cache %s", a, cache.getName());
               return;
            }
            throw new CacheException(t);
         }
         if (a == null) {
            // Local cache manager reports null for address
            a = LocalModeAddress.INSTANCE;
         }
         if (!v.isEmpty())
            resultMap.put(a, v);
      };
      boolean accurateSize = globalConfiguration.metrics().accurateSize();
      DistributedCacheStatsCallable task = new DistributedCacheStatsCallable(cache.getName(), accurateSize);
      try {
         CompletableFuture<Void> future = clusterExecutor.submitConsumer(task, triConsumer);
         future.join();

         Collection<Map<String, Number>> responseList = resultMap.values();

         for (String att : LONG_ATTRIBUTES)
            putLongAttributes(responseList, att);

         putLongAttributesAverage(responseList, AVERAGE_WRITE_TIME);
         putLongAttributesAverage(responseList, AVERAGE_WRITE_TIME_NANOS);
         putLongAttributesAverage(responseList, AVERAGE_READ_TIME);
         putLongAttributesAverage(responseList, AVERAGE_READ_TIME_NANOS);
         putLongAttributesAverage(responseList, AVERAGE_REMOVE_TIME);
         putLongAttributesAverage(responseList, AVERAGE_REMOVE_TIME_NANOS);
         putLongAttributesAverage(responseList, OFF_HEAP_MEMORY_USED);

         putIntAttributes(responseList, NUMBER_OF_LOCKS_HELD);
         putIntAttributes(responseList, NUMBER_OF_LOCKS_AVAILABLE);
         putIntAttributesMax(responseList, REQUIRED_MIN_NODES);

         putLongAttributes(responseList, APPROXIMATE_ENTRIES);
         putLongAttributes(responseList, APPROXIMATE_ENTRIES_IN_MEMORY);
         putLongAttributes(responseList, APPROXIMATE_ENTRIES_UNIQUE);

         if (accurateSize) {
            // Count each entry only once
            long numberOfEntriesInMemory = cache.withFlags(Flag.SKIP_CACHE_LOAD).size();
            statsMap.put(NUMBER_OF_ENTRIES_IN_MEMORY, numberOfEntriesInMemory);
            int numberOfEntries = cache.size();
            statsMap.put(NUMBER_OF_ENTRIES, (long) numberOfEntries);
         } else {
            statsMap.put(NUMBER_OF_ENTRIES_IN_MEMORY, -1L);
            statsMap.put(NUMBER_OF_ENTRIES, -1L);
         }

         updateTimeSinceStart(responseList);
         updateRatios(responseList);
      } catch (CompletionException e) {
         log.debug("Error while collecting cluster-wide cache stats", e.getCause());
      }
   }

   // -------------------------------------------- JMX information -----------------------------------------------

   @Override
   @ManagedAttribute(description = "Average number of milliseconds for a read operation on the cache across the cluster",
         displayName = "Cluster-wide total average read time (ms)",
         units = Units.MILLISECONDS,
         clusterWide = true
   )
   public long getAverageReadTime() {
      return getStatAsLong(AVERAGE_READ_TIME);
   }

   @Override
   @ManagedAttribute(description = "Average number of nanoseconds for a read operation on the cache across the cluster",
         displayName = "Cluster-wide average read time (ns)",
         units = Units.NANOSECONDS,
         clusterWide = true
   )
   public long getAverageReadTimeNanos() {
      return getStatAsLong(AVERAGE_READ_TIME_NANOS);
   }

   @Override
   @ManagedAttribute(description = "Average number of milliseconds for a remove operation in the cache across the cluster",
         displayName = "Cluster-wide average remove time (ms)",
         units = Units.MILLISECONDS,
         clusterWide = true
   )
   public long getAverageRemoveTime() {
      return getStatAsLong(AVERAGE_REMOVE_TIME);
   }

   @Override
   @ManagedAttribute(description = "Average number of nanoseconds for a remove operation in the cache across the cluster",
         displayName = "Cluster-wide average remove time (ns)",
         units = Units.NANOSECONDS,
         clusterWide = true
   )
   public long getAverageRemoveTimeNanos() {
      return getStatAsLong(AVERAGE_REMOVE_TIME_NANOS);
   }

   @Override
   @ManagedAttribute(description = "Average number of milliseconds for a write operation in the cache across the cluster",
         displayName = "Cluster-wide average write time (ms)",
         units = Units.MILLISECONDS,
         clusterWide = true
   )
   public long getAverageWriteTime() {
      return getStatAsLong(AVERAGE_WRITE_TIME);
   }

   @Override
   @ManagedAttribute(description = "Average number of nanoseconds for a write operation in the cache across the cluster",
         displayName = "Cluster-wide average write time (ns)",
         units = Units.NANOSECONDS,
         clusterWide = true
   )
   public long getAverageWriteTimeNanos() {
      return getStatAsLong(AVERAGE_WRITE_TIME_NANOS);
   }

   @ManagedAttribute(description = "Minimum number of nodes to avoid losing data",
         displayName = "Required minimum number of nodes",
         clusterWide = true
   )
   @Override
   public int getRequiredMinimumNumberOfNodes() {
      return getStatAsInt(REQUIRED_MIN_NODES);
   }

   @ManagedAttribute(description = "Total number of cache eviction operations across the cluster",
         displayName = "Cluster-wide total number of cache evictions",
         measurementType = MeasurementType.TRENDSUP,
         clusterWide = true
   )
   @Override
   public long getEvictions() {
      return getStatAsLong(EVICTIONS);
   }

   @ManagedAttribute(description = "Total number of cache read hits across the cluster",
         displayName = "Cluster-wide total number of cache read hits",
         measurementType = MeasurementType.TRENDSUP,
         clusterWide = true
   )
   @Override
   public long getHits() {
      return getStatAsLong(HITS);
   }

   @ManagedAttribute(description = "Percentage hit/(hit+miss) ratio for this cache",
         displayName = "Cluster-wide hit ratio",
         units = Units.PERCENTAGE,
         clusterWide = true
   )
   @Override
   public double getHitRatio() {
      if (isStatisticsEnabled()) {
         fetchStatsIfNeeded();
         return hitRatio;
      } else {
         return -1;
      }
   }

   @Override
   @ManagedAttribute(description = "Total number of cache read misses",
         displayName = "Cluster-wide number of cache read misses",
         measurementType = MeasurementType.TRENDSUP,
         clusterWide = true
   )
   public long getMisses() {
      return getStatAsLong(MISSES);
   }

   @ManagedAttribute(description = "Approximate number of entry replicas in the cache across the cluster, including passivated entries",
         displayName = "Cluster-wide approximate number of entry replicas",
         clusterWide = true
   )
   public long getApproximateEntries() {
      return getStatAsLong(APPROXIMATE_ENTRIES);
   }

   @Override
   @ManagedAttribute(description = "Approximate number of entry replicas in memory across the cluster",
         displayName = "Cluster-wide approximate number of entry replicas in memory",
         clusterWide = true
   )
   public long getApproximateEntriesInMemory() {
      return getStatAsLong(APPROXIMATE_ENTRIES_IN_MEMORY);
   }

   @Override
   @ManagedAttribute(description = "Approximate number of unique entries in the cache across the cluster, ignoring duplicate replicas",
         displayName = "Cluster-wide approximate number of unique entries",
         clusterWide = true
   )
   public long getApproximateEntriesUnique() {
      return getStatAsLong(APPROXIMATE_ENTRIES_UNIQUE);
   }

   @ManagedAttribute(description = "Current number of entries in the cache across the cluster, including passivated entries",
         displayName = "Cluster-wide number of current cache entries",
         clusterWide = true
   )
   public int getNumberOfEntries() {
      return getStatAsInt(NUMBER_OF_ENTRIES);
   }

   @Override
   @ManagedAttribute(description = "Current number of entries in memory across the cluster",
         displayName = "Cluster-wide number of entries in memory",
         clusterWide = true
   )
   public int getCurrentNumberOfEntriesInMemory() {
      return getStatAsInt(NUMBER_OF_ENTRIES_IN_MEMORY);
   }

   @ManagedAttribute(description = "Cluster-wide read/writes ratio for the cache",
         displayName = "Cluster-wide read/write ratio",
         units = Units.PERCENTAGE,
         clusterWide = true
   )
   @Override
   public double getReadWriteRatio() {
      if (isStatisticsEnabled()) {
         fetchStatsIfNeeded();
         return readWriteRatio;
      } else {
         return -1;
      }
   }

   @ManagedAttribute(description = "Cluster-wide total number of cache removal hits",
         displayName = "Cluster-wide total number of cache removal hits",
         measurementType = MeasurementType.TRENDSUP,
         clusterWide = true
   )
   @Override
   public long getRemoveHits() {
      return getStatAsLong(REMOVE_HITS);
   }

   @ManagedAttribute(description = "Cluster-wide total number of cache removals where keys were not found",
         displayName = "Cluster-wide total number of cache removal misses",
         measurementType = MeasurementType.TRENDSUP,
         clusterWide = true
   )
   @Override
   public long getRemoveMisses() {
      return getStatAsLong(REMOVE_MISSES);
   }

   @ManagedAttribute(description = "Cluster-wide total number of cache put operations",
         displayName = "Cluster-wide total number of cache puts",
         measurementType = MeasurementType.TRENDSUP,
         clusterWide = true
   )
   @Override
   public long getStores() {
      return getStatAsLong(STORES);
   }

   @ManagedAttribute(description = "Number of seconds since the first cache node started",
         displayName = "Number of seconds since the first cache node started",
         measurementType = MeasurementType.TRENDSUP,
         clusterWide = true
   )
   @Override
   public long getTimeSinceStart() {
      return getStatAsLong(TIME_SINCE_START);
   }

   @Override
   public int getCurrentNumberOfEntries() {
      return getNumberOfEntries();
   }

   @ManagedAttribute(
         description = "Amount in bytes of memory used across the cluster for entries in this cache with eviction",
         displayName = "Cluster-wide memory used by eviction",
         clusterWide = true
   )
   @Override
   public long getDataMemoryUsed() {
      return getStatAsLong(DATA_MEMORY_USED);
   }

   @ManagedAttribute(
         description = "Amount in bytes of off-heap memory used across the cluster for this cache",
         displayName = "Cluster-wide off-heap memory used",
         clusterWide = true
   )
   @Override
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

   @ManagedAttribute(description = "Current number of exclusive locks available across the cluster",
         displayName = "Cluster-wide number of locks available",
         clusterWide = true
   )
   @Override
   public int getNumberOfLocksAvailable() {
      return getStatAsInt(NUMBER_OF_LOCKS_AVAILABLE);
   }

   @ManagedAttribute(description = "Current number of locks held across the cluster",
         displayName = "Cluster-wide number of locks held",
         clusterWide = true
   )
   @Override
   public int getNumberOfLocksHeld() {
      return getStatAsInt(NUMBER_OF_LOCKS_HELD);
   }

   @Override
   @ManagedAttribute(description = "The total number of invalidations in the cluster",
         displayName = "Cluster-wide total number of invalidations",
         measurementType = MeasurementType.TRENDSUP,
         clusterWide = true
   )
   public long getInvalidations() {
      return getStatAsLong(INVALIDATIONS);
   }

   @ManagedAttribute(description = "The total number of activations across the cluster",
         displayName = "Cluster-wide total number of activations",
         measurementType = MeasurementType.TRENDSUP,
         clusterWide = true
   )
   @Override
   public long getActivations() {
      return getStatAsLong(ACTIVATIONS);
   }

   @ManagedAttribute(description = "The total number of passivations across the cluster",
         displayName = "Cluster-wide total number of passivations",
         measurementType = MeasurementType.TRENDSUP,
         clusterWide = true
   )
   @Override
   public long getPassivations() {
      return getStatAsLong(PASSIVATIONS);
   }

   @ManagedAttribute(description = "The total number of persistence load operations in the cluster",
         displayName = "Cluster-wide total number of persistence loads",
         measurementType = MeasurementType.TRENDSUP,
         clusterWide = true
   )
   @Override
   public long getCacheLoaderLoads() {
      return getStatAsLong(CACHE_LOADER_LOADS);
   }

   @ManagedAttribute(description = "The total number of cacheloader load misses in the cluster",
         displayName = "Cluster-wide total number of cacheloader misses",
         measurementType = MeasurementType.TRENDSUP,
         clusterWide = true
   )
   @Override
   public long getCacheLoaderMisses() {
      return getStatAsLong(CACHE_LOADER_MISSES);
   }

   @ManagedAttribute(description = "The total number of cachestore store operations in the cluster",
         displayName = "Cluster-wide total number of cachestore stores",
         measurementType = MeasurementType.TRENDSUP,
         clusterWide = true
   )
   @Override
   public long getStoreWrites() {
      return getStatAsLong(CACHE_WRITER_STORES);
   }

   private void updateTimeSinceStart(Collection<Map<String, Number>> responseList) {
      long timeSinceStartMax = 0;
      for (Map<String, Number> m : responseList) {
         Number timeSinceStart = m.get(TIME_SINCE_START);
         if (timeSinceStart.longValue() > timeSinceStartMax) {
            timeSinceStartMax = timeSinceStart.longValue();
         }
      }
      statsMap.put(TIME_SINCE_START, timeSinceStartMax);
   }

   private void updateRatios(Collection<Map<String, Number>> responseList) {
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
      return ComponentRegistry.of(cache).getInterceptorChain().wired().findInterceptorExtending(interceptorClass);
   }

   @ProtoTypeId(ProtoStreamTypeIds.DISTRIBUTED_CACHE_STATS_CALLABLE)
   public static class DistributedCacheStatsCallable implements Function<EmbeddedCacheManager, Map<String, Number>> {

      @ProtoField(1)
      final String cacheName;

      @ProtoField(2)
      final boolean accurateSize;

      @ProtoFactory
      DistributedCacheStatsCallable(String cacheName, boolean accurateSize) {
         this.cacheName = cacheName;
         this.accurateSize = accurateSize;
      }

      @Override
      public Map<String, Number> apply(EmbeddedCacheManager embeddedCacheManager) {
         if (!embeddedCacheManager.cacheExists(cacheName))
            return Collections.emptyMap();

         AdvancedCache<Object, Object> remoteCache =
               SecurityActions.getUnwrappedCache(embeddedCacheManager.getCache(cacheName)).getAdvancedCache();
         CacheMgmtInterceptor stats = getFirstInterceptorWhichExtends(remoteCache, CacheMgmtInterceptor.class);

         Map<String, Number> map = new HashMap<>();
         map.put(AVERAGE_READ_TIME, stats.getAverageReadTime());
         map.put(AVERAGE_READ_TIME_NANOS, stats.getAverageReadTimeNanos());
         map.put(AVERAGE_WRITE_TIME, stats.getAverageWriteTime());
         map.put(AVERAGE_WRITE_TIME_NANOS, stats.getAverageWriteTimeNanos());
         map.put(AVERAGE_REMOVE_TIME, stats.getAverageRemoveTime());
         map.put(AVERAGE_REMOVE_TIME_NANOS, stats.getAverageRemoveTimeNanos());
         map.put(EVICTIONS, stats.getEvictions());
         map.put(HITS, stats.getHits());
         map.put(MISSES, stats.getMisses());

         map.put(APPROXIMATE_ENTRIES, stats.getApproximateEntries());
         map.put(APPROXIMATE_ENTRIES_IN_MEMORY, stats.getApproximateEntriesInMemory());
         map.put(APPROXIMATE_ENTRIES_UNIQUE, stats.getApproximateEntriesUnique());

         map.put(DATA_MEMORY_USED, stats.getDataMemoryUsed());
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
         PassivationManager pManager =  ComponentRegistry.componentOf(remoteCache, PassivationManager.class);
         if (pManager != null) {
            map.put(PASSIVATIONS, pManager.getPassivations());
         } else {
            map.put(PASSIVATIONS, 0);
         }

         //activations
         ActivationManager aManager =  ComponentRegistry.componentOf(remoteCache, ActivationManager.class);
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
   }
}
