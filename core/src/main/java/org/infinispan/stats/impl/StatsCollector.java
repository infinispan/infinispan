package org.infinispan.stats.impl;

import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.offheap.OffHeapMemoryAllocator;
import org.infinispan.context.Flag;
import org.infinispan.eviction.EvictionType;
import org.infinispan.factories.AbstractNamedCacheComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.SurvivesRestarts;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.interceptors.impl.CacheMgmtInterceptor;
import org.infinispan.jmx.JmxStatisticsExposer;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.jmx.annotations.MeasurementType;
import org.infinispan.jmx.annotations.Units;
import org.infinispan.stats.Stats;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@MBean(objectName = "Statistics", description = "General statistics such as timings, hit/miss ratio, etc.")
@Scope(Scopes.NAMED_CACHE)
public final class StatsCollector implements Stats, JmxStatisticsExposer {
   private final LongAdder hitTimes = new LongAdder();
   private final LongAdder missTimes = new LongAdder();
   private final LongAdder storeTimes = new LongAdder();
   private final LongAdder removeTimes = new LongAdder();
   private final LongAdder hits = new LongAdder();
   private final LongAdder misses = new LongAdder();
   private final LongAdder stores = new LongAdder();
   private final LongAdder evictions = new LongAdder();
   private final AtomicLong startNanoseconds = new AtomicLong(0);
   private final AtomicLong resetNanoseconds = new AtomicLong(0);
   private final LongAdder removeHits = new LongAdder();
   private final LongAdder removeMisses = new LongAdder();

   @Inject ComponentRef<AdvancedCache> cache;
   @Inject TimeService timeService;
   @Inject InternalDataContainer dataContainer;
   @Inject OffHeapMemoryAllocator allocator;
   @Inject Configuration configuration;
   @Inject ComponentRegistry componentRegistry;

   @Start
   public void start() {
      statisticsEnabled = configuration.statistics().enabled();
   }

   // probably it's not *that* important to have perfect stats to make this variable volatile
   private boolean statisticsEnabled = false;

   @ManagedAttribute(
         description = "Number of cache attribute hits",
         displayName = "Number of cache hits",
         measurementType = MeasurementType.TRENDSUP
   )
   @Override
   public long getHits() {
      return hits.sum();
   }

   @ManagedAttribute(
         description = "Number of cache attribute misses",
         displayName = "Number of cache misses",
         measurementType = MeasurementType.TRENDSUP
   )
   @Override
   public long getMisses() {
      return misses.sum();
   }

   @ManagedAttribute(
         description = "Number of cache removal hits",
         displayName = "Number of cache removal hits",
         measurementType = MeasurementType.TRENDSUP
   )
   @Override
   public long getRemoveHits() {
      return removeHits.sum();
   }

   @ManagedAttribute(
         description = "Number of cache removals where keys were not found",
         displayName = "Number of cache removal misses",
         measurementType = MeasurementType.TRENDSUP
   )
   @Override
   public long getRemoveMisses() {
      return removeMisses.sum();
   }

   @ManagedAttribute(
         description = "Number of cache attribute put operations",
         displayName = "Number of cache puts" ,
         measurementType = MeasurementType.TRENDSUP
   )
   @Override
   public long getStores() {
      return stores.sum();
   }

   @Override
   public long getRetrievals() {
      return hits.longValue() + misses.longValue();
   }

   @ManagedAttribute(
         description = "Number of cache eviction operations",
         displayName = "Number of cache evictions",
         measurementType = MeasurementType.TRENDSUP
   )
   @Override
   public long getEvictions() {
      return evictions.sum();
   }

   @ManagedAttribute(
         description = "Percentage hit/(hit+miss) ratio for the cache",
         displayName = "Hit ratio",
         units = Units.PERCENTAGE
   )
   public double getHitRatio() {
      long hitsL = hits.sum();
      double total = hitsL + misses.sum();
      // The reason for <= is that equality checks
      // should be avoided for floating point numbers.
      if (total <= 0)
         return 0;
      return hitsL / total;
   }

   @ManagedAttribute(
         description = "Read/writes ratio for the cache",
         displayName = "Read/write ratio",
         units = Units.PERCENTAGE
   )
   public double getReadWriteRatio() {
      long sum = stores.sum();
      if (sum == 0)
         return 0;
      return (double) (hits.sum() + misses.sum()) / (double) sum;
   }

   @ManagedAttribute(
         description = "Average number of milliseconds for a read operation on the cache",
         displayName = "Average read time",
         units = Units.MILLISECONDS
   )
   @Override
   public long getAverageReadTime() {
      return TimeUnit.NANOSECONDS.toMillis(getAverageReadTimeNanos());
   }

   @ManagedAttribute(
         description = "Average number of nanoseconds for a read operation on the cache",
         displayName = "Average read time (ns)",
         units = Units.NANOSECONDS
   )
   @Override
   public long getAverageReadTimeNanos() {
      long total = hits.sum() + misses.sum();
      if (total == 0)
         return 0;
      return (hitTimes.sum() + missTimes.sum()) / total;
   }

   @ManagedAttribute(
         description = "Average number of milliseconds for a write operation in the cache",
         displayName = "Average write time",
         units = Units.MILLISECONDS
   )
   @Override
   public long getAverageWriteTime() {
      return TimeUnit.NANOSECONDS.toMillis(getAverageWriteTimeNanos());
   }

   @ManagedAttribute(
         description = "Average number of nanoseconds for a write operation in the cache",
         displayName = "Average write time (ns)",
         units = Units.NANOSECONDS
   )
   @Override
   public long getAverageWriteTimeNanos() {
      long sum = stores.sum();
      if (sum == 0)
         return 0;
      return (storeTimes.sum()) / sum;
   }

   @ManagedAttribute(
         description = "Average number of milliseconds for a remove operation in the cache",
         displayName = "Average remove time",
         units = Units.MILLISECONDS
   )
   @Override
   public long getAverageRemoveTime() {
      return TimeUnit.NANOSECONDS.toMillis(getAverageWriteTimeNanos());
   }

   @ManagedAttribute(
         description = "Average number of nanoseconds for a remove operation in the cache",
         displayName = "Average remove time (ns)",
         units = Units.NANOSECONDS
   )
   @Override
   public long getAverageRemoveTimeNanos() {
      long removes = getRemoveHits();
      if (removes == 0)
         return 0;
      return removeTimes.sum() / removes;
   }

   @ManagedAttribute(description = "Required minimum number of nodes to hold current cache data",
         displayName = "Required minimum number of nodes"
   )
   @Override
   public int getRequiredMinimumNumberOfNodes() {
      return CacheMgmtInterceptor.calculateRequiredMinimumNumberOfNodes(cache.wired(), componentRegistry);
   }

   @Override
   public void reset() {
      resetStatistics();
   }

   @Override
   public boolean getStatisticsEnabled() {
      return statisticsEnabled;
   }

   @ManagedAttribute(description = "Enables or disables the gathering of statistics by this component", writable = true)
   @Override
   public void setStatisticsEnabled(boolean enabled) {
      statisticsEnabled = enabled;
   }

   @ManagedAttribute(
         description = "Number of entries in the cache including passivated entries",
         displayName = "Number of current cache entries"
   )
   public int getNumberOfEntries() {
      return cache.wired().withFlags(Flag.CACHE_MODE_LOCAL).size();
   }

   @ManagedAttribute(
         description = "Number of entries currently in-memory excluding expired entries",
         displayName = "Number of in-memory cache entries"
   )
   @Override
   public int getCurrentNumberOfEntriesInMemory() {
      return dataContainer.size();
   }

   @ManagedAttribute(
         description = "Number of seconds since cache started",
         displayName = "Seconds since cache started",
         units = Units.SECONDS,
         measurementType = MeasurementType.TRENDSUP
   )
   @Override
   public long getTimeSinceStart() {
      return timeService.timeDuration(startNanoseconds.get(), TimeUnit.SECONDS);
   }

   @ManagedAttribute(
         description = "Number of seconds since the cache statistics were last reset",
         displayName = "Seconds since cache statistics were reset",
         units = Units.SECONDS
   )
   @Override
   public long getTimeSinceReset() {
      return timeService.timeDuration(resetNanoseconds.get(), TimeUnit.SECONDS);
   }

   @Override
   public int getCurrentNumberOfEntries() {
      return getNumberOfEntries();
   }

   @Override
   public long getTotalNumberOfEntries() {
      return stores.longValue();
   }

   @ManagedAttribute(
         description = "Amount of memory in bytes allocated for use in eviction for data in the cache",
         displayName = "Memory used by data in the cache"
   )
   @Override
   public long getDataMemoryUsed() {
      if (configuration.memory().isEvictionEnabled() && configuration.memory().evictionType() == EvictionType.MEMORY) {
         return dataContainer.evictionSize();
      }
      return 0;
   }

   @ManagedAttribute(
         description = "Amount off-heap memory used by this cache (bytes)",
         displayName = "Off-Heap memory used"
   )
   @Override
   public long getOffHeapMemoryUsed() {
      return allocator.getAllocatedAmount();
   }

   @ManagedOperation(
         description = "Resets statistics gathered by this component",
         displayName = "Reset Statistics (Statistics)"
   )
   @Override
   public void resetStatistics() {
      hits.reset();
      misses.reset();
      stores.reset();
      evictions.reset();
      hitTimes.reset();
      missTimes.reset();
      storeTimes.reset();
      removeHits.reset();
      removeTimes.reset();
      removeMisses.reset();
      resetNanoseconds.set(timeService.time());
   }

   public void recordMisses(int misses, long time) {
      this.misses.add(misses);
      this.missTimes.add(time);
   }

   public void recordHits(int hits, long time) {
      this.hits.add(hits);
      this.hitTimes.add(time);
   }

   public void recordEviction() {
      evictions.increment();
   }

   public void recordStores(int stores, long time) {
      this.stores.add(stores);
      this.storeTimes.add(time);
   }

   public void recordRemoveHits(int removes, long time) {
      this.removeHits.add(removes);
      this.removeTimes.add(time);
   }

   public void recordRemoveMisses(int removes) {
      this.removeMisses.add(removes);
   }

   @DefaultFactoryFor(classes = StatsCollector.class)
   @SurvivesRestarts
   public static class Factory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
      @Override
      public Object construct(String componentName) {
         if (componentName.equals(StatsCollector.class.getName())) {
            return new StatsCollector();
         } else {
            throw CONTAINER.factoryCannotConstructComponent(componentName);
         }
      }
   }
}
