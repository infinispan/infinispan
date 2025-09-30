package org.infinispan.spring.starter.embedded.metrics;

import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.cache.CacheMeterBinder;

/**
 * Implements {@link CacheMeterBinder} to expose Infinispan embedded metrics
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 2.1
 */
public class InfinispanCacheMeterBinder<K, V> extends CacheMeterBinder<Cache<K, V>> {

   public InfinispanCacheMeterBinder(Cache<K, V> cache, Iterable<Tag> tags) {
      super(cache, cache.getName(), tags);
   }

   @Override
   protected Long size() {
      if (getCache() == null) return 0L;

      return checkNegativeStat(getCache().getAdvancedCache().getStats().getApproximateEntriesInMemory());
   }

   @Override
   protected long hitCount() {
      if (getCache() == null) return 0L;

      return checkNegativeStat(getCache().getAdvancedCache().getStats().getHits());
   }

   @Override
   protected Long missCount() {
      if (getCache() == null) return 0L;

      return checkNegativeStat(getCache().getAdvancedCache().getStats().getMisses());
   }

   @Override
   protected Long evictionCount() {
      if (getCache() == null) return 0L;

      return checkNegativeStat(getCache().getAdvancedCache().getStats().getEvictions());
   }

   @Override
   protected long putCount() {
      if (getCache() == null) return 0L;

      return checkNegativeStat(getCache().getAdvancedCache().getStats().getStores());
   }

   @Override
   protected void bindImplementationSpecificMetrics(MeterRegistry registry) {
      if (getCache() == null) return;

      Gauge.builder("cache.start", getCache(), cache -> checkNegativeStat(cache.getAdvancedCache().getStats().getTimeSinceStart()))
            .baseUnit(TimeUnit.SECONDS.name())
            .tags(getTagsWithCacheName())
            .description("Time elapsed since start")
            .register(registry);

      Gauge.builder("cache.reset", getCache(), cache -> checkNegativeStat(cache.getAdvancedCache().getStats().getTimeSinceReset()))
            .baseUnit(TimeUnit.SECONDS.name())
            .tags(getTagsWithCacheName())
            .description("Time elapsed since the last statistics reset")
            .register(registry);

      memory(registry);
      averages(registry);
      averagesNanos(registry);
   }

   private void memory(MeterRegistry registry) {
      Cache<K, V> cache = getCache();
      Gauge.builder("cache.memory.size", cache, c -> checkNegativeStat(c.getAdvancedCache().getStats().getApproximateEntriesInMemory()))
            .tags(getTagsWithCacheName())
            .description("Approximate number of entries in the cache, excluding passivated entries")
            .register(registry);

      if (cache.getCacheConfiguration().memory().isEvictionEnabled()) {
         Gauge.builder("cache.memory.used", cache, c -> checkNegativeStat(c.getAdvancedCache().getStats().getDataMemoryUsed()))
               .tags(getTagsWithCacheName())
               .description("Provides how much memory the current eviction algorithm estimates is in use for data")
               .register(registry);
      }

      Gauge.builder("cache.memory.offHeap", cache, c -> checkNegativeStat(c.getAdvancedCache().getStats().getOffHeapMemoryUsed()))
            .tags(getTagsWithCacheName())
            .description("The amount of off-heap memory used by this cache")
            .register(registry);
   }

   private void averages(MeterRegistry registry) {
      Cache<K, V> cache = getCache();
      Gauge.builder("cache.puts.latency", cache, c -> checkNegativeStat(c.getAdvancedCache().getStats().getAverageWriteTime()))
            .baseUnit(TimeUnit.MILLISECONDS.name())
            .tags(getTagsWithCacheName())
            .description("Cache puts")
            .register(registry);

      Gauge.builder("cache.gets.latency", cache, c -> checkNegativeStat(c.getAdvancedCache().getStats().getAverageReadTime()))
            .baseUnit(TimeUnit.MILLISECONDS.name())
            .tags(getTagsWithCacheName())
            .description("Cache gets")
            .register(registry);

      Gauge.builder("cache.removes.latency", cache, c -> checkNegativeStat(c.getAdvancedCache().getStats().getAverageRemoveTime()))
            .baseUnit(TimeUnit.MILLISECONDS.name())
            .tags(getTagsWithCacheName())
            .description("Cache removes")
            .register(registry);
   }

   private void averagesNanos(MeterRegistry registry) {
      Cache<K, V> cache = getCache();
      Gauge.builder("cache.puts.nlatency", cache, c -> checkNegativeStat(c.getAdvancedCache().getStats().getAverageWriteTimeNanos()))
            .baseUnit(TimeUnit.NANOSECONDS.name())
            .tags(getTagsWithCacheName())
            .description("Cache puts in nanos")
            .register(registry);

      Gauge.builder("cache.gets.nlatency", cache, c -> checkNegativeStat(c.getAdvancedCache().getStats().getAverageReadTimeNanos()))
            .baseUnit(TimeUnit.NANOSECONDS.name())
            .tags(getTagsWithCacheName())
            .description("Cache gets in nanos")
            .register(registry);

      Gauge.builder("cache.removes.nlatency", cache, c -> checkNegativeStat(c.getAdvancedCache().getStats().getAverageRemoveTimeNanos()))
            .baseUnit(TimeUnit.NANOSECONDS.name())
            .tags(getTagsWithCacheName())
            .description("Cache removes in nanos")
            .register(registry);
   }

   private static long checkNegativeStat(long stat) {
      return stat < 0 ? 0L : stat;
   }

}
