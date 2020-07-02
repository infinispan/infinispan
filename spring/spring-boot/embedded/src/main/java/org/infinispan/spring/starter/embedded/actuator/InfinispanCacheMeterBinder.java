package org.infinispan.spring.starter.embedded.actuator;

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
public class InfinispanCacheMeterBinder extends CacheMeterBinder {

   private final Cache cache;

   public InfinispanCacheMeterBinder(Cache cache, Iterable<Tag> tags) {
      super(cache, cache.getName(), tags);
      this.cache = cache;
   }

   @Override
   protected Long size() {
      if (cache == null) return 0L;

      return cache.getAdvancedCache().getStats().getStores();
   }

   @Override
   protected long hitCount() {
      if (cache == null) return 0L;

      return cache.getAdvancedCache().getStats().getHits();
   }

   @Override
   protected Long missCount() {
      if (cache == null) return 0L;

      return cache.getAdvancedCache().getStats().getMisses();
   }

   @Override
   protected Long evictionCount() {
      if (cache == null) return 0L;

      return cache.getAdvancedCache().getStats().getEvictions();
   }

   @Override
   protected long putCount() {
      if (cache == null) return 0L;

      return cache.getAdvancedCache().getStats().getStores();
   }

   @Override
   protected void bindImplementationSpecificMetrics(MeterRegistry registry) {
      if (cache == null) return;

      Gauge.builder("cache.start", cache, cache -> cache.getAdvancedCache().getStats().getTimeSinceStart())
            .baseUnit(TimeUnit.SECONDS.name())
            .tags(getTagsWithCacheName())
            .description("Time elapsed since start")
            .register(registry);

      Gauge.builder("cache.reset", cache, cache -> cache.getAdvancedCache().getStats().getTimeSinceReset())
            .baseUnit(TimeUnit.SECONDS.name())
            .tags(getTagsWithCacheName())
            .description("Time elapsed since the last statistics reset")
            .register(registry);

      memory(registry);
      averages(registry);
   }

   private void memory(MeterRegistry registry) {
      Gauge.builder("cache.memory.size", cache, cache -> cache.getAdvancedCache().getStats().getCurrentNumberOfEntriesInMemory())
            .tags(getTagsWithCacheName())
            .description("Number of entries currently in the cache, excluding passivated entries")
            .register(registry);

      if (cache.getCacheConfiguration().memory().evictionStrategy().isEnabled()) {
         Gauge.builder("cache.memory.used", cache, cache -> cache.getAdvancedCache().getStats().getDataMemoryUsed())
               .tags(getTagsWithCacheName())
               .description("Provides how much memory the current eviction algorithm estimates is in use for data")
               .register(registry);
      }

      Gauge.builder("cache.memory.offHeap", cache, cache -> cache.getAdvancedCache().getStats().getOffHeapMemoryUsed())
            .tags(getTagsWithCacheName())
            .description("The amount of off-heap memory used by this cache")
            .register(registry);
   }

   private void averages(MeterRegistry registry) {
      Gauge.builder("cache.puts.latency", cache, cache -> cache.getAdvancedCache().getStats().getAverageWriteTime())
            .baseUnit(TimeUnit.MILLISECONDS.name())
            .tags(getTagsWithCacheName())
            .description("Cache puts")
            .register(registry);

      Gauge.builder("cache.gets.latency", cache, cache -> cache.getAdvancedCache().getStats().getAverageReadTime())
            .baseUnit(TimeUnit.MILLISECONDS.name())
            .tags(getTagsWithCacheName())
            .description("Cache gets")
            .register(registry);

      Gauge.builder("cache.removes.latency", cache, cache -> cache.getAdvancedCache().getStats().getAverageRemoveTime())
            .baseUnit(TimeUnit.MILLISECONDS.name())
            .tags(getTagsWithCacheName())
            .description("Cache removes")
            .register(registry);
   }
}
