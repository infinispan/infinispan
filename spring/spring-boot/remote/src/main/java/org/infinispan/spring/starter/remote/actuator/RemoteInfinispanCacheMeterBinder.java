package org.infinispan.spring.starter.remote.actuator;

import org.infinispan.client.hotrod.RemoteCache;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.binder.cache.CacheMeterBinder;

/**
 * Implements {@link CacheMeterBinder} to expose Infinispan remote metrics
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 2.1
 */
public class RemoteInfinispanCacheMeterBinder extends CacheMeterBinder {

   private final RemoteCache cache;

   public RemoteInfinispanCacheMeterBinder(RemoteCache cache, Iterable<Tag> tags) {
      super(cache, cache.getName(), tags);
      this.cache = cache;
   }

   @Override
   protected Long size() {
      if (cache == null) return 0L;

      /**
       * TODO implement this. Which is the equivalent to
       * {@code cache.getAdvancedCache().getStats().getTotalNumberOfEntries();}
       */
      return Long.valueOf(cache.size());
   }

   @Override
   protected long hitCount() {
      if (cache == null) return 0L;

      return cache.clientStatistics().getRemoteHits();
   }

   @Override
   protected Long missCount() {
      if (cache == null) return 0L;

      return cache.clientStatistics().getRemoteMisses();
   }

   @Override
   protected Long evictionCount() {
      if (cache == null) return 0L;

      return cache.clientStatistics().getRemoteRemoves();
   }

   @Override
   protected long putCount() {
      if (cache == null) return 0L;

      return cache.clientStatistics().getRemoteStores();
   }

   @Override
   protected void bindImplementationSpecificMetrics(MeterRegistry registry) {
      if (cache == null) return;

      Gauge.builder("cache.reset", cache, cache -> cache.clientStatistics().getTimeSinceReset())
            .tags(getTagsWithCacheName()).tag("ownership", "backup")
            .description("Time elapsed in seconds since the last statistics reset")
            .register(registry);

      averages(registry);
      nearCacheMetrics(registry);
   }

   private void averages(MeterRegistry registry) {
      Gauge.builder("cache.puts.latency", cache, cache -> cache.clientStatistics().getAverageRemoteStoreTime())
            .tags(getTagsWithCacheName())
            .description("Cache puts")
            .register(registry);

      Gauge.builder("cache.gets.latency", cache, cache -> cache.clientStatistics().getAverageRemoteReadTime())
            .tags(getTagsWithCacheName())
            .description("Cache gets")
            .register(registry);

      Gauge.builder("cache.removes.latency", cache, cache -> cache.clientStatistics().getAverageRemoteRemovesTime())
            .tags(getTagsWithCacheName())
            .description("Cache removes")
            .register(registry);
   }

   private void nearCacheMetrics(MeterRegistry registry) {
      if (isNearCacheEnabled()) {
         Gauge.builder("cache.near.requests", cache, cache -> cache.clientStatistics().getNearCacheHits())
               .tags(getTagsWithCacheName()).tag("result", "hit")
               .description("The number of hits (reads) of near cache entries owned by this client")
               .register(registry);

         Gauge.builder("cache.near.requests", cache, cache -> cache.clientStatistics().getNearCacheMisses())
               .tags(getTagsWithCacheName()).tag("result", "miss")
               .description("The number of hits (reads) of near cache entries owned by this client")
               .register(registry);

         Gauge.builder("cache.near.invalidations", cache, cache -> cache.clientStatistics().getNearCacheInvalidations())
               .tags(getTagsWithCacheName())
               .description("The number of invalidations of near cache entries owned by this client")
               .register(registry);

         Gauge.builder("cache.near.size", cache, cache -> cache.clientStatistics().getNearCacheSize())
               .tags(getTagsWithCacheName())
               .description("The size of the near cache owned by this client")
               .register(registry);
      }
   }

   private boolean isNearCacheEnabled() {
      return cache.getRemoteCacheManager().getConfiguration().nearCache().mode().enabled();
   }

}
