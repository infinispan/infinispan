package org.infinispan.spring.starter.remote.metrics;

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
public class RemoteInfinispanCacheMeterBinder<K, V> extends CacheMeterBinder<RemoteCache<K, V>> {

   public RemoteInfinispanCacheMeterBinder(RemoteCache<K, V> cache, Iterable<Tag> tags) {
      super(cache, cache.getName(), tags);
   }

   @Override
   protected Long size() {
      return null;
   }

   @Override
   protected long hitCount() {
      if (getCache() == null) return 0L;

      return getCache().clientStatistics().getRemoteHits();
   }

   @Override
   protected Long missCount() {
      if (getCache() == null) return 0L;

      return getCache().clientStatistics().getRemoteMisses();
   }

   @Override
   protected Long evictionCount() {
      if (getCache() == null) return 0L;

      return getCache().clientStatistics().getRemoteRemoves();
   }

   @Override
   protected long putCount() {
      if (getCache() == null) return 0L;

      return getCache().clientStatistics().getRemoteStores();
   }

   @Override
   protected void bindImplementationSpecificMetrics(MeterRegistry registry) {
      if (getCache() == null) return;

      Gauge.builder("cache.reset", getCache(), cache -> cache.clientStatistics().getTimeSinceReset())
            .tags(getTagsWithCacheName()).tag("ownership", "backup")
            .description("Time elapsed in seconds since the last statistics reset")
            .register(registry);

      averages(registry);
      nearCacheMetrics(registry);
   }

   private void averages(MeterRegistry registry) {
      Gauge.builder("cache.puts.latency", getCache(), cache -> cache.clientStatistics().getAverageRemoteStoreTime())
            .tags(getTagsWithCacheName())
            .description("Cache puts")
            .register(registry);

      Gauge.builder("cache.gets.latency", getCache(), cache -> cache.clientStatistics().getAverageRemoteReadTime())
            .tags(getTagsWithCacheName())
            .description("Cache gets")
            .register(registry);

      Gauge.builder("cache.removes.latency", getCache(), cache -> cache.clientStatistics().getAverageRemoteRemovesTime())
            .tags(getTagsWithCacheName())
            .description("Cache removes")
            .register(registry);
   }

   private void nearCacheMetrics(MeterRegistry registry) {
      if (isNearCacheEnabled()) {
         Gauge.builder("cache.near.requests", getCache(), cache -> cache.clientStatistics().getNearCacheHits())
               .tags(getTagsWithCacheName()).tag("result", "hit")
               .description("The number of hits (reads) of near cache entries owned by this client")
               .register(registry);

         Gauge.builder("cache.near.requests", getCache(), cache -> cache.clientStatistics().getNearCacheMisses())
               .tags(getTagsWithCacheName()).tag("result", "miss")
               .description("The number of hits (reads) of near cache entries owned by this client")
               .register(registry);

         Gauge.builder("cache.near.invalidations", getCache(), cache -> cache.clientStatistics().getNearCacheInvalidations())
               .tags(getTagsWithCacheName())
               .description("The number of invalidations of near cache entries owned by this client")
               .register(registry);

         Gauge.builder("cache.near.size", getCache(), cache -> cache.clientStatistics().getNearCacheSize())
               .tags(getTagsWithCacheName())
               .description("The size of the near cache owned by this client")
               .register(registry);
      }
   }

   private boolean isNearCacheEnabled() {
      return getCache().clientStatistics().getNearCacheSize() > 0;
   }

}
