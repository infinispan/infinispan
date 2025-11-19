package org.infinispan.query.core.impl;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.objectfilter.impl.aggregation.FieldAccumulator;

import com.google.errorprone.annotations.ThreadSafe;

/**
 * A local cache for 'parsed' queries. Each cache manager has at most one QueryCache which is backed by a lazily created
 * Cache.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
@ThreadSafe
@Scope(Scopes.GLOBAL)
public final class QueryCache {

   @FunctionalInterface
   public interface QueryCreator<Q> {

      /**
       * Create a new query object based on the input args, or just return {@code null}. If {@code null} is returned
       * this will be propagated to the caller of {@link QueryCache#get} and the {@code null} will not be cached.
       */
      Q create(String queryString, List<FieldAccumulator> accumulators);
   }

   private static final Log log = Log.getLog(QueryCache.class);

   /**
    * Users can define a cache configuration with this name if they need to fine tune query caching. If they do not do
    * so a default config is used (see {@link QueryCache#getQueryCacheConfig()}).
    */
   public static final String QUERY_CACHE_NAME = "___query_cache";

   /**
    * Max number of cached entries.
    */
   private static final long MAX_ENTRIES = 200;

   /**
    * Cache entry lifespan in seconds.
    */
   private static final long ENTRY_LIFESPAN = 300;

   @Inject
   EmbeddedCacheManager cacheManager;

   private volatile Cache<QueryCacheKey, Object> cache;

   /**
    * Gets the cached query object. The key used for lookup is an object pair containing the query string and a
    * discriminator value which is usually the Class of the cached query object and an optional {@link List} of {@link
    * FieldAccumulator}s.
    */
   public <T> T get(String cacheName, String queryString, List<FieldAccumulator> accumulators, Object queryTypeDiscriminator, QueryCreator<T> queryCreator) {
      QueryCacheKey key = new QueryCacheKey(cacheName, queryString, accumulators, queryTypeDiscriminator);
      return (T) getCache().computeIfAbsent(key, (k) -> queryCreator.create(k.queryString, k.accumulators));
   }

   public void clear() {
      log.debug("Clearing query cache for all caches");
      if (cache != null) {
         cache.clear();
      }
   }

   public void clear(String cacheName) {
      log.debugf("Clearing query cache for cache %s", cacheName);
      if (cache != null) {
         cache.keySet().removeIf(k -> k.cacheName.equals(cacheName));
      }
   }

   private Cache<QueryCacheKey, Object> getCache() {
      if (cache == null) {
         cache = cacheManager.getCache(QUERY_CACHE_NAME);
      }
      return cache;
   }

   /**
    * Create the configuration of the internal query cache.
    */
   public static ConfigurationBuilder getQueryCacheConfig() {
      ConfigurationBuilder cfgBuilder = new ConfigurationBuilder();
      cfgBuilder
            .simpleCache(true)
            .expiration().maxIdle(ENTRY_LIFESPAN, TimeUnit.SECONDS)
            .memory().maxCount(MAX_ENTRIES);
      return cfgBuilder;
   }

   /**
    * The key of the query cache: a tuple with 3 components. Serialization of this object is not expected as the cache
    * is local and there is no store configured.
    */
   private record QueryCacheKey(String cacheName, String queryString, List<FieldAccumulator> accumulators,
                                Object queryTypeDiscriminator) {
   }
}
