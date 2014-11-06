package org.infinispan.query.dsl.embedded.impl;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.logging.Log;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.TimeUnit;

/**
 * A local cache for 'parsed' queries. Each cache manager has one instance.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
@ThreadSafe
public class QueryCache {

   private static final Log log = LogFactory.getLog(QueryCache.class, Log.class);

   /**
    * Users can define a cache configuration with this name if they need to fine tune query caching. If they do not do
    * so a default config is used (see {@link QueryCache#getDefaultQueryCacheConfig()}).
    */
   public static final String QUERY_CACHE_NAME = "___query_cache";

   /**
    * Max number of cached entries.
    */
   private static final int MAX_ENTRIES = 200;

   /**
    * Cache entry lifespan in seconds.
    */
   private static final int ENTRY_LIFESPAN = 300;

   private EmbeddedCacheManager cacheManager;

   private volatile Cache<KeyValuePair<String, Class>, Object> lazyCache;

   @Inject
   public void init(EmbeddedCacheManager cacheManager) {
      this.cacheManager = cacheManager;
   }

   /**
    * Gets the cache object. The key used for lookup if a pair containing the query string and the Class of the cached
    * query object.
    */
   public <T> T get(KeyValuePair<String, Class> queryKey) {
      Object cachedResult = getCache().get(queryKey);
      if (cachedResult != null) {
         log.debugf("QueryCache hit: %s, %s", queryKey.getKey(), queryKey.getValue());
      }
      return (T) cachedResult;
   }

   public void put(KeyValuePair<String, Class> queryKey, Object queryParsingResult) {
      getCache().put(queryKey, queryParsingResult);
   }

   /**
    * Obtain the cache. Start it lazily when needed.
    */
   private Cache<KeyValuePair<String, Class>, Object> getCache() {
      final Cache<KeyValuePair<String, Class>, Object> cache = lazyCache;

      //Most likely branch first:
      if (cache != null) {
         return cache;
      }
      synchronized (this) {
         if (lazyCache == null) {
            // define the query cache configuration if it does not already exist (from a previous call or manually defined by the user)
            if (cacheManager.getCacheConfiguration(QUERY_CACHE_NAME) == null) {
               cacheManager.defineConfiguration(QUERY_CACHE_NAME, getDefaultQueryCacheConfig().build());
            }
            lazyCache = cacheManager.getCache(QUERY_CACHE_NAME);
         }
         return lazyCache;
      }
   }

   private ConfigurationBuilder getDefaultQueryCacheConfig() {
      ConfigurationBuilder cfgBuilder = new ConfigurationBuilder();
      cfgBuilder
            .clustering().cacheMode(CacheMode.LOCAL)
            .transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL)
            .expiration().maxIdle(ENTRY_LIFESPAN, TimeUnit.SECONDS)
            .eviction().maxEntries(MAX_ENTRIES)
            .strategy(EvictionStrategy.LIRS);
      return cfgBuilder;
   }
}
