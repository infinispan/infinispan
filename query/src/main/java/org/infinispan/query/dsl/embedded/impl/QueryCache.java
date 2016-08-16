package org.infinispan.query.dsl.embedded.impl;

import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.eviction.EvictionType;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.logging.Log;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.KeyValuePair;
import org.infinispan.util.logging.LogFactory;

import net.jcip.annotations.ThreadSafe;

/**
 * A local cache for 'parsed' queries. Each cache manager has at most one QueryCache which is backed by a lazily
 * created cache.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
@ThreadSafe
public class QueryCache {

   private static final Log log = LogFactory.getLog(QueryCache.class, Log.class);

   private final boolean trace = log.isTraceEnabled();

   /**
    * Users can define a cache configuration with this name if they need to fine tune query caching. If they do not do
    * so a default config is used (see {@link QueryCache#getDefaultQueryCacheConfig()}).
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

   private EmbeddedCacheManager cacheManager;

   private InternalCacheRegistry internalCacheRegistry;

   private volatile Cache<KeyValuePair<String, ?>, Object> lazyCache;

   @Inject
   public void init(EmbeddedCacheManager cacheManager, InternalCacheRegistry internalCacheRegistry) {
      this.cacheManager = cacheManager;
      this.internalCacheRegistry = internalCacheRegistry;
   }

   /**
    * Gets the cached query object. The key used for lookup is an object pair containing the query string and a
    * discriminator value which is usually the Class of the cached query object.
    */
   public <T> T get(KeyValuePair<String, ?> queryKey) {
      T cachedResult = (T) getCache().get(queryKey);
      if (trace && cachedResult != null) {
         log.tracef("QueryCache hit: %s, %s", queryKey.getKey(), queryKey.getValue());
      }
      return cachedResult;
   }

   public void put(KeyValuePair<String, ?> queryKey, Object queryParsingResult) {
      getCache().put(queryKey, queryParsingResult);
   }

   public void clear() {
      getCache().clear();
   }

   /**
    * Obtain the cache. Start it lazily when needed.
    */
   private Cache<KeyValuePair<String, ?>, Object> getCache() {
      final Cache<KeyValuePair<String, ?>, Object> cache = lazyCache;

      //Most likely branch first:
      if (cache != null) {
         return cache;
      }
      synchronized (this) {
         if (lazyCache == null) {
            // define the query cache configuration if it does not already exist (from a previous call or manually defined by the user)
            internalCacheRegistry.registerInternalCache(QUERY_CACHE_NAME, getDefaultQueryCacheConfig().build(), EnumSet.noneOf(InternalCacheRegistry.Flag.class));
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
            .eviction().type(EvictionType.COUNT).size(MAX_ENTRIES)
            .strategy(EvictionStrategy.LIRS);
      return cfgBuilder;
   }
}
