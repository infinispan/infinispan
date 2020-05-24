package org.infinispan.query.core.impl;

import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.objectfilter.impl.aggregation.FieldAccumulator;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.logging.LogFactory;

import net.jcip.annotations.ThreadSafe;

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

   private static final Log log = LogFactory.getLog(QueryCache.class, Log.class);

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

   @Inject
   InternalCacheRegistry internalCacheRegistry;

   private volatile Cache<QueryCacheKey, Object> lazyCache;

   /**
    * Gets the cached query object. The key used for lookup is an object pair containing the query string and a
    * discriminator value which is usually the Class of the cached query object and an optional {@link List} of {@link
    * FieldAccumulator}s.
    */
   public <T> T get(String cacheName, String queryString, List<FieldAccumulator> accumulators, Object queryTypeDiscriminator, QueryCreator<T> queryCreator) {
      QueryCacheKey key = new QueryCacheKey(cacheName, queryString, accumulators, queryTypeDiscriminator);
      return (T) getOptionalCache(true).map(c -> c.computeIfAbsent(key, (k) -> queryCreator.create(k.queryString, k.accumulators))).orElse(null);
   }

   public void clear() {
      log.debug("Clearing query cache for all caches");
      getOptionalCache(false).ifPresent(Cache::clear);
   }

   public void clear(String cacheName) {
      log.debugf("Clearing query cache for cache %s", cacheName);
      getOptionalCache(false).ifPresent(c -> c.keySet().removeIf(k -> k.cacheName.equals(cacheName)));
   }

   /**
    * Obtain and return the cache, starting it lazily if needed.
    */
   private Optional<Cache<QueryCacheKey, Object>> getOptionalCache(boolean createIfAbsent) {
      Cache<QueryCacheKey, Object> cache = lazyCache;
      if (createIfAbsent && cache == null) {
         synchronized (this) {
            if (lazyCache == null) {
               // define the query cache configuration if it does not already exist (from a previous call or manually defined by the user)
               internalCacheRegistry.registerInternalCache(QUERY_CACHE_NAME, getQueryCacheConfig().build(), EnumSet.noneOf(InternalCacheRegistry.Flag.class));
               lazyCache = cacheManager.getCache(QUERY_CACHE_NAME);
            }
            cache = lazyCache;
         }
      }
      return Optional.ofNullable(cache);
   }

   /**
    * Create the configuration of the internal query cache.
    */
   private ConfigurationBuilder getQueryCacheConfig() {
      ConfigurationBuilder cfgBuilder = new ConfigurationBuilder();
      cfgBuilder
            .clustering().cacheMode(CacheMode.LOCAL)
            .transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL)
            .expiration().maxIdle(ENTRY_LIFESPAN, TimeUnit.SECONDS)
            .memory().maxCount(MAX_ENTRIES);
      return cfgBuilder;
   }

   /**
    * The key of the query cache: a tuple with 3 components. Serialization of this object is not expected as the cache
    * is local and there is no store configured.
    */
   private static final class QueryCacheKey {

      final String cacheName;

      final String queryString;

      final List<FieldAccumulator> accumulators;

      final Object queryTypeDiscriminator;

      QueryCacheKey(String cacheName, String queryString, List<FieldAccumulator> accumulators, Object queryTypeDiscriminator) {
         this.cacheName = cacheName;
         this.queryString = queryString;
         this.accumulators = accumulators;
         this.queryTypeDiscriminator = queryTypeDiscriminator;
      }

      @Override
      public boolean equals(Object obj) {
         if (this == obj) return true;
         if (!(obj instanceof QueryCacheKey)) return false;
         QueryCacheKey other = (QueryCacheKey) obj;
         return cacheName.equals(other.cacheName)
               && queryString.equals(other.queryString)
               && (accumulators != null ? accumulators.equals(other.accumulators) : other.accumulators == null)
               && queryTypeDiscriminator.equals(other.queryTypeDiscriminator);
      }

      @Override
      public int hashCode() {
         int result = cacheName.hashCode();
         result = 31 * result + queryString.hashCode();
         result = 31 * result + (accumulators != null ? accumulators.hashCode() : 0);
         result = 31 * result + queryTypeDiscriminator.hashCode();
         return result;
      }

      @Override
      public String toString() {
         return "QueryCacheKey{" +
               "cacheName='" + cacheName + '\'' +
               ", queryString='" + queryString + '\'' +
               ", accumulators=" + accumulators +
               ", queryTypeDiscriminator=" + queryTypeDiscriminator +
               '}';
      }
   }
}
