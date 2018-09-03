package org.infinispan.query.dsl.embedded.impl;

import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionType;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.objectfilter.impl.aggregation.FieldAccumulator;
import org.infinispan.query.logging.Log;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.logging.LogFactory;

/**
 * A local cache for 'parsed' queries. Each cache manager has at most one QueryCache which is backed by a lazily
 * created Cache.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
@ThreadSafe
@Scope(Scopes.GLOBAL)
public class QueryCache {

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
   private static final long ENTRY_LIFESPAN = 300;  // seconds

   @Inject private EmbeddedCacheManager cacheManager;
   @Inject private InternalCacheRegistry internalCacheRegistry;

   private volatile Cache<QueryCacheKey, Object> lazyCache;

   /**
    * Gets the cached query object. The key used for lookup is an object pair containing the query string and a
    * discriminator value which is usually the Class of the cached query object and an optional {@link List} of {@link
    * FieldAccumulator}s.
    */
   public <T> T get(String queryString, List<FieldAccumulator> accumulators, Object queryTypeDiscriminator, QueryCreator<T> queryCreator) {
      QueryCacheKey key = new QueryCacheKey(queryString, accumulators, queryTypeDiscriminator);
      return (T) getCache().computeIfAbsent(key, (k) -> queryCreator.create(k.queryString, k.accumulators));
   }

   public void clear() {
      getCache().clear();
   }

   /**
    * Obtain the cache. Start it lazily when needed.
    */
   private Cache<QueryCacheKey, Object> getCache() {
      final Cache<QueryCacheKey, Object> cache = lazyCache;

      //Most likely branch first:
      if (cache != null) {
         return cache;
      }
      synchronized (this) {
         if (lazyCache == null) {
            // define the query cache configuration if it does not already exist (from a previous call or manually defined by the user)
            internalCacheRegistry.registerInternalCache(QUERY_CACHE_NAME, getQueryCacheConfig().build(), EnumSet.noneOf(InternalCacheRegistry.Flag.class));
            lazyCache = cacheManager.getCache(QUERY_CACHE_NAME);
         }
         return lazyCache;
      }
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
            .memory().evictionType(EvictionType.COUNT).size(MAX_ENTRIES);
      return cfgBuilder;
   }

   /**
    * The key of the query cache: a tuple with 3 components. Serialization of this object is not expected as
    * the cache is local and there is no store configured.
    */
   private static final class QueryCacheKey {

      final String queryString;

      final List<FieldAccumulator> accumulators;

      final Object queryTypeDiscriminator;

      QueryCacheKey(String queryString, List<FieldAccumulator> accumulators, Object queryTypeDiscriminator) {
         this.queryString = queryString;
         this.accumulators = accumulators;
         this.queryTypeDiscriminator = queryTypeDiscriminator;
      }

      @Override
      public boolean equals(Object obj) {
         if (this == obj) return true;
         if (!(obj instanceof QueryCacheKey)) return false;
         QueryCacheKey other = (QueryCacheKey) obj;
         return queryString.equals(other.queryString)
               && (accumulators != null ? accumulators.equals(other.accumulators) : other.accumulators == null)
               && queryTypeDiscriminator.equals(other.queryTypeDiscriminator);
      }

      @Override
      public int hashCode() {
         int result = queryString.hashCode();
         result = 31 * result + (accumulators != null ? accumulators.hashCode() : 0);
         result = 31 * result + queryTypeDiscriminator.hashCode();
         return result;
      }

      @Override
      public String toString() {
         return "QueryCacheKey{" +
               "queryString='" + queryString + '\'' +
               ", accumulators=" + accumulators +
               ", queryTypeDiscriminator=" + queryTypeDiscriminator +
               '}';
      }
   }
}
