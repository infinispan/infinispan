package org.infinispan.query.core;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverter;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.impl.ReflectionMatcher;
import org.infinispan.query.api.continuous.ContinuousQuery;
import org.infinispan.query.core.impl.EmbeddedQueryFactory;
import org.infinispan.query.core.impl.Log;
import org.infinispan.query.core.impl.QueryEngine;
import org.infinispan.query.core.impl.continuous.ContinuousQueryImpl;
import org.infinispan.query.core.impl.eventfilter.IckleCacheEventFilterConverter;
import org.infinispan.query.core.impl.eventfilter.IckleFilterAndConverter;
import org.infinispan.query.core.stats.SearchStatistics;
import org.infinispan.query.core.stats.SearchStatisticsSnapshot;
import org.infinispan.query.core.stats.impl.SearchStatsRetriever;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.util.logging.LogFactory;

/**
 * <b>EXPERIMENTAL</b>
 * This is the entry point for the Infinispan index-less query API. It provides the {@link QueryFactory} which is your
 * starting point for building Ickle queries or DSL-based queries, continuous queries and event filters for unindexed
 * caches.
 *
 * @author anistor@redhat.com
 * @since 10.1
 */
public final class Search {

   private static final Log log = LogFactory.getLog(Search.class, Log.class);

   private Search() {
      // prevent instantiation
   }

   /**
    * Create an event filter out of an Ickle query string.
    */
   public static <K, V> CacheEventFilterConverter<K, V, ObjectFilter.FilterResult> makeFilter(String queryString) {
      return makeFilter(queryString, null);
   }

   /**
    * Create an event filter out of an Ickle query string.
    */
   public static <K, V> CacheEventFilterConverter<K, V, ObjectFilter.FilterResult> makeFilter(String queryString, Map<String, Object> namedParameters) {
      IckleFilterAndConverter<K, V> filterAndConverter = new IckleFilterAndConverter<>(queryString, namedParameters, ReflectionMatcher.class);
      return new IckleCacheEventFilterConverter<>(filterAndConverter);
   }

   /**
    * Create an event filter out of an Ickle query.
    */
   public static <K, V> CacheEventFilterConverter<K, V, ObjectFilter.FilterResult> makeFilter(Query<?> query) {
      return makeFilter(query.getQueryString(), query.getParameters());
   }

   /**
    * Obtain the query factory for building DSL based Ickle queries.
    */
   public static QueryFactory getQueryFactory(Cache<?, ?> cache) {
      AdvancedCache<?, ?> advancedCache = getAdvancedCache(cache);
      QueryEngine<?> queryEngine = SecurityActions.getCacheComponentRegistry(advancedCache).getComponent(QueryEngine.class);
      if (queryEngine == null) {
         throw new IllegalStateException(QueryEngine.class.getName() + " not found in component registry");
      }
      return new EmbeddedQueryFactory(queryEngine);
   }

   private static AdvancedCache<?, ?> getAdvancedCache(Cache<?, ?> cache) {
      if (cache == null) {
         throw new IllegalArgumentException("cache parameter must not be null");
      }
      AdvancedCache<?, ?> advancedCache = cache.getAdvancedCache();
      if (advancedCache == null) {
         throw new IllegalArgumentException("The given cache must expose an AdvancedCache");
      }
      checkBulkReadPermission(advancedCache);
      return advancedCache;
   }

   /**
    * Obtain the {@link ContinuousQuery} object for a cache.
    */
   public static <K, V> ContinuousQuery<K, V> getContinuousQuery(Cache<K, V> cache) {
      return new ContinuousQueryImpl<>(cache);
   }

   private static void checkBulkReadPermission(AdvancedCache<?, ?> cache) {
      AuthorizationManager authorizationManager = SecurityActions.getCacheAuthorizationManager(cache);
      if (authorizationManager != null) {
         authorizationManager.checkPermission(AuthorizationPermission.BULK_READ);
      }
   }

   private static SearchStatsRetriever getStatsRetriever(Cache<?, ?> cache) {
      AdvancedCache<?, ?> advancedCache = getAdvancedCache(cache);
      ComponentRegistry registry = SecurityActions.getCacheComponentRegistry(advancedCache);
      return registry.getComponent(SearchStatsRetriever.class);
   }

   /**
    * @return {@link SearchStatistics} for the Cache.
    */
   public static SearchStatistics getSearchStatistics(Cache<?, ?> cache) {
      return getStatsRetriever(cache).getSearchStatistics();
   }

   /**
    * @return {@link SearchStatistics} for the whole cluster combined. The returned object is a snapshot.
    */
   public static CompletionStage<SearchStatisticsSnapshot> getClusteredSearchStatistics(Cache<?, ?> cache) {
      return getStatsRetriever(cache).getDistributedSearchStatistics();
   }

}
