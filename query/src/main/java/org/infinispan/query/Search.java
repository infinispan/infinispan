package org.infinispan.query;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverter;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.query.api.continuous.ContinuousQuery;
import org.infinispan.query.core.impl.EmbeddedQueryFactory;
import org.infinispan.query.core.impl.continuous.ContinuousQueryImpl;
import org.infinispan.query.core.impl.eventfilter.IckleCacheEventFilterConverter;
import org.infinispan.query.core.impl.eventfilter.IckleFilterAndConverter;
import org.infinispan.query.core.stats.SearchStatistics;
import org.infinispan.query.core.stats.SearchStatisticsSnapshot;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.impl.ObjectReflectionMatcher;
import org.infinispan.query.dsl.embedded.impl.QueryEngine;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;

/**
 * Entry point for performing Infinispan queries.
 * Provides the {@link QueryFactory} that you use to build Ickle queries, continuous queries, and event filters for indexed and non-indexed caches.
 *
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 * @author anistor@redhat.com
 */
public final class Search {

   private Search() {
      // prevent instantiation
   }

   /**
    * Creates an event filter from an Ickle query string.
    */
   public static <K, V> CacheEventFilterConverter<K, V, ObjectFilter.FilterResult> makeFilter(String queryString) {
      return makeFilter(queryString, null);
   }

   /**
    * Creates event filters from Ickle query strings.
    */
   public static <K, V> CacheEventFilterConverter<K, V, ObjectFilter.FilterResult> makeFilter(String queryString, Map<String, Object> namedParameters) {
      IckleFilterAndConverter<K, V> filterAndConverter = new IckleFilterAndConverter<>(queryString, namedParameters, ObjectReflectionMatcher.class);
      return new IckleCacheEventFilterConverter<>(filterAndConverter);
   }

   /**
    * Creates event filters from Ickle query strings.
    */
   public static <K, V> CacheEventFilterConverter<K, V, ObjectFilter.FilterResult> makeFilter(Query<?> query) {
      return makeFilter(query.getQueryString(), query.getParameters());
   }

   /**
    * Obtains a query factory to build DSL-based Ickle queries.
    */
   public static QueryFactory getQueryFactory(Cache<?, ?> cache) {
      if (cache == null) {
         throw new IllegalArgumentException("The cache parameter cannot be null.");
      }
      AdvancedCache<?, ?> advancedCache = cache.getAdvancedCache();
      if (advancedCache == null) {
         throw new IllegalArgumentException("The given cache must expose an AdvancedCache interface.");
      }
      checkBulkReadPermission(advancedCache);
      QueryEngine<?> queryEngine = ComponentRegistryUtils.getEmbeddedQueryEngine(advancedCache);
      return new EmbeddedQueryFactory(queryEngine);
   }

   /**
    * Obtains the {@link ContinuousQuery} object for the cache.
    */
   public static <K, V> ContinuousQuery<K, V> getContinuousQuery(Cache<K, V> cache) {
      return new ContinuousQueryImpl<>(cache);
   }

   private static <K, V> AdvancedCache<K, V> getAdvancedCache(Cache<K, V> cache) {
      AdvancedCache<K, V> advancedCache = Objects.requireNonNull(cache, "The cache parameter cannot be null.").getAdvancedCache();
      if (advancedCache == null) {
         throw new IllegalArgumentException("The given cache must expose an AdvancedCache interface.");
      }
      checkBulkReadPermission(advancedCache);
      return advancedCache;
   }

   /**
    * @return Obtains the {@link Indexer} instance for the cache.
    * @since 11.0
    */
   public static <K, V> Indexer getIndexer(Cache<K, V> cache) {
      AdvancedCache<K, V> advancedCache = getAdvancedCache(cache);
      return ComponentRegistryUtils.getIndexer(advancedCache);
   }

   private static void checkBulkReadPermission(AdvancedCache<?, ?> cache) {
      AuthorizationManager authorizationManager = SecurityActions.getCacheAuthorizationManager(cache);
      if (authorizationManager != null) {
         authorizationManager.checkPermission(AuthorizationPermission.BULK_READ);
      }
   }

   /**
    * Returns search statistics for the local node.
    */
   public static <K, V> SearchStatistics getSearchStatistics(Cache<K, V> cache) {
      return ComponentRegistryUtils.getSearchStatsRetriever(cache).getSearchStatistics();
   }

   /**
    * Returns aggregated search statistics for all nodes in the cluster.
    */
   public static CompletionStage<SearchStatisticsSnapshot> getClusteredSearchStatistics(Cache<?, ?> cache) {
      return ComponentRegistryUtils.getSearchStatsRetriever(cache).getDistributedSearchStatistics();
   }
}
