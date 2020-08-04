package org.infinispan.query;

import java.util.Map;
import java.util.Objects;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverter;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.query.api.continuous.ContinuousQuery;
import org.infinispan.query.core.impl.EmbeddedQueryFactory;
import org.infinispan.query.core.impl.continuous.ContinuousQueryImpl;
import org.infinispan.query.core.impl.eventfilter.IckleCacheEventFilterConverter;
import org.infinispan.query.core.impl.eventfilter.IckleFilterAndConverter;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.impl.ObjectReflectionMatcher;
import org.infinispan.query.dsl.embedded.impl.QueryEngine;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;

/**
 * This is the entry point for the Infinispan search API. It provides the {@link QueryFactory} which is your
 * starting point for building Ickle queries, continuous queries and event filters, for both indexed and unindexed caches.
 *
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 * @author anistor@redhat.com
 */
public final class Search {

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
      IckleFilterAndConverter<K, V> filterAndConverter = new IckleFilterAndConverter<>(queryString, namedParameters, ObjectReflectionMatcher.class);
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
      if (cache == null) {
         throw new IllegalArgumentException("cache parameter must not be null");
      }
      AdvancedCache<?, ?> advancedCache = cache.getAdvancedCache();
      if (advancedCache == null) {
         throw new IllegalArgumentException("The given cache must expose an AdvancedCache");
      }
      checkBulkReadPermission(advancedCache);
      QueryEngine<?> queryEngine = ComponentRegistryUtils.getEmbeddedQueryEngine(advancedCache);
      return new EmbeddedQueryFactory(queryEngine);
   }

   /**
    * Obtain the {@link ContinuousQuery} object for a cache.
    */
   public static <K, V> ContinuousQuery<K, V> getContinuousQuery(Cache<K, V> cache) {
      return new ContinuousQueryImpl<>(cache);
   }

   /**
    * @return Obtain the {@link Indexer} instance for the cache.
    * @since 11.0
    */
   public static <K, V> Indexer getIndexer(Cache<K, V> cache) {
      AdvancedCache<K, V> advancedCache = Objects.requireNonNull(cache, "cache parameter must not be null").getAdvancedCache();
      if (advancedCache == null) {
         throw new IllegalArgumentException("The given cache must expose an AdvancedCache interface");
      }
      checkBulkReadPermission(advancedCache);
      return ComponentRegistryUtils.getIndexer(advancedCache);
   }

   private static void checkBulkReadPermission(AdvancedCache<?, ?> cache) {
      AuthorizationManager authorizationManager = SecurityActions.getCacheAuthorizationManager(cache);
      if (authorizationManager != null) {
         authorizationManager.checkPermission(AuthorizationPermission.BULK_READ);
      }
   }
}
