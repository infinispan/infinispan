package org.infinispan.query;

import java.util.Map;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverter;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.impl.ReflectionMatcher;
import org.infinispan.query.api.continuous.ContinuousQuery;
import org.infinispan.query.continuous.impl.ContinuousQueryImpl;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.impl.EmbeddedQueryEngine;
import org.infinispan.query.dsl.embedded.impl.EmbeddedQueryFactory;
import org.infinispan.query.dsl.embedded.impl.IckleCacheEventFilterConverter;
import org.infinispan.query.dsl.embedded.impl.IckleFilterAndConverter;
import org.infinispan.query.impl.SearchManagerImpl;
import org.infinispan.query.logging.Log;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.util.logging.LogFactory;

/**
 * Helper class to get a SearchManager out of an indexing enabled cache.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @author anistor@redhat.com
 */
public final class Search {

   private static final Log log = LogFactory.getLog(Search.class, Log.class);

   private Search() {
   }

   public static <K, V> CacheEventFilterConverter<K, V, ObjectFilter.FilterResult> makeFilter(String queryString) {
      return makeFilter(queryString, null);
   }

   public static <K, V> CacheEventFilterConverter<K, V, ObjectFilter.FilterResult> makeFilter(String queryString, Map<String, Object> namedParameters) {
      IckleFilterAndConverter<K, V> filterAndConverter = new IckleFilterAndConverter<>(queryString, namedParameters, ReflectionMatcher.class);
      return new IckleCacheEventFilterConverter<>(filterAndConverter);
   }

   public static <K, V> CacheEventFilterConverter<K, V, ObjectFilter.FilterResult> makeFilter(Query query) {
      return makeFilter(query.getQueryString(), query.getParameters());
   }

   public static QueryFactory getQueryFactory(Cache<?, ?> cache) {
      if (cache == null || cache.getAdvancedCache() == null) {
         throw new IllegalArgumentException("cache parameter shall not be null");
      }
      AdvancedCache<?, ?> advancedCache = cache.getAdvancedCache();
      ensureAccessPermissions(advancedCache);
      EmbeddedQueryEngine queryEngine = SecurityActions.getCacheComponentRegistry(advancedCache).getComponent(EmbeddedQueryEngine.class);
      if (queryEngine == null) {
         throw log.queryModuleNotInitialised();
      }
      return new EmbeddedQueryFactory(queryEngine);
   }

   public static <K, V> ContinuousQuery<K, V> getContinuousQuery(Cache<K, V> cache) {
      return new ContinuousQueryImpl<>(cache);
   }

   public static SearchManager getSearchManager(Cache<?, ?> cache) {
      if (cache == null || cache.getAdvancedCache() == null) {
         throw new IllegalArgumentException("cache parameter shall not be null");
      }
      AdvancedCache<?, ?> advancedCache = cache.getAdvancedCache();
      ensureAccessPermissions(advancedCache);
      return new SearchManagerImpl(advancedCache);
   }

   private static void ensureAccessPermissions(final AdvancedCache<?, ?> cache) {
      AuthorizationManager authorizationManager = SecurityActions.getCacheAuthorizationManager(cache);
      if (authorizationManager != null) {
         authorizationManager.checkPermission(AuthorizationPermission.BULK_READ);
      }
   }
}
