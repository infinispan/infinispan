package org.infinispan.query;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverter;
import org.infinispan.objectfilter.impl.ReflectionMatcher;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.impl.EmbeddedQueryFactory;
import org.infinispan.query.dsl.embedded.impl.JPACacheEventFilterConverter;
import org.infinispan.query.dsl.embedded.impl.JPAFilterAndConverter;
import org.infinispan.query.dsl.impl.BaseQuery;
import org.infinispan.query.impl.SearchManagerImpl;
import org.infinispan.query.spi.SearchManagerImplementor;
import org.infinispan.security.AuthorizationManager;
import org.infinispan.security.AuthorizationPermission;

/**
 * Helper class to get a SearchManager out of an indexing enabled cache.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 */
public final class Search {

   public static <K, V, C> CacheEventFilterConverter<K, V, C> makeFilter(Query query) {
      JPAFilterAndConverter<K, V> filterAndConverter = new JPAFilterAndConverter<K, V>(((BaseQuery) query).getJPAQuery(), ReflectionMatcher.class);
      return new JPACacheEventFilterConverter<K, V, C>(filterAndConverter);
   }

   public static QueryFactory getQueryFactory(Cache<?, ?> cache) {
      if (cache == null || cache.getAdvancedCache() == null) {
         throw new IllegalArgumentException("cache parameter shall not be null");
      }
      AdvancedCache<?, ?> advancedCache = cache.getAdvancedCache();
      ensureAccessPermissions(advancedCache);

      if (advancedCache.getCacheConfiguration().indexing().index().isEnabled()) {
         return getSearchManager(advancedCache).unwrap(SearchManagerImplementor.class).getQueryFactory();
      }

      return new EmbeddedQueryFactory(advancedCache);
   }

   public static SearchManager getSearchManager(Cache<?, ?> cache) {
      if (cache == null || cache.getAdvancedCache() == null) {
         throw new IllegalArgumentException("cache parameter shall not be null");
      }
      ensureAccessPermissions(cache.getAdvancedCache());
      return new SearchManagerImpl(cache.getAdvancedCache());
   }

   private static void ensureAccessPermissions(final AdvancedCache<?, ?> cache) {
      AuthorizationManager authorizationManager = SecurityActions.getCacheAuthorizationManager(cache);
      if (authorizationManager != null) {
         authorizationManager.checkPermission(AuthorizationPermission.BULK_READ);
      }
   }
}
