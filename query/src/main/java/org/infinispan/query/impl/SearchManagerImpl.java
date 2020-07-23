package org.infinispan.query.impl;

import java.util.concurrent.ExecutorService;

import org.hibernate.search.util.common.SearchException;
import org.infinispan.AdvancedCache;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.MassIndexer;
import org.infinispan.query.SearchManager;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.dsl.embedded.impl.QueryEngine;
import org.infinispan.query.dsl.embedded.impl.SearchQueryBuilder;
import org.infinispan.search.mapper.mapping.SearchMappingHolder;

/**
 * Class that is used to build a {@link org.infinispan.query.CacheQuery} based on a Lucene or an Ickle query, only for
 * indexed caches.
 *
 * @author Navin Surtani
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 * @author Marko Luksa
 * @since 4.0
 */
public final class SearchManagerImpl implements SearchManager {

   private final SearchMappingHolder searchMappingHolder;
   private final QueryInterceptor queryInterceptor;
   private final KeyTransformationHandler keyTransformationHandler;
   private final QueryEngine<?> queryEngine;
   private final MassIndexer massIndexer;

   public SearchManagerImpl(AdvancedCache<?, ?> cache, QueryEngine<?> queryEngine) {
      if (cache == null) {
         throw new IllegalArgumentException("cache parameter shall not be null");
      }
      this.searchMappingHolder = ComponentRegistryUtils.getSearchMappingHolder(cache);
      this.queryInterceptor = ComponentRegistryUtils.getQueryInterceptor(cache);
      this.keyTransformationHandler = ComponentRegistryUtils.getKeyTransformationHandler(cache);
      this.queryEngine = queryEngine;
      this.massIndexer = (MassIndexer) ComponentRegistryUtils.getIndexer(cache);
   }

   public <E> CacheQuery<E> getQuery(SearchQueryBuilder searchQuery) {
      return queryEngine.buildCacheQuery(searchQuery);
   }

   @Override
   public <E> CacheQuery<E> getQuery(String queryString, IndexedQueryMode indexedQueryMode) {
      ExecutorService asyncExecutor = queryInterceptor.getAsyncExecutor();
      try {
         return queryEngine.buildCacheQuery(queryString, indexedQueryMode, keyTransformationHandler, asyncExecutor);
      } catch (SearchException se) {
         throw new SearchException("'" + queryString + "' cannot be converted to an indexed query", se);
      }
   }

   @Override
   public <E> CacheQuery<E> getQuery(String queryString) {
      return getQuery(queryString, null);
   }

   public <E> CacheQuery<E> getQuery(QueryDefinition queryDefinition, IndexedQueryMode indexedQueryMode) {
      ExecutorService asyncExecutor = queryInterceptor.getAsyncExecutor();
      return queryEngine.buildCacheQuery(queryDefinition, indexedQueryMode, asyncExecutor);
   }

   @Override
   public MassIndexer getMassIndexer() {
      return massIndexer;
   }

   @Override
   public void purge(Class<?> entityType) {
      queryInterceptor.purgeIndex(entityType);
   }

   @Override
   public <T> T unwrap(Class<T> cls) {
      if (SearchMappingHolder.class.isAssignableFrom(cls)) {
         return (T) this.searchMappingHolder;
      }
      if (SearchManagerImpl.class.isAssignableFrom(cls)) {
         return (T) this;
      } else {
         throw new IllegalArgumentException("Cannot unwrap a SearchManagerImpl into a '" + cls.getName() + "'");
      }
   }
}
