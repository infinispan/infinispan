package org.infinispan.query.impl;

import java.util.concurrent.ExecutorService;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;
import org.hibernate.search.exception.SearchException;
import org.hibernate.search.query.engine.spi.TimeoutExceptionFactory;
import org.hibernate.search.spi.CustomTypeMetadata;
import org.hibernate.search.spi.IndexedTypeIdentifier;
import org.hibernate.search.spi.IndexedTypeMap;
import org.hibernate.search.spi.SearchIntegrator;
import org.hibernate.search.spi.impl.PojoIndexedTypeIdentifier;
import org.hibernate.search.stat.Statistics;
import org.infinispan.AdvancedCache;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.MassIndexer;
import org.infinispan.query.Transformer;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.dsl.embedded.impl.QueryEngine;
import org.infinispan.query.spi.SearchManagerImplementor;

/**
 * Class that is used to build a {@link org.infinispan.query.CacheQuery} based on a Lucene or an Ickle query, only for
 * indexed caches.
 *
 * @author Navin Surtani
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 * @author Marko Luksa
 * @since 4.0
 */
public final class SearchManagerImpl implements SearchManagerImplementor {

   private final AdvancedCache<?, ?> cache;
   private final SearchIntegrator searchFactory;
   private final QueryInterceptor queryInterceptor;
   private final KeyTransformationHandler keyTransformationHandler;
   private final QueryEngine<?> queryEngine;
   private final MassIndexer massIndexer;
   private TimeoutExceptionFactory timeoutExceptionFactory;

   public SearchManagerImpl(AdvancedCache<?, ?> cache) {
      this(cache, ComponentRegistryUtils.getEmbeddedQueryEngine(cache));
   }

   public SearchManagerImpl(AdvancedCache<?, ?> cache, QueryEngine<?> queryEngine) {
      if (cache == null) {
         throw new IllegalArgumentException("cache parameter shall not be null");
      }
      this.cache = cache;
      this.searchFactory = ComponentRegistryUtils.getSearchIntegrator(cache);
      this.queryInterceptor = ComponentRegistryUtils.getQueryInterceptor(cache);
      this.keyTransformationHandler = ComponentRegistryUtils.getKeyTransformationHandler(cache);
      this.queryEngine = queryEngine;
      this.massIndexer = ComponentRegistryUtils.getMassIndexer(cache);
   }

   @Override
   public <E> CacheQuery<E> getQuery(Query luceneQuery, IndexedQueryMode indexedQueryMode, Class<?> entity) {
      return queryEngine.buildCacheQuery(luceneQuery, keyTransformationHandler, timeoutExceptionFactory, entity);
   }

   @Override
   public <E> CacheQuery<E> getQuery(String queryString, IndexedQueryMode indexedQueryMode) {
      ExecutorService asyncExecutor = queryInterceptor.getAsyncExecutor();
      try {
         return queryEngine.buildCacheQuery(queryString, indexedQueryMode, keyTransformationHandler, timeoutExceptionFactory, asyncExecutor);
      } catch (SearchException se) {
         throw new SearchException("'" + queryString + "' cannot be converted to an indexed query", se);
      }
   }

   @Override
   public <E> CacheQuery<E> getQuery(String queryString) {
      return getQuery(queryString, null);
   }

   @Override
   public <E> CacheQuery<E> getQuery(QueryDefinition queryDefinition, IndexedQueryMode indexedQueryMode, IndexedTypeMap<CustomTypeMetadata> indexedTypeMap) {
      ExecutorService asyncExecutor = queryInterceptor.getAsyncExecutor();
      return queryEngine.buildCacheQuery(queryDefinition, indexedQueryMode, keyTransformationHandler, asyncExecutor, indexedTypeMap);
   }

   @Override
   public void registerKeyTransformer(Class<?> keyClass, Class<? extends Transformer> transformerClass) {
      keyTransformationHandler.registerTransformer(keyClass, transformerClass);
   }

   @Override
   public void setTimeoutExceptionFactory(TimeoutExceptionFactory timeoutExceptionFactory) {
      this.timeoutExceptionFactory = timeoutExceptionFactory;
   }

   @Override
   public MassIndexer getMassIndexer() {
      return massIndexer;
   }

   @Override
   public Analyzer getAnalyzer(String name) {
      return searchFactory.getAnalyzer(name);
   }

   @Override
   public Statistics getStatistics() {
      return searchFactory.getStatistics();
   }

   @Override
   public Analyzer getAnalyzer(Class<?> clazz) {
      IndexedTypeIdentifier type = new PojoIndexedTypeIdentifier(clazz);
      return searchFactory.getAnalyzer(type);
   }

   @Override
   public void purge(Class<?> entityType) {
      queryInterceptor.purgeIndex(entityType);
   }

   @Override
   public <T> T unwrap(Class<T> cls) {
      if (SearchIntegrator.class.isAssignableFrom(cls)) {
         return (T) this.searchFactory;
      }
      if (SearchManagerImplementor.class.isAssignableFrom(cls)) {
         return (T) this;
      } else {
         throw new IllegalArgumentException("Cannot unwrap a SearchManagerImpl into a '" + cls.getName() + "'");
      }
   }
}
