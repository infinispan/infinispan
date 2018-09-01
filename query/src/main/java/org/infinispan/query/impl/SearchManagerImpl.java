package org.infinispan.query.impl;

import java.util.concurrent.ExecutorService;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;
import org.hibernate.search.exception.SearchException;
import org.hibernate.search.query.dsl.EntityContext;
import org.hibernate.search.query.engine.spi.HSQuery;
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
import org.infinispan.query.clustered.ClusteredCacheQueryImpl;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.dsl.embedded.impl.EmbeddedQueryEngine;
import org.infinispan.query.impl.massindex.DistributedExecutorMassIndexer;
import org.infinispan.query.spi.SearchManagerImplementor;

/**
 * Class that is used to build {@link org.infinispan.query.CacheQuery}
 *
 * @author Navin Surtani
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @author Marko Luksa
 * @since 4.0
 */
public final class SearchManagerImpl implements SearchManagerImplementor {

   private final AdvancedCache<?, ?> cache;
   private final SearchIntegrator searchFactory;
   private final QueryInterceptor queryInterceptor;
   private final EmbeddedQueryEngine queryEngine;
   private TimeoutExceptionFactory timeoutExceptionFactory;

   public SearchManagerImpl(AdvancedCache<?, ?> cache) {
      if (cache == null) {
         throw new IllegalArgumentException("cache parameter shall not be null");
      }
      this.cache = cache;
      this.searchFactory = ComponentRegistryUtils.getSearchIntegrator(cache);
      this.queryInterceptor = ComponentRegistryUtils.getQueryInterceptor(cache);
      this.queryEngine = ComponentRegistryUtils.getEmbeddedQueryEngine(cache);
   }

   @Override
   public <E> CacheQuery<E> getQuery(Query luceneQuery, IndexedQueryMode indexedQueryMode, Class<?>... classes) {
      queryInterceptor.enableClasses(classes);
      KeyTransformationHandler keyTransformationHandler = queryInterceptor.getKeyTransformationHandler();
      ExecutorService asyncExecutor = queryInterceptor.getAsyncExecutor();
      return queryEngine.buildCacheQuery(luceneQuery, indexedQueryMode, keyTransformationHandler, timeoutExceptionFactory, asyncExecutor, classes);
   }

   @Override
   public <E> CacheQuery<E> getQuery(String queryString, IndexedQueryMode indexedQueryMode, Class<?>... classes) {
      queryInterceptor.enableClasses(classes);
      KeyTransformationHandler keyTransformationHandler = queryInterceptor.getKeyTransformationHandler();
      ExecutorService asyncExecutor = queryInterceptor.getAsyncExecutor();
      try {
         return queryEngine.buildCacheQuery(queryString, indexedQueryMode, keyTransformationHandler, timeoutExceptionFactory, asyncExecutor, classes);
      } catch (SearchException se) {
         throw new SearchException(queryString + " cannot be converted to an indexed Query", se);
      }
   }

   @Override
   public <E> CacheQuery<E> getQuery(QueryDefinition queryDefinition, IndexedQueryMode indexedQueryMode, IndexedTypeMap<CustomTypeMetadata> indexedTypeMap) {
      KeyTransformationHandler keyTransformationHandler = queryInterceptor.getKeyTransformationHandler();
      ExecutorService asyncExecutor = queryInterceptor.getAsyncExecutor();
      return queryEngine.buildCacheQuery(queryDefinition, indexedQueryMode, keyTransformationHandler, timeoutExceptionFactory, asyncExecutor, indexedTypeMap);
   }

   @Override
   public <E> CacheQuery<E> getQuery(Query luceneQuery, Class<?>... classes) {
      return getQuery(luceneQuery, IndexedQueryMode.FETCH, classes);
   }

   /**
    * Internal and experimental! Creates a {@link CacheQuery}, filtered according to the given {@link HSQuery}.
    *
    * @param hSearchQuery {@link HSQuery}
    * @return the CacheQuery object which can be used to iterate through results
    */
   public <E> CacheQuery<E> getQuery(HSQuery hSearchQuery, IndexedQueryMode queryMode) {
      if (timeoutExceptionFactory != null) {
         hSearchQuery.timeoutExceptionFactory(timeoutExceptionFactory);
      }
      Class<?>[] classes = hSearchQuery.getTargetedEntities().toPojosSet().toArray(new Class[hSearchQuery.getTargetedEntities().size()]);
      queryInterceptor.enableClasses(classes);

      if (queryMode == IndexedQueryMode.BROADCAST) {
         ExecutorService asyncExecutor = queryInterceptor.getAsyncExecutor();
         return new ClusteredCacheQueryImpl<>(new QueryDefinition(hSearchQuery), asyncExecutor, cache, queryInterceptor.getKeyTransformationHandler(), null);
      } else {
         return new CacheQueryImpl<>(hSearchQuery, cache, queryInterceptor.getKeyTransformationHandler());
      }
   }

   @Override
   public <E> CacheQuery<E> getClusteredQuery(Query luceneQuery, Class<?>... classes) {
      return getQuery(luceneQuery, IndexedQueryMode.BROADCAST, classes);
   }

   @Override
   public void registerKeyTransformer(Class<?> keyClass, Class<? extends Transformer> transformerClass) {
      queryInterceptor.registerKeyTransformer(keyClass, transformerClass);
   }

   @Override
   public void setTimeoutExceptionFactory(TimeoutExceptionFactory timeoutExceptionFactory) {
      this.timeoutExceptionFactory = timeoutExceptionFactory;
   }

   @Override
   public EntityContext buildQueryBuilderForClass(Class<?> entityType) {
      queryInterceptor.enableClasses(new Class[]{entityType});
      return searchFactory.buildQueryBuilder().forEntity(entityType);
   }

   @Override
   public MassIndexer getMassIndexer() {
      // TODO: Should a new instance be created every time?
      return new DistributedExecutorMassIndexer(cache, searchFactory);
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
