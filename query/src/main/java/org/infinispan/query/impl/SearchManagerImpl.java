package org.infinispan.query.impl;

import java.util.concurrent.ExecutorService;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;
import org.hibernate.search.query.dsl.EntityContext;
import org.hibernate.search.query.engine.spi.TimeoutExceptionFactory;
import org.hibernate.search.spi.SearchIntegrator;
import org.hibernate.search.stat.Statistics;
import org.infinispan.AdvancedCache;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.MassIndexer;
import org.infinispan.query.Transformer;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.clustered.ClusteredCacheQueryImpl;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.LuceneQuery;
import org.infinispan.query.dsl.embedded.impl.EmbeddedLuceneQueryFactory;
import org.infinispan.query.dsl.embedded.impl.QueryEngine;
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
public class SearchManagerImpl implements SearchManagerImplementor {

   private final AdvancedCache<?, ?> cache;
   private final SearchIntegrator searchFactory;
   private final QueryInterceptor queryInterceptor;
   private final QueryEngine queryEngine;
   private TimeoutExceptionFactory timeoutExceptionFactory;

   public SearchManagerImpl(AdvancedCache<?, ?> cache) {
      if (cache == null) {
         throw new IllegalArgumentException("cache parameter shall not be null");
      }
      this.cache = cache;
      this.searchFactory = ComponentRegistryUtils.getComponent(cache, SearchIntegrator.class);
      this.queryInterceptor = ComponentRegistryUtils.getQueryInterceptor(cache);
      queryEngine = new QueryEngine(cache, this);
   }

   //TODO remove in 8.0
   @Override
   public QueryFactory<LuceneQuery> getQueryFactory() {
      return new EmbeddedLuceneQueryFactory(queryEngine);
   }

   /* (non-Javadoc)
    * @see org.infinispan.query.SearchManager#getQuery(org.apache.lucene.search.Query, java.lang.Class)
    */
   @Override
   public CacheQuery getQuery(Query luceneQuery, Class<?>... classes) {
      queryInterceptor.enableClasses(classes);
      return new CacheQueryImpl(luceneQuery, searchFactory, cache,
         queryInterceptor.getKeyTransformationHandler(), timeoutExceptionFactory, classes);
   }

   /**
    *
    * This probably should be hided in the getQuery method.
    *
    * @param luceneQuery
    * @param classes
    * @return
    */
   @Override
   public CacheQuery getClusteredQuery(Query luceneQuery, Class<?>... classes) {
      queryInterceptor.enableClasses(classes);
      ExecutorService asyncExecutor = queryInterceptor.getAsyncExecutor();
      return new ClusteredCacheQueryImpl(luceneQuery, searchFactory, asyncExecutor, cache, queryInterceptor.getKeyTransformationHandler(), classes);
   }

   @Override
   public void registerKeyTransformer(Class<?> keyClass, Class<? extends Transformer> transformerClass) {
      queryInterceptor.registerKeyTransformer(keyClass, transformerClass);
   }

   @Override
   public void setTimeoutExceptionFactory(TimeoutExceptionFactory timeoutExceptionFactory) {
      this.timeoutExceptionFactory = timeoutExceptionFactory;
   }

   /* (non-Javadoc)
    * @see org.infinispan.query.SearchManager#buildQueryBuilderForClass(java.lang.Class)
    */
   @Override
   public EntityContext buildQueryBuilderForClass(Class<?> entityType) {
      queryInterceptor.enableClasses(new Class[] { entityType });
      return searchFactory.buildQueryBuilder().forEntity(entityType);
   }

   /* (non-Javadoc)
    * @see org.infinispan.query.SearchManager#getSearchFactory()
    */
   @Override @Deprecated
   public SearchIntegrator getSearchFactory() {
      return searchFactory;
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
      return searchFactory.getAnalyzer(clazz);
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
      if (QueryEngine.class.isAssignableFrom(cls)) {
         return (T) this.queryEngine;
      }
      if (SearchManagerImplementor.class.isAssignableFrom(cls)) {
         return (T) this;
      }
      else {
         throw new IllegalArgumentException("Can not unwrap a SearchManagerImpl into a '" + cls + "'");
      }
   }

}
