package org.infinispan.query.impl;

import java.util.concurrent.ExecutorService;

import org.apache.lucene.search.Query;
import org.hibernate.hql.ast.spi.EntityNamesResolver;
import org.hibernate.search.SearchFactory;
import org.hibernate.search.query.dsl.EntityContext;
import org.hibernate.search.query.engine.spi.TimeoutExceptionFactory;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.AdvancedCache;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.MassIndexer;
import org.infinispan.query.Transformer;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.clustered.ClusteredCacheQueryImpl;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.LuceneQuery;
import org.infinispan.query.dsl.embedded.impl.EmbeddedLuceneQueryFactory;
import org.infinispan.query.impl.massindex.MapReduceMassIndexer;
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
   private final SearchFactoryIntegrator searchFactory;
   private final QueryInterceptor queryInterceptor;
   private TimeoutExceptionFactory timeoutExceptionFactory;

   public SearchManagerImpl(AdvancedCache<?, ?> cache) {
      if (cache == null) {
         throw new IllegalArgumentException("cache parameter shall not be null");
      }
      this.cache = cache;
      this.searchFactory = ComponentRegistryUtils.getComponent(cache, SearchFactoryIntegrator.class);
      this.queryInterceptor = ComponentRegistryUtils.getQueryInterceptor(cache);
   }

   @Override
   public QueryFactory<LuceneQuery> getQueryFactory() {
      EntityNamesResolver entityNamesResolver = new EntityNamesResolver() {
         @Override
         public Class<?> getClassFromName(String entityName) {
            Class clazz;
            try {
               clazz = Class.forName(entityName);  //todo [anistor] any potential class loading issues in AS ?
            } catch (ClassNotFoundException e) {
               return null;
            }
            return queryInterceptor.isIndexed(clazz) ? clazz : null;
         }
      };
      return new EmbeddedLuceneQueryFactory(this, entityNamesResolver);
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
   @Override
   public SearchFactory getSearchFactory() {
      return searchFactory;
   }

   @Override
   public MassIndexer getMassIndexer() {
      // TODO: Should a new instance be created every time?
      return new MapReduceMassIndexer(cache, searchFactory);
   }

}
