package org.infinispan.query.impl;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.hibernate.search.exception.SearchException;
import org.hibernate.search.filter.FullTextFilter;
import org.hibernate.search.query.engine.spi.DocumentExtractor;
import org.hibernate.search.query.engine.spi.EntityInfo;
import org.hibernate.search.query.engine.spi.FacetManager;
import org.hibernate.search.query.engine.spi.HSQuery;
import org.hibernate.search.query.engine.spi.TimeoutExceptionFactory;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.AdvancedCache;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.FetchOptions;
import org.infinispan.query.FetchOptions.FetchMode;
import org.infinispan.query.ResultIterator;
import org.infinispan.query.backend.KeyTransformationHandler;

/**
 * Implementation class of the CacheQuery interface.
 *
 * @author Navin Surtani
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 * @author Marko Luksa
 */
public class CacheQueryImpl<E> implements CacheQuery<E> {

   /**
    * Since CacheQuery extends {@link Iterable} it is possible to implicitly invoke
    * {@link #iterator()} in an "enhanced for loop".
    * When using the {@link FetchMode#LAZY} it is mandatory to close the {@link ResultIterator},
    * but users of the enhanced loop have no chance to invoke the method.
    * Therefore, it's important that the default fetch options use EAGER iteration.
    */
   private static final FetchOptions DEFAULT_FETCH_OPTIONS = new FetchOptions().fetchMode(FetchMode.EAGER);

   protected final AdvancedCache<?, ?> cache;
   protected final KeyTransformationHandler keyTransformationHandler;
   protected final PartitionHandlingSupport partitionHandlingSupport;
   protected QueryDefinition queryDefinition;
   private ProjectionConverter projectionConverter;

   /**
    * Create a CacheQueryImpl based on a Lucene query.
    */
   public CacheQueryImpl(Query luceneQuery, SearchIntegrator searchFactory, AdvancedCache<?, ?> cache,
                         KeyTransformationHandler keyTransformationHandler, TimeoutExceptionFactory timeoutExceptionFactory,
                         Class<?>... classes) {
      this(timeoutExceptionFactory == null ? searchFactory.createHSQuery(luceneQuery, classes) :
                  searchFactory.createHSQuery(luceneQuery, classes).timeoutExceptionFactory(timeoutExceptionFactory),
            cache, keyTransformationHandler);
   }

   public CacheQueryImpl(QueryDefinition queryDefinition, AdvancedCache<?, ?> cache,
                         KeyTransformationHandler keyTransformationHandler) {
      this.queryDefinition = queryDefinition;
      this.cache = cache;
      this.keyTransformationHandler = keyTransformationHandler;
      this.partitionHandlingSupport = new PartitionHandlingSupport(cache);
   }

   /**
    * Create a CacheQueryImpl based on a HSQuery.
    */
   public CacheQueryImpl(HSQuery hSearchQuery, AdvancedCache<?, ?> cache, KeyTransformationHandler keyTransformationHandler) {
      this(new QueryDefinition(hSearchQuery), cache, keyTransformationHandler);
   }

   /**
    * Takes in a lucene filter and sets it to the filter field in the class.
    *
    * @param filter - lucene filter
    */
   @Override
   public CacheQuery<E> filter(Filter filter) {
      queryDefinition.filter(filter);
      return this;
   }

   /**
    * @return The result size of the query.
    */
   @Override
   public int getResultSize() {
      partitionHandlingSupport.checkCacheAvailable();
      return queryDefinition.getHsQuery().queryResultSize();
   }

   @Override
   public CacheQuery<E> sort(Sort sort) {
      queryDefinition.setSort(sort);
      return this;
   }

   /**
    * Enable a given filter by its name.
    *
    * @param name of filter.
    * @return a FullTextFilter object.
    */
   @Override
   public FullTextFilter enableFullTextFilter(String name) {
      return queryDefinition.enableFullTextFilter(name);
   }

   /**
    * Disable a given filter by its name.
    *
    * @param name of filter.
    */
   @Override
   public CacheQuery<E> disableFullTextFilter(String name) {
      queryDefinition.disableFullTextFilter(name);
      return this;
   }

   /**
    * Sets the the result of the given integer value to the first result.
    *
    * @param firstResult index to be set.
    * @throws IllegalArgumentException if the index given is less than zero.
    */
   @Override
   public CacheQuery<E> firstResult(int firstResult) {
      queryDefinition.setFirstResult(firstResult);
      return this;
   }

   @Override
   public CacheQuery<E> maxResults(int maxResults) {
      queryDefinition.setMaxResults(maxResults);
      return this;
   }

   @Override
   public ResultIterator<E> iterator() throws SearchException {
      return iterator(DEFAULT_FETCH_OPTIONS);
   }

   @Override
   public ResultIterator<E> iterator(FetchOptions fetchOptions) throws SearchException {
      partitionHandlingSupport.checkCacheAvailable();
      HSQuery hSearchQuery = queryDefinition.getHsQuery();
      if (fetchOptions.getFetchMode() == FetchOptions.FetchMode.EAGER) {
         hSearchQuery.getTimeoutManager().start();
         List<EntityInfo> entityInfos = hSearchQuery.queryEntityInfos();
         return filterNulls(new EagerIterator<>(entityInfos, getResultLoader(hSearchQuery), fetchOptions.getFetchSize()));
      } else if (fetchOptions.getFetchMode() == FetchOptions.FetchMode.LAZY) {
         DocumentExtractor extractor = hSearchQuery.queryDocumentExtractor();   //triggers actual Lucene search
         return filterNulls(new LazyIterator<>(extractor, getResultLoader(hSearchQuery), fetchOptions.getFetchSize()));
      } else {
         throw new IllegalArgumentException("Unknown FetchMode " + fetchOptions.getFetchMode());
      }
   }

   private ResultIterator<E> filterNulls(ResultIterator<E> iterator) {
      return new NullFilteringResultIterator<>(iterator);
   }

   @Override
   public List<E> list() throws SearchException {
      partitionHandlingSupport.checkCacheAvailable();
      HSQuery hSearchQuery = queryDefinition.getHsQuery();
      hSearchQuery.getTimeoutManager().start();
      List<EntityInfo> entityInfos = hSearchQuery.queryEntityInfos();
      return (List<E>) getResultLoader(hSearchQuery).load(entityInfos);
   }

   private QueryResultLoader getResultLoader(HSQuery hSearchQuery) {
      EntityLoader entityLoader = new EntityLoader(cache, keyTransformationHandler);
      return hSearchQuery.getProjectedFields() == null ? entityLoader : new ProjectionLoader(projectionConverter, entityLoader);
   }

   @Override
   public FacetManager getFacetManager() {
      return queryDefinition.getHsQuery().getFacetManager();
   }

   @Override
   public Explanation explain(int documentId) {
      return queryDefinition.getHsQuery().explain(documentId);
   }

   @Override
   public CacheQuery<Object[]> projection(String... fields) {
      projectionConverter = new ProjectionConverter(fields, keyTransformationHandler);
      queryDefinition.getHsQuery().projection(projectionConverter.getHSearchProjection());
      return (CacheQuery<Object[]>) this;
   }

   @Override
   public CacheQuery<E> timeout(long timeout, TimeUnit timeUnit) {
      queryDefinition.getHsQuery().getTimeoutManager().setTimeout(timeout, timeUnit);
      return this;
   }
}
