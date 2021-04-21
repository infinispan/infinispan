package org.infinispan.query.impl;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.hibernate.search.engine.search.query.SearchQuery;
import org.hibernate.search.engine.search.query.SearchResult;
import org.hibernate.search.util.common.SearchException;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.query.SearchTimeoutException;
import org.infinispan.query.core.impl.MappingIterator;
import org.infinispan.query.core.impl.PartitionHandlingSupport;
import org.infinispan.query.core.impl.QueryResultImpl;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.query.dsl.QueryResult;
import org.infinispan.query.dsl.embedded.impl.SearchQueryBuilder;

/**
 * Implementation class of the Lucene based query interface.
 * <p/>
 *
 * @author Navin Surtani
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 * @author Marko Luksa
 */
public class IndexedQueryImpl<E> implements IndexedQuery<E> {

   protected final AdvancedCache<?, ?> cache;
   protected final PartitionHandlingSupport partitionHandlingSupport;
   protected QueryDefinition queryDefinition;
   protected LocalQueryStatistics queryStatistics;
   private static final int SCROLL_CHUNK = 100;

   public IndexedQueryImpl(QueryDefinition queryDefinition, AdvancedCache<?, ?> cache, LocalQueryStatistics queryStatistics) {
      this.queryDefinition = queryDefinition;
      this.cache = cache;
      this.partitionHandlingSupport = new PartitionHandlingSupport(cache);
      this.queryStatistics = queryStatistics;
   }

   /**
    * Create a CacheQueryImpl based on a SearchQuery.
    */
   public IndexedQueryImpl(String queryString, SearchQueryBuilder searchQuery,
                           AdvancedCache<?, ?> cache, LocalQueryStatistics queryStatistics) {
      this(new QueryDefinition(queryString, searchQuery), cache, queryStatistics);
   }

   /**
    * @return The result size of the query.
    */
   public int getResultSize() {
      partitionHandlingSupport.checkCacheAvailable();
      return Math.toIntExact(queryDefinition.getSearchQuery().build().fetchTotalHitCount());
   }

   /**
    * Sets the the result of the given integer value to the first result.
    *
    * @param firstResult index to be set.
    * @throws IllegalArgumentException if the index given is less than zero.
    */
   @Override
   public IndexedQuery<E> firstResult(int firstResult) {
      queryDefinition.setFirstResult(firstResult);
      return this;
   }

   @Override
   public IndexedQuery<E> maxResults(int maxResults) {
      queryDefinition.setMaxResults(maxResults);
      return this;
   }

   private void recordQuery(String q, long took) {
      queryStatistics.localIndexedQueryExecuted(q, took);
   }

   public CloseableIterator<E> iterator() throws SearchException {
      partitionHandlingSupport.checkCacheAvailable();
      SearchQuery<?> searchQuery = queryDefinition.getSearchQuery().build();
      long start = 0;
      if (queryStatistics.isEnabled()) start = System.nanoTime();

      MappingIterator<?, Object> limit = new MappingIterator<>(iterator(searchQuery))
            .skip(queryDefinition.getFirstResult())
            .limit(queryDefinition.getMaxResults());

      if (queryStatistics.isEnabled()) recordQuery(queryDefinition.getQueryString(), System.nanoTime() - start);

      return (CloseableIterator<E>) limit;
   }

   @Override
   public QueryResult<E> execute() {
      try {
         partitionHandlingSupport.checkCacheAvailable();
         SearchQuery<E> searchQuery = (SearchQuery<E>) queryDefinition.getSearchQuery().build();
         long start = 0;
         if (queryStatistics.isEnabled()) start = System.nanoTime();

         SearchResult<E> searchResult = searchQuery.fetch(queryDefinition.getFirstResult(), queryDefinition.getMaxResults());

         if (queryStatistics.isEnabled()) recordQuery(queryDefinition.getQueryString(), System.nanoTime() - start);

         return new QueryResultImpl<>(searchResult.total().hitCount(), searchResult.hits());
      } catch (org.hibernate.search.util.common.SearchTimeoutException timeoutException) {
         throw new SearchTimeoutException();
      }
   }

   @Override
   public List<E> list() throws SearchException {
      return execute().list();
   }

   private CloseableIterator<?> iterator(SearchQuery<?> searchQuery) {
      try {
         return new ScrollerIteratorAdaptor<>(searchQuery.scroll(SCROLL_CHUNK));
      } catch (org.hibernate.search.util.common.SearchTimeoutException timeoutException) {
         throw new SearchTimeoutException();
      }
   }

   @Override
   public IndexedQuery<E> timeout(long timeout, TimeUnit timeUnit) {
      queryDefinition.failAfter(timeout, timeUnit);
      return this;
   }
}
