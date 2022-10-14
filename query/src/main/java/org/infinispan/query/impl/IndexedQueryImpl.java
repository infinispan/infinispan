package org.infinispan.query.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.infinispan.query.logging.Log.CONTAINER;

import org.hibernate.search.backend.lucene.search.query.LuceneSearchQuery;
import org.hibernate.search.engine.search.query.SearchQuery;
import org.hibernate.search.engine.search.query.SearchResult;
import org.hibernate.search.util.common.SearchException;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.query.SearchTimeoutException;
import org.infinispan.query.core.impl.MappingIterator;
import org.infinispan.query.core.impl.PartitionHandlingSupport;
import org.infinispan.query.core.impl.QueryResultImpl;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.query.dsl.QueryResult;
import org.infinispan.query.dsl.embedded.impl.SearchQueryBuilder;
import org.infinispan.search.mapper.common.EntityReference;

/**
 * Lucene based indexed query implementation.
 *
 * @author Navin Surtani
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 * @author Marko Luksa
 */
public class IndexedQueryImpl<E> implements IndexedQuery<E> {

   private static final int SCROLL_CHUNK = 100;

   protected final AdvancedCache<?, ?> cache;
   protected final PartitionHandlingSupport partitionHandlingSupport;
   protected final QueryDefinition queryDefinition;
   protected final LocalQueryStatistics queryStatistics;

   public IndexedQueryImpl(QueryDefinition queryDefinition, AdvancedCache<?, ?> cache, LocalQueryStatistics queryStatistics) {
      this.queryDefinition = queryDefinition;
      this.cache = cache;
      this.partitionHandlingSupport = new PartitionHandlingSupport(cache);
      this.queryStatistics = queryStatistics;
   }

   /**
    * Create a IndexedQueryImpl based on a SearchQuery.
    */
   public IndexedQueryImpl(String queryString, IckleParsingResult.StatementType statementType,
                           SearchQueryBuilder searchQuery, AdvancedCache<?, ?> cache,
                           LocalQueryStatistics queryStatistics, int defaultMaxResults) {
      this(new QueryDefinition(queryString, statementType, searchQuery, defaultMaxResults), cache, queryStatistics);
   }

   /**
    * @return The result size of the query.
    */
   @Override
   public int getResultSize() {
      partitionHandlingSupport.checkCacheAvailable();
      LuceneSearchQuery<?> searchQuery = queryDefinition.getSearchQueryBuilder().build();
      return Math.toIntExact(searchQuery.fetchTotalHitCount());
   }

   /**
    * Sets the result of the given integer value to the first result.
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

   private void recordQuery(long nanos) {
      queryStatistics.localIndexedQueryExecuted(queryDefinition.getQueryString(), nanos);
   }

   @Override
   public CloseableIterator<E> iterator() throws SearchException {
      partitionHandlingSupport.checkCacheAvailable();
      long start = queryStatistics.isEnabled() ? System.nanoTime(): 0;
      SearchQuery<?> searchQuery = queryDefinition.getSearchQueryBuilder().build();

      MappingIterator<?, Object> iterator = new MappingIterator<>(iterator(searchQuery))
            .skip(queryDefinition.getFirstResult())
            .limit(queryDefinition.getMaxResults());

      if (queryStatistics.isEnabled()) recordQuery(System.nanoTime() - start);

      return (CloseableIterator<E>) iterator;
   }

   @Override
   public <K> CloseableIterator<Map.Entry<K, E>> entryIterator() {
      partitionHandlingSupport.checkCacheAvailable();
      long start = queryStatistics.isEnabled() ? System.nanoTime() : 0;

      SearchQueryBuilder searchQueryBuilder = queryDefinition.getSearchQueryBuilder();
      // sanity check: if query has projections other than the entity itself throw an exception
      if (!searchQueryBuilder.isEntityProjection()) {
         throw CONTAINER.entryIteratorDoesNotAllowProjections();
      }

      SearchQuery<List<Object>> searchQuery = searchQueryBuilder.keyAndEntity();

      MappingIterator<List<Object>, Map.Entry<K, E>> iterator = new MappingIterator<>(iterator(searchQuery), this::mapToEntry);
      iterator.skip(queryDefinition.getFirstResult())
            .limit(queryDefinition.getMaxResults());

      if (queryStatistics.isEnabled()) recordQuery(System.nanoTime() - start);

      return iterator;
   }

   private <K, V> Map.Entry<K, V> mapToEntry(List<Object> projection) {
      return new Map.Entry<K, V>() {
         @Override
         public K getKey() {
            // todo [anistor] should also apply keyDataConversion.fromStorage() maybe ?
            return (K) ((EntityReference) projection.get(0)).key();
         }

         @Override
         public V getValue() {
            return (V) projection.get(1);
         }

         @Override
         public V setValue(V value) {
            throw new UnsupportedOperationException("Entry is immutable");
         }
      };
   }

   @Override
   public QueryResult<?> execute() {
      if (queryDefinition.getStatementType() != IckleParsingResult.StatementType.SELECT) {
         return new QueryResultImpl<E>(executeStatement(), Collections.emptyList());
      }

      try {
         partitionHandlingSupport.checkCacheAvailable();

         long start = queryStatistics.isEnabled() ? System.nanoTime() : 0;

         SearchQueryBuilder searchQueryBuilder = queryDefinition.getSearchQueryBuilder();
         SearchQuery<E> searchQuery = (SearchQuery<E>) searchQueryBuilder.build();
         SearchResult<E> searchResult = searchQuery.fetch(queryDefinition.getFirstResult(), queryDefinition.getMaxResults());

         if (queryStatistics.isEnabled()) recordQuery(System.nanoTime() - start);

         return new QueryResultImpl<E>(searchResult.total().hitCount(), searchResult.hits());
      } catch (org.hibernate.search.util.common.SearchTimeoutException timeoutException) {
         throw new SearchTimeoutException();
      }
   }

   @Override
   public int executeStatement() {
      // at the moment the only supported statement is DELETE
      if (queryDefinition.getStatementType() != IckleParsingResult.StatementType.DELETE) {
         throw CONTAINER.unsupportedStatement();
      }

      if (queryDefinition.getFirstResult() != 0 || queryDefinition.isCustomMaxResults()) {
         throw CONTAINER.deleteStatementsCannotUsePaging();
      }

      try {
         partitionHandlingSupport.checkCacheAvailable();

         long start = queryStatistics.isEnabled() ? System.nanoTime() : 0;

         SearchQueryBuilder searchQueryBuilder = queryDefinition.getSearchQueryBuilder();
         LuceneSearchQuery<EntityReference> searchQuery = searchQueryBuilder.entityReference();
         List<EntityReference> hits = searchQuery.fetchAllHits();

         int count = 0;
         for (EntityReference ref : hits) {
            Object removed = cache.remove(ref.key());
            if (removed != null) {
               count++;
            }
         }

         if (queryStatistics.isEnabled()) recordQuery(System.nanoTime() - start);

         return count;
      } catch (org.hibernate.search.util.common.SearchTimeoutException timeoutException) {
         throw new SearchTimeoutException();
      }
   }

   private <T> CloseableIterator<T> iterator(SearchQuery<T> searchQuery) {
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
