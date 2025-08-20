package org.infinispan.query.impl;

import static java.lang.Float.NaN;
import static org.infinispan.query.core.impl.Log.CONTAINER;
import static org.infinispan.query.dsl.embedded.impl.SearchQueryBuilder.INFINISPAN_AGGREGATION_KEY_NAME;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.lucene.search.Sort;
import org.hibernate.search.backend.lucene.search.query.LuceneSearchQuery;
import org.hibernate.search.engine.common.EntityReference;
import org.hibernate.search.engine.search.aggregation.AggregationKey;
import org.hibernate.search.engine.search.query.SearchQuery;
import org.hibernate.search.engine.search.query.SearchResult;
import org.hibernate.search.util.common.SearchException;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.api.query.ClosableIteratorWithCount;
import org.infinispan.commons.api.query.EntityEntry;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.query.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.query.SearchTimeoutException;
import org.infinispan.query.core.impl.MappingIterator;
import org.infinispan.query.core.impl.PartitionHandlingSupport;
import org.infinispan.query.core.impl.QueryResultImpl;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.query.dsl.QueryResult;
import org.infinispan.query.dsl.TotalHitCount;
import org.infinispan.query.dsl.embedded.impl.SearchQueryBuilder;

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

   @Override
   public IndexedQuery<E> hitCountAccuracy(int hitCountAccuracy) {
      queryDefinition.setHitCountAccuracy(hitCountAccuracy);
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
   public <K> ClosableIteratorWithCount<EntityEntry<K, E>> entryIterator(boolean withMetadata) {
      partitionHandlingSupport.checkCacheAvailable();
      long start = queryStatistics.isEnabled() ? System.nanoTime() : 0;

      SearchQueryBuilder searchQueryBuilder = queryDefinition.getSearchQueryBuilder();
      // sanity check: if query has projections other than the entity itself throw an exception
      if (!searchQueryBuilder.isEntityProjection()) {
         throw CONTAINER.entryIteratorDoesNotAllowProjections();
      }

      SearchQuery<List<Object>> searchQuery = (queryDefinition.isScoreRequired()) ?
            searchQueryBuilder.keyEntityAndScore(withMetadata) : searchQueryBuilder.keyAndEntity(withMetadata);

      ClosableIteratorWithCount<List<Object>> closableIteratorWithCount = iterator(searchQuery);
      MappingIterator<List<Object>, EntityEntry<K, E>> iterator =
            new MappingIterator<>(closableIteratorWithCount, this::mapToEntry, closableIteratorWithCount.count());
      iterator.skip(queryDefinition.getFirstResult())
            .limit(queryDefinition.getMaxResults());

      if (queryStatistics.isEnabled()) recordQuery(System.nanoTime() - start);

      return iterator;
   }

   private <K, V> EntityEntry<K, V> mapToEntry(List<Object> projection) {
      float score = (projection.size() > 2) ? (float) projection.get(2) : NaN;
      EntityReference entityReference = (EntityReference) projection.get(0);
      EntityLoaded<V> entityLoaded = (EntityLoaded<V>) projection.get(1);
      return new EntityEntry<>((K) entityReference.id(),
            entityLoaded.entity(), score, entityLoaded.metadata());
   }

   @Override
   public QueryResult<?> execute() {
      if (queryDefinition.getStatementType() != IckleParsingResult.StatementType.SELECT) {
         return new QueryResultImpl<E>(executeStatement(), Collections.emptyList());
      }

      try {
         partitionHandlingSupport.checkCacheAvailable();
         SearchQueryBuilder searchQueryBuilder = queryDefinition.getSearchQueryBuilder();
         if (searchQueryBuilder.aggregation() != null) {
            return aggregation();
         }

         long start = queryStatistics.isEnabled() ? System.nanoTime() : 0;
         SearchQuery<?> searchQuery = searchQueryBuilder.build();
         SearchResult<?> searchResult = searchQuery.fetch(queryDefinition.getFirstResult(), queryDefinition.getMaxResults());
         if (queryStatistics.isEnabled()) recordQuery(System.nanoTime() - start);

         return new QueryResultImpl<>(
               // the hit count cannot exceed the cache size
               new TotalHitCount((int) searchResult.total().hitCountLowerBound(), searchResult.total().isHitCountExact()),
               searchResult.hits().stream().map(origin -> {
                  if (origin instanceof EntityLoaded<?> entityLoaded) {
                     return entityLoaded.entity();
                  }
                  return origin;
               }).collect(Collectors.toList()));
      } catch (org.hibernate.search.util.common.SearchTimeoutException timeoutException) {
         throw new SearchTimeoutException();
      }
   }

   public QueryResult<?> aggregation() {
      long start = queryStatistics.isEnabled() ? System.nanoTime() : 0;
      SearchQueryBuilder searchQueryBuilder = queryDefinition.getSearchQueryBuilder();
      SearchQuery<E> searchQuery = (SearchQuery<E>) searchQueryBuilder.build();
      SearchResult<E> searchResult = searchQuery.fetch(0, 0); // we don't need to fetch any values except the aggregations
      if (queryStatistics.isEnabled()) recordQuery(System.nanoTime() - start);

      Map<Comparable<?>, Long> aggregationResult = searchResult
            .aggregation(AggregationKey.of(INFINISPAN_AGGREGATION_KEY_NAME));
      Sort sort = searchQueryBuilder.getLuceneSort();
      Map<Comparable<?>, Long> aggregation;
      if (sort.getSort().length == 1) {
         aggregation = sort.getSort()[0].getReverse() ? new TreeMap<>(Collections.reverseOrder()) : new TreeMap<>();
         aggregation.putAll(aggregationResult);
      } else {
         aggregation = aggregationResult;
      }

      ArrayList<Object[]> result = new ArrayList<>(aggregation.size());
      boolean displayGroupFirst = searchQueryBuilder.aggregation().displayGroupFirst();
      for (Map.Entry<?, Long> groupAggregation : aggregation.entrySet()) {
         if (displayGroupFirst) {
            result.add(new Object[]{groupAggregation.getKey(), groupAggregation.getValue()});
         } else {
            result.add(new Object[]{groupAggregation.getValue(), groupAggregation.getKey()});
         }
      }
      return new QueryResultImpl<>(
            // the hit count cannot exceed the cache size
            new TotalHitCount((int) searchResult.total().hitCountLowerBound(), searchResult.total().isHitCountExact()),
            result);
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
         LuceneSearchQuery<Object> searchQuery = searchQueryBuilder.ids();
         List<Object> hits = searchQuery.fetchAllHits();

         int count = 0;
         for (Object id : hits) {
            Object removed = cache.remove(id);
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

   private <T> ClosableIteratorWithCount<T> iterator(SearchQuery<T> searchQuery) {
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

   @Override
   public void scoreRequired() {
      queryDefinition.scoreRequired();
   }
}
