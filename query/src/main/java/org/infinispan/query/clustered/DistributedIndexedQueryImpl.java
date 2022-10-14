package org.infinispan.query.clustered;

import static java.util.Spliterators.spliteratorUnknownSize;
import static java.util.stream.StreamSupport.stream;

import static org.infinispan.query.logging.Log.CONTAINER;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.hibernate.search.util.common.SearchException;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.query.SearchTimeoutException;
import org.infinispan.query.core.impl.QueryResultImpl;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.query.dsl.QueryResult;
import org.infinispan.query.impl.IndexedQuery;
import org.infinispan.query.impl.IndexedQueryImpl;
import org.infinispan.query.impl.QueryDefinition;
import org.infinispan.remoting.transport.Address;

/**
 * An extension of IndexedQueryImpl used for distributed queries.
 *
 * @author Israel Lacerra &lt;israeldl@gmail.com&gt;
 * @since 5.1
 */
public final class DistributedIndexedQueryImpl<E> extends IndexedQueryImpl<E> {

   private Integer resultSize;

   private final ClusteredQueryInvoker invoker;

   private int maxResults;

   private int firstResult = 0;

   public DistributedIndexedQueryImpl(QueryDefinition queryDefinition, AdvancedCache<?, ?> cache,
                                      LocalQueryStatistics queryStatistics, int defaultMaxResults) {
      super(queryDefinition, cache, queryStatistics);
      this.invoker = new ClusteredQueryInvoker(cache, queryStatistics);
      this.maxResults = defaultMaxResults;
   }

   @Override
   public IndexedQuery<E> maxResults(int maxResults) {
      this.maxResults = maxResults;
      return super.maxResults(maxResults);
   }

   @Override
   public IndexedQuery<E> firstResult(int firstResult) {
      this.firstResult = firstResult;
      return super.firstResult(firstResult);
   }

   @Override
   public int getResultSize() {
      partitionHandlingSupport.checkCacheAvailable();
      if (resultSize == null) {
         List<QueryResponse> responses = invoker.broadcast(ClusteredQueryOperation.getResultSize(queryDefinition));
         int accumulator = 0;
         for (QueryResponse response : responses) {
            accumulator += response.getResultSize();
         }
         resultSize = accumulator;
      }
      return resultSize;
   }

   @Override
   public CloseableIterator<E> iterator() throws SearchException {
      partitionHandlingSupport.checkCacheAvailable();
      queryDefinition.setMaxResults(getNodeMaxResults());

      ClusteredQueryOperation command = ClusteredQueryOperation.createEagerIterator(queryDefinition);
      Map<Address, NodeTopDocs> topDocsResponses = broadcastQuery(command);

      return new DistributedIterator<>(queryStatistics, queryDefinition.getSearchQueryBuilder().getLuceneSort(),
            maxResults, resultSize, maxResults,
            firstResult, topDocsResponses, cache);
   }

   @Override
   public <K> CloseableIterator<Map.Entry<K, E>> entryIterator() {
      partitionHandlingSupport.checkCacheAvailable();
      queryDefinition.setMaxResults(getNodeMaxResults());

      ClusteredQueryOperation command = ClusteredQueryOperation.createEagerIterator(queryDefinition);
      Map<Address, NodeTopDocs> topDocsResponses = broadcastQuery(command);

      // sanity check: if query has projections other than the entity itself throw an exception
      if (!queryDefinition.getSearchQueryBuilder().isEntityProjection()) {
         throw CONTAINER.entryIteratorDoesNotAllowProjections();
      }

      return new DistributedEntryIterator<>(queryStatistics, queryDefinition.getSearchQueryBuilder().getLuceneSort(),
            maxResults, resultSize, maxResults, firstResult, topDocsResponses, cache);
   }

   // number of results of each node of cluster
   private int getNodeMaxResults() {
      return maxResults + firstResult;
   }

   private Map<Address, NodeTopDocs> broadcastQuery(ClusteredQueryOperation command) {
      Map<Address, NodeTopDocs> topDocsResponses = new HashMap<>();
      int resultSize = 0;
      List<QueryResponse> responses = invoker.broadcast(command);

      for (QueryResponse queryResponse : responses) {
         if (queryResponse.getNodeTopDocs().topDocs != null) {
            topDocsResponses.put(queryResponse.getNodeTopDocs().address, queryResponse.getNodeTopDocs());
         }
         resultSize += queryResponse.getResultSize();
      }

      this.resultSize = resultSize;
      return topDocsResponses;
   }

   @Override
   public QueryResult<?> execute() {
      if (queryDefinition.getStatementType() != IckleParsingResult.StatementType.SELECT) {
         return new QueryResultImpl<E>(executeStatement(), Collections.emptyList());
      }

      try {
         partitionHandlingSupport.checkCacheAvailable();
         List<E> hits = stream(spliteratorUnknownSize(iterator(), 0), false).collect(Collectors.toList());
         return new QueryResultImpl<>(resultSize, hits);
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

         List<QueryResponse> responses = invoker.broadcast(ClusteredQueryOperation.delete(queryDefinition));
         int count = 0;
         for (QueryResponse response : responses) {
            count += response.getResultSize();
         }

         if (queryStatistics.isEnabled()) recordQuery(System.nanoTime() - start);

         return count;
      } catch (org.hibernate.search.util.common.SearchTimeoutException timeoutException) {
         throw new SearchTimeoutException();
      }
   }

   private void recordQuery(long nanos) {
      queryStatistics.distributedIndexedQueryExecuted(queryDefinition.getQueryString(), nanos);
   }

   @Override
   public IndexedQuery<E> timeout(long timeout, TimeUnit timeUnit) {
      queryDefinition.setTimeout(timeout, timeUnit);
      return this;
   }
}
