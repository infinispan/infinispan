package org.infinispan.query.clustered;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.hibernate.search.exception.SearchException;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.AdvancedCache;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.FetchOptions;
import org.infinispan.query.QueryDefinition;
import org.infinispan.query.ResultIterator;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.impl.CacheQueryImpl;

/**
 * A extension of CacheQueryImpl used for distributed queries.
 *
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public class ClusteredCacheQueryImpl<E> extends CacheQueryImpl<E> {

   private Integer resultSize;

   private final ExecutorService asyncExecutor;

   // like QueryHits.DEFAULT_TOP_DOC_RETRIEVAL_SIZE = 100;
   // (just to have the same default size of not clustered queries)
   private int maxResults = 100;

   private int firstResult = 0;

   public ClusteredCacheQueryImpl(Query luceneQuery, SearchIntegrator searchFactory,
                                  ExecutorService asyncExecutor, AdvancedCache<?, ?> cache, KeyTransformationHandler keyTransformationHandler, Class<?>... classes) {
      super(luceneQuery, searchFactory, cache, keyTransformationHandler, null, classes);
      this.asyncExecutor = asyncExecutor;
   }

   public ClusteredCacheQueryImpl(String queryString, ExecutorService asyncExecutor, AdvancedCache<?, ?> cache,
                                  KeyTransformationHandler keyTransformationHandler) {
      super(queryString, cache, keyTransformationHandler);
      this.asyncExecutor = asyncExecutor;
      this.queryDefinition = new QueryDefinition(queryString);
   }

   @Override
   public CacheQuery<E> maxResults(int maxResults) {
      this.maxResults = maxResults;
      this.queryDefinition.setMaxResults(maxResults);
      return super.maxResults(maxResults);
   }

   @Override
   public CacheQuery<E> firstResult(int firstResult) {
      this.firstResult = firstResult;
      this.queryDefinition.setFirstResult(firstResult);
      return this;
   }

   @Override
   public CacheQuery<E> sort(Sort sort) {
      this.queryDefinition.setSort(sort);
      return super.sort(sort);
   }

   @Override
   public int getResultSize() {
      int accumulator;
      if (resultSize == null) {
         ClusteredQueryCommand command = ClusteredQueryCommand.getResultSize(queryDefinition, cache);

         ClusteredQueryInvoker invoker = new ClusteredQueryInvoker(cache, asyncExecutor);
         List<QueryResponse> responses = invoker.broadcast(command);

         accumulator = 0;
         for (QueryResponse response : responses) {
            accumulator += response.getResultSize();
         }
         resultSize = accumulator;
      } else {
         accumulator = resultSize;
      }
      return accumulator;
   }

   @Override
   public ResultIterator<E> iterator(FetchOptions fetchOptions) throws SearchException {
      queryDefinition.setMaxResults(getNodeMaxResults());
      switch (fetchOptions.getFetchMode()) {
         case EAGER: {
            ClusteredQueryCommand command = ClusteredQueryCommand.createEagerIterator(queryDefinition, cache);
            HashMap<UUID, ClusteredTopDocs> topDocsResponses = broadcastQuery(command);

            return new DistributedIterator<>(queryDefinition.getSort(),
                  fetchOptions.getFetchSize(), this.resultSize, maxResults,
                  firstResult, topDocsResponses, cache);
         }
         case LAZY: {
            UUID lazyItId = UUID.randomUUID();
            ClusteredQueryCommand command = ClusteredQueryCommand.createLazyIterator(queryDefinition, cache, lazyItId);
            HashMap<UUID, ClusteredTopDocs> topDocsResponses = broadcastQuery(command);

            // Make a sort copy to avoid reversed results
            return new DistributedLazyIterator<>(queryDefinition.getSort(),
                  fetchOptions.getFetchSize(), this.resultSize, maxResults,
                  firstResult, lazyItId, topDocsResponses, asyncExecutor, cache);
         }
         default:
            throw new IllegalArgumentException("Unknown FetchMode " + fetchOptions.getFetchMode());
      }
   }

   // number of results of each node of cluster
   private int getNodeMaxResults() {
      return maxResults + firstResult;
   }

   private HashMap<UUID, ClusteredTopDocs> broadcastQuery(ClusteredQueryCommand command) {
      ClusteredQueryInvoker invoker = new ClusteredQueryInvoker(cache, asyncExecutor);

      HashMap<UUID, ClusteredTopDocs> topDocsResponses = new HashMap<>();
      int resultSize = 0;
      List<QueryResponse> responses = invoker.broadcast(command);

      for (QueryResponse queryResponse : responses) {
         if (queryResponse.nonEmpty()) {
            topDocsResponses.put(queryResponse.getNodeUUID(), queryResponse.toClusteredTopDocs());
            resultSize += queryResponse.getResultSize();
         }
      }

      this.resultSize = resultSize;
      return topDocsResponses;
   }

   @Override
   public List<E> list() throws SearchException {
      List<E> values = new ArrayList<>();
      try (ResultIterator<E> iterator = iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.EAGER))) {
         while (iterator.hasNext()) {
            values.add(iterator.next());
         }
      }
      return values;
   }

   @Override
   public CacheQuery<E> timeout(long timeout, TimeUnit timeUnit) {
      throw new UnsupportedOperationException("Clustered queries do not support timeouts yet.");   // TODO
   }
}
