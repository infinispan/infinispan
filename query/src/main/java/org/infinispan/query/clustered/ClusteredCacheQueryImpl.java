package org.infinispan.query.clustered;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.hibernate.search.util.common.SearchException;
import org.infinispan.AdvancedCache;
import org.infinispan.query.FetchOptions;
import org.infinispan.query.ResultIterator;
import org.infinispan.query.impl.CacheQueryImpl;
import org.infinispan.query.impl.IndexedQuery;
import org.infinispan.query.impl.QueryDefinition;
import org.infinispan.remoting.transport.Address;

/**
 * An extension of CacheQueryImpl used for distributed queries.
 *
 * @author Israel Lacerra &lt;israeldl@gmail.com&gt;
 * @since 5.1
 */
public final class ClusteredCacheQueryImpl<E> extends CacheQueryImpl<E> {

   private Integer resultSize;

   private final ClusteredQueryInvoker invoker;

   // like QueryHits.DEFAULT_TOP_DOC_RETRIEVAL_SIZE = 100;
   // (just to have the same default size of not clustered queries)
   private int maxResults = 100;

   private int firstResult = 0;

   public ClusteredCacheQueryImpl(QueryDefinition queryDefinition, ExecutorService asyncExecutor, AdvancedCache<?, ?> cache) {
      super(queryDefinition, cache);
      this.invoker = new ClusteredQueryInvoker(cache, asyncExecutor);
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
   public long getResultSize() {
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
   public ResultIterator<E> iterator(FetchOptions fetchOptions) throws SearchException {
      partitionHandlingSupport.checkCacheAvailable();
      queryDefinition.setMaxResults(getNodeMaxResults());
      switch (fetchOptions.getFetchMode()) {
         case EAGER: {
            ClusteredQueryOperation command = ClusteredQueryOperation.createEagerIterator(queryDefinition);
            Map<Address, NodeTopDocs> topDocsResponses = broadcastQuery(command);

            return new DistributedIterator<>(queryDefinition.getSearchQuery().getLuceneSort(),
                  fetchOptions.getFetchSize(), resultSize, maxResults,
                  firstResult, topDocsResponses, cache);
         }
         case LAZY: {
//            UUID queryId = UUID.randomUUID();
//            ClusteredQueryOperation command = ClusteredQueryOperation.createLazyIterator(queryDefinition, queryId);
//            Map<Address, NodeTopDocs> topDocsResponses = broadcastQuery(command);

            // TODO HSEARCH-3323 Restore support for scrolling
//            // Make a sort copy to avoid reversed results
//            return new DistributedLazyIterator<>(queryDefinition.getSort(),
//                  fetchOptions.getFetchSize(), resultSize, maxResults,
//                  firstResult, queryId, topDocsResponses, invoker, cache);
         }
         default:
            throw new IllegalArgumentException("Unknown FetchMode " + fetchOptions.getFetchMode());
      }
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
         if (queryResponse.getResultSize() > 0) {
            topDocsResponses.put(queryResponse.getNodeTopDocs().address, queryResponse.getNodeTopDocs());
            resultSize += queryResponse.getResultSize();
         }
      }

      this.resultSize = resultSize;
      return topDocsResponses;
   }

   @Override
   public List<E> list() throws SearchException {
      partitionHandlingSupport.checkCacheAvailable();
      List<E> values = new ArrayList<>();
      try (ResultIterator<E> iterator = iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.EAGER))) {
         while (iterator.hasNext()) {
            values.add(iterator.next());
         }
      }
      return values;
   }

   @Override
   public IndexedQuery<E> timeout(long timeout, TimeUnit timeUnit) {
      queryDefinition.setTimeout(timeout, timeUnit);
      return this;
   }
}
