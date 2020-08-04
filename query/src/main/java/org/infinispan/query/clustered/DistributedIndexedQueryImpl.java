package org.infinispan.query.clustered;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.hibernate.search.util.common.SearchException;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.CloseableIterator;
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

   // like QueryHits.DEFAULT_TOP_DOC_RETRIEVAL_SIZE = 100;
   // (just to have the same default size of not clustered queries)
   private int maxResults = 100;

   private int firstResult = 0;

   public DistributedIndexedQueryImpl(QueryDefinition queryDefinition, AdvancedCache<?, ?> cache) {
      super(queryDefinition, cache);
      this.invoker = new ClusteredQueryInvoker(cache);
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
      return Math.toIntExact(resultSize);
   }

   @Override
   public CloseableIterator<E> iterator() throws SearchException {
      partitionHandlingSupport.checkCacheAvailable();
      queryDefinition.setMaxResults(getNodeMaxResults());

      ClusteredQueryOperation command = ClusteredQueryOperation.createEagerIterator(queryDefinition);
      Map<Address, NodeTopDocs> topDocsResponses = broadcastQuery(command);

      return new DistributedIterator<>(queryDefinition.getSearchQuery().getLuceneSort(),
            maxResults, resultSize, maxResults,
            firstResult, topDocsResponses, cache);

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
      try (CloseableIterator<E> iterator = iterator()) {
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
