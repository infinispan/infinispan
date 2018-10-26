package org.infinispan.query.clustered;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.search.Query;
import org.hibernate.search.exception.SearchException;
import org.hibernate.search.spi.CustomTypeMetadata;
import org.hibernate.search.spi.IndexedTypeMap;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.AdvancedCache;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.FetchOptions;
import org.infinispan.query.ResultIterator;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.impl.CacheQueryImpl;
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

   public ClusteredCacheQueryImpl(Query luceneQuery, SearchIntegrator searchFactory,
                                  ExecutorService asyncExecutor, AdvancedCache<?, ?> cache, KeyTransformationHandler keyTransformationHandler, Class<?>... classes) {
      super(luceneQuery, searchFactory, cache, keyTransformationHandler, null, classes);
      this.invoker = new ClusteredQueryInvoker(cache, asyncExecutor);
   }

   public ClusteredCacheQueryImpl(QueryDefinition queryDefinition, ExecutorService asyncExecutor, AdvancedCache<?, ?> cache,
                                  KeyTransformationHandler keyTransformationHandler, IndexedTypeMap<CustomTypeMetadata> metadata) {
      super(queryDefinition, cache, keyTransformationHandler);
      if (metadata != null) {
         this.queryDefinition.setIndexedType(metadata.keySet().iterator().next().getPojoType());
         this.queryDefinition.setSortableField(metadata.values().iterator().next().getSortableFields());
      }
      this.invoker = new ClusteredQueryInvoker(cache, asyncExecutor);
   }

   @Override
   public CacheQuery<E> maxResults(int maxResults) {
      this.maxResults = maxResults;
      return super.maxResults(maxResults);
   }

   @Override
   public CacheQuery<E> firstResult(int firstResult) {
      this.firstResult = firstResult;
      return super.firstResult(firstResult);
   }

   @Override
   public int getResultSize() {
      partitionHandlingSupport.checkCacheAvailable();
      if (resultSize == null) {
         List<QueryResponse> responses = invoker.broadcast(ClusteredQueryCommand.getResultSize(queryDefinition, cache));
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
            ClusteredQueryCommand command = ClusteredQueryCommand.createEagerIterator(queryDefinition, cache);
            Map<Address, NodeTopDocs> topDocsResponses = broadcastQuery(command);

            return new DistributedIterator<>(queryDefinition.getSort(),
                  fetchOptions.getFetchSize(), resultSize, maxResults,
                  firstResult, topDocsResponses, cache);
         }
         case LAZY: {
            UUID queryId = UUID.randomUUID();
            ClusteredQueryCommand command = ClusteredQueryCommand.createLazyIterator(queryDefinition, cache, queryId);
            Map<Address, NodeTopDocs> topDocsResponses = broadcastQuery(command);

            // Make a sort copy to avoid reversed results
            return new DistributedLazyIterator<>(queryDefinition.getSort(),
                  fetchOptions.getFetchSize(), resultSize, maxResults,
                  firstResult, queryId, topDocsResponses, invoker, cache);
         }
         default:
            throw new IllegalArgumentException("Unknown FetchMode " + fetchOptions.getFetchMode());
      }
   }

   // number of results of each node of cluster
   private int getNodeMaxResults() {
      return maxResults + firstResult;
   }

   private Map<Address, NodeTopDocs> broadcastQuery(ClusteredQueryCommand command) {
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
   public CacheQuery<E> timeout(long timeout, TimeUnit timeUnit) {
      // TODO [anistor] see https://issues.jboss.org/browse/ISPN-9469
      throw new UnsupportedOperationException("Clustered queries do not support timeouts yet.");
   }
}
