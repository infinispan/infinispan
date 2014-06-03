package org.infinispan.query.clustered;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.hibernate.search.exception.SearchException;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.AdvancedCache;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.FetchOptions;
import org.infinispan.query.ResultIterator;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.impl.CacheQueryImpl;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.commons.util.Util;

/**
 * A extension of CacheQueryImpl used for distributed queries.
 *
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public class ClusteredCacheQueryImpl extends CacheQueryImpl {

   private Sort sort;

   private Integer resultSize;

   private final ExecutorService asyncExecutor;

   // like QueryHits.DEFAULT_TOP_DOC_RETRIEVAL_SIZE = 100;
   // (just to have the same default size of not clustered queries)
   private int maxResults = 100;

   private int firstResult = 0;

   private Marshaller marshaller;

   public ClusteredCacheQueryImpl(Query luceneQuery, SearchFactoryIntegrator searchFactory,
            ExecutorService asyncExecutor, AdvancedCache<?, ?> cache, KeyTransformationHandler keyTransformationHandler, Class<?>... classes) {
      super(luceneQuery, searchFactory, cache, keyTransformationHandler, classes);
      this.asyncExecutor = asyncExecutor;
      this.hSearchQuery = searchFactory.createHSQuery().luceneQuery(luceneQuery)
               .targetedEntities(Arrays.asList(classes));
      this.marshaller = ComponentRegistryUtils.getComponent(cache,
            StreamingMarshaller.class, KnownComponentNames.CACHE_MARSHALLER);
   }

   @Override
   public CacheQuery maxResults(int maxResults) {
      this.maxResults = maxResults;
      return super.maxResults(maxResults);
   }

   @Override
   public CacheQuery firstResult(int firstResult) {
      this.firstResult = firstResult;
      return this;
   }

   @Override
   public CacheQuery sort(Sort sort) {
      this.sort = sort;
      return super.sort(sort);
   }

   @Override
   public int getResultSize() {
      int accumulator;
      if (resultSize == null) {
         ClusteredQueryCommand command = ClusteredQueryCommand.getResultSize(hSearchQuery, cache);

         ClusteredQueryInvoker invoker = new ClusteredQueryInvoker(cache, asyncExecutor);
         List<QueryResponse> responses = invoker.broadcast(command);

         accumulator = 0;
         for (QueryResponse response : responses) {
            accumulator += response.getResultSize();
         }
         resultSize = Integer.valueOf(accumulator);
      } else {
         accumulator = resultSize.intValue();
      }
      return accumulator;
   }

   @Override
   public ResultIterator iterator(FetchOptions fetchOptions) throws SearchException {
      hSearchQuery.maxResults(getNodeMaxResults());
      switch (fetchOptions.getFetchMode()) {
         case EAGER: {
            ClusteredQueryCommand command = ClusteredQueryCommand.createEagerIterator(hSearchQuery, cache);
            HashMap<UUID, ClusteredTopDocs> topDocsResponses = broadcastQuery(command);

            return new DistributedIterator(sort,
                  fetchOptions.getFetchSize(), this.resultSize, maxResults,
                  firstResult, topDocsResponses, cache);
         }
         case LAZY: {
            UUID lazyItId = UUID.randomUUID();
            ClusteredQueryCommand command = ClusteredQueryCommand.createLazyIterator(hSearchQuery, cache, lazyItId);
            HashMap<UUID, ClusteredTopDocs> topDocsResponses = broadcastQuery(command);

            // Make a sort copy to avoid reversed results
            return new DistributedLazyIterator(sort,
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

      HashMap<UUID, ClusteredTopDocs> topDocsResponses = new HashMap<UUID, ClusteredTopDocs>();
      int resultSize = 0;
      List<QueryResponse> responses = invoker.broadcast(command);

      for (QueryResponse queryResponse : responses) {
         ClusteredTopDocs topDocs = new ClusteredTopDocs(queryResponse.getTopDocs(), queryResponse.getNodeUUID());

         resultSize += queryResponse.getResultSize();
         topDocs.setNodeAddress(queryResponse.getAddress());
         topDocsResponses.put(queryResponse.getNodeUUID(), topDocs);
      }

      this.resultSize = resultSize;
      return topDocsResponses;
   }

   @Override
   public List<Object> list() throws SearchException {
      ResultIterator iterator = iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.EAGER));
      List<Object> values = new ArrayList<Object>();
      while (iterator.hasNext()) {
         values.add(iterator.next());
      }

      return values;
   }

   @Override
   public CacheQuery timeout(long timeout, TimeUnit timeUnit) {
      throw new UnsupportedOperationException("Clustered queries do not support timeouts yet.");   // TODO
   }
}
