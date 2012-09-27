/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
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
import org.hibernate.search.SearchException;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.AdvancedCache;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.FetchOptions;
import org.infinispan.query.QueryIterator;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.impl.CacheQueryImpl;

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

   public ClusteredCacheQueryImpl(Query luceneQuery, SearchFactoryIntegrator searchFactory,
            ExecutorService asyncExecutor, AdvancedCache<?, ?> cache, KeyTransformationHandler keyTransformationHandler, Class<?>... classes) {
      super(luceneQuery, searchFactory, cache, keyTransformationHandler, classes);
      this.asyncExecutor = asyncExecutor;
      hSearchQuery = searchFactory.createHSQuery().luceneQuery(luceneQuery)
               .targetedEntities(Arrays.asList(classes));
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
      }
      else {
         accumulator = resultSize.intValue();
      }
      return accumulator;
   }

   @Override
   public QueryIterator iterator(FetchOptions fetchOptions) throws SearchException {
      hSearchQuery.maxResults(getNodeMaxResults());
      if (fetchOptions.getFetchMode().equals(FetchOptions.FetchMode.EAGER)) {
         ClusteredQueryCommand command = ClusteredQueryCommand.createEagerIterator(hSearchQuery, cache);
         HashMap<UUID, ClusteredTopDocs> topDocsResponses = broadcastQuery(command);

         return new DistributedIterator(sort, fetchOptions.getFetchSize(), this.resultSize, maxResults, firstResult,
               topDocsResponses, cache);

      } else if (fetchOptions.getFetchMode().equals(FetchOptions.FetchMode.LAZY)) {
         UUID lazyItId = UUID.randomUUID();
         ClusteredQueryCommand command = ClusteredQueryCommand.createLazyIterator(hSearchQuery, cache, lazyItId);
         HashMap<UUID, ClusteredTopDocs> topDocsResponses = broadcastQuery(command);

         return new DistributedLazyIterator(sort, fetchOptions.getFetchSize(), this.resultSize, maxResults, firstResult,
               lazyItId, topDocsResponses, asyncExecutor, cache);
      } else {
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

      for (Object response : responses) {
         QueryResponse queryResponse = (QueryResponse) response;
         ClusteredTopDocs topDocs = new ClusteredTopDocs(queryResponse.getTopDocs(),
                  queryResponse.getNodeUUID());

         resultSize += queryResponse.getResultSize();
         topDocs.setNodeAddress(queryResponse.getAddress());
         topDocsResponses.put(queryResponse.getNodeUUID(), topDocs);
      }

      this.resultSize = resultSize;
      return topDocsResponses;
   }

   @Override
   public List<Object> list() throws SearchException {
      QueryIterator iterator = iterator(new FetchOptions(FetchOptions.FetchMode.EAGER));
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
