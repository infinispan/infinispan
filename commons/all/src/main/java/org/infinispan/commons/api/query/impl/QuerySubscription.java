package org.infinispan.commons.api.query.impl;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.commons.reactive.AbstractAsyncPublisherHandler;

public class QuerySubscription<E> extends AbstractAsyncPublisherHandler<Query<E>, E, QueryResult<E>, QueryResult<E>> {
   private final long initialOffset;
   private final int maxResults;

   private long currentOffset;

   protected QuerySubscription(int maxBatchSize, Query<E> query) {
      super(maxBatchSize, () -> null, query);
      this.initialOffset = query.getStartOffset();
      this.currentOffset = query.getStartOffset();
      this.maxResults = query.getMaxResults();
   }

   @Override
   protected void sendCancel(Query<E> es) {
      // Nothing to do
   }

   @Override
   protected CompletionStage<QueryResult<E>> sendInitialCommand(Query<E> es, int batchSize) {
      return es.maxResults(maxResults > 0 ? Math.min(batchSize, maxResults) : batchSize).executeAsync();
   }

   @Override
   protected CompletionStage<QueryResult<E>> sendNextCommand(Query<E> es, int batchSize) {
      if (maxResults > 0) {
         int remaining = maxResults - (int) (currentOffset - initialOffset);
         if (remaining < batchSize) {
            es = es.maxResults(remaining);
         }
      }
      return es.startOffset(currentOffset).executeAsync();
   }

   @Override
   protected long handleInitialResponse(QueryResult<E> eQueryResult, Query<E> es) {
      List<E> batchResult = eQueryResult.list();
      int resultSize = batchResult.size();
      if ((currentOffset += resultSize) - initialOffset == maxResults || resultSize < batchSize) {
         // Reset the values back before it was subscribed
         es.startOffset(initialOffset);
         es.maxResults(maxResults);
         targetComplete();
      }
      for (E result : batchResult) {
         if (!onNext(result)) {
            break;
         }
      }
      return resultSize;
   }

   @Override
   protected long handleNextResponse(QueryResult<E> eQueryResult, Query<E> es) {
      return handleInitialResponse(eQueryResult, es);
   }
}
