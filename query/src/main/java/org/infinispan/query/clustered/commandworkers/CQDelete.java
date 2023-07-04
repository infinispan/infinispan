package org.infinispan.query.clustered.commandworkers;

import static org.infinispan.query.logging.Log.CONTAINER;

import java.util.BitSet;
import java.util.concurrent.CompletionStage;

import org.hibernate.search.backend.lucene.search.query.LuceneSearchResult;
import org.infinispan.query.clustered.QueryResponse;
import org.infinispan.query.dsl.embedded.impl.SearchQueryBuilder;

/**
 * Deletes the matching results on current node.
 *
 * @author anistor@redhat.com
 * @since 13.0
 */
final class CQDelete extends CQWorker {

   @Override
   CompletionStage<QueryResponse> perform(BitSet segments) {
      setFilter(segments);

      // Must never apply any kind of limits to a DELETE! Limits are just for paging a SELECT.
      if (queryDefinition.getFirstResult() != 0 || queryDefinition.isCustomMaxResults()) {
         throw CONTAINER.deleteStatementsCannotUsePaging();
      }

      SearchQueryBuilder query = queryDefinition.getSearchQueryBuilder();
      return blockingManager.supplyBlocking(() -> fetchIds(query), this)
            .thenApply(queryResult -> queryResult.hits().stream().map(id -> cache.remove(id) != null ? 1 : 0).reduce(0, Integer::sum))
            .thenApply(QueryResponse::new);
   }

   public LuceneSearchResult<Object> fetchIds(SearchQueryBuilder query) {
      long start = queryStatistics.isEnabled() ? System.nanoTime() : 0;

      LuceneSearchResult<Object> result = query.ids().fetch(queryDefinition.getMaxResults());

      if (queryStatistics.isEnabled()) {
         queryStatistics.localIndexedQueryExecuted(queryDefinition.getQueryString(), System.nanoTime() - start);
      }
      return result;
   }
}
