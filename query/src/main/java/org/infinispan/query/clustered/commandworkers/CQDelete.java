package org.infinispan.query.clustered.commandworkers;

import java.util.BitSet;
import java.util.concurrent.CompletionStage;

import org.hibernate.search.backend.lucene.search.query.LuceneSearchResult;
import org.infinispan.query.clustered.QueryResponse;
import org.infinispan.query.dsl.embedded.impl.SearchQueryBuilder;
import org.infinispan.search.mapper.common.EntityReference;

/**
 * Deletes the matching results.
 *
 * @author anistor@redhat.com
 * @since 13.0
 */
final class CQDelete extends CQWorker {

   @Override
   CompletionStage<QueryResponse> perform(BitSet segments) {
      SearchQueryBuilder query = queryDefinition.getSearchQueryBuilder();
      setFilter(segments);
      return blockingManager.supplyBlocking(() -> fetchReferences(query), this)
            .thenApply(queryResult -> queryResult.hits().stream().map(ref -> cache.remove(ref.key()) != null ? 1 : 0).reduce(0, Integer::sum))
            .thenApply(QueryResponse::new);
   }

   private LuceneSearchResult<EntityReference> fetchReferences(SearchQueryBuilder query) {
      long start = queryStatistics.isEnabled() ? System.nanoTime() : 0;

      LuceneSearchResult<EntityReference> result = query.entityReference().fetch(queryDefinition.getMaxResults());

      if (queryStatistics.isEnabled())
         queryStatistics.localIndexedQueryExecuted(queryDefinition.getQueryString(), System.nanoTime() - start);

      return result;
   }
}
