package org.infinispan.query.clustered.commandworkers;

import static org.infinispan.query.core.impl.Log.CONTAINER;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.query.clustered.QueryResponse;
import org.infinispan.query.core.impl.UpdateQueryHelper;
import org.infinispan.query.impl.SearchQueryBuilder;
import org.infinispan.util.concurrent.WithinThreadExecutor;

import io.reactivex.rxjava3.schedulers.Schedulers;

/**
 * Applies update operations to matching entries on the current node.
 *
 * @since 16.3
 */
final class CQUpdate extends CQWorker {

    @Override
    CompletionStage<QueryResponse> perform(BitSet segments) {
       setFilter(segments);

       // Must never apply any kind of limits to an UPDATE! Limits are just for paging a SELECT.
       if (queryDefinition.getFirstResult() != 0 || queryDefinition.isCustomMaxResults()) {
          throw CONTAINER.statementCannotUsePaging();
       }

       int concurrencyLevel = cache.getCacheConfiguration().locking().concurrencyLevel();

        UpdateQueryHelper.UpdateBiFunction fn = new UpdateQueryHelper.UpdateBiFunction(
              queryDefinition.getQueryString(), queryDefinition.getNamedParameters(), null,
              queryDefinition.getQueryEngineProvider());

       SearchQueryBuilder query = queryDefinition.getSearchQueryBuilder();
       return blockingManager.supplyBlocking(() -> fetchIds(query), this)
             .thenCompose(ids -> CompletionStages.performConcurrently(ids, concurrencyLevel,
                   Schedulers.from(new WithinThreadExecutor()),
                   key -> UpdateQueryHelper.applyUpdateAsync((AdvancedCache<Object, Object>) cache, key, fn),
                   Collectors.summingInt(prev -> prev ? 1 : 0)))
             .thenApply(QueryResponse::new);
    }

   private List<Object> fetchIds(SearchQueryBuilder query) {
      long start = queryStatistics.isEnabled() ? System.nanoTime() : 0;
      List<Object> result = query.ids().fetchAllHits();
      if (queryStatistics.isEnabled()) {
         queryStatistics.localIndexedQueryExecuted(queryDefinition.getQueryString(), System.nanoTime() - start);
      }
      return result;
   }
}
