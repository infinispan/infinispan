package org.infinispan.query.clustered.commandworkers;

import static org.infinispan.query.core.impl.Log.CONTAINER;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.query.clustered.QueryResponse;
import org.infinispan.query.core.impl.UpdateQueryHelper;
import org.infinispan.query.dsl.embedded.impl.SearchQueryBuilder;

/**
 * Applies update operations to matching entries on the current node.
 *
 * @since 16.3
 */
final class CQUpdate extends CQWorker {

   @Override
   CompletionStage<QueryResponse> perform(BitSet segments) {
      setFilter(segments);

      if (queryDefinition.getFirstResult() != 0 || queryDefinition.isCustomMaxResults()) {
         throw CONTAINER.statementCannotUsePaging();
      }

      return blockingManager.supplyBlocking(() -> {
         SearchQueryBuilder query = queryDefinition.getSearchQueryBuilder();
         List<Object> ids = fetchIds(query);

         UpdateQueryHelper.UpdateBiFunction fn = new UpdateQueryHelper.UpdateBiFunction(
               queryDefinition.getQueryString(), queryDefinition.getNamedParameters(), null);

         int count = 0;
         for (Object key : ids) {
            try {
               if (UpdateQueryHelper.applyUpdate((AdvancedCache<Object, Object>) cache, key, fn)) {
                  count++;
               }
            } catch (Exception e) {
               throw CONTAINER.updateByQueryFailed(key, e);
            }
         }
         return new QueryResponse(count);
      }, this);
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
