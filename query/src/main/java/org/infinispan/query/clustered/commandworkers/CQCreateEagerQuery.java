package org.infinispan.query.clustered.commandworkers;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.hibernate.search.engine.backend.common.DocumentReference;
import org.infinispan.query.clustered.NodeTopDocs;
import org.infinispan.query.clustered.QueryResponse;
import org.infinispan.query.dsl.embedded.impl.SearchQueryBuilder;

/**
 * Returns the results of a node to create an eager distributed iterator.
 *
 * @author Israel Lacerra &lt;israeldl@gmail.com&gt;
 * @since 5.1
 */
final class CQCreateEagerQuery extends CQWorker {

   @Override
   CompletionStage<QueryResponse> perform(BitSet segments) {
      SearchQueryBuilder query = queryDefinition.getSearchQuery();
      setFilter(segments);

      CompletionStage<NodeTopDocs> nodeTopDocs = (query.hasEntityProjection()) ? collectKeys(query) : collectProjections(query);

      return nodeTopDocs.thenApply(QueryResponse::new);
   }

   private CompletionStage<NodeTopDocs> collectKeys(SearchQueryBuilder query) {
      return blockingManager.supplyBlocking(() -> query.documentReference().fetchAll(), "CQCreateEagerQuery#collectKeys")
            .thenApply(queryResult -> {
               if (queryResult.totalHitCount() == 0L) return null;

               Object[] keys = queryResult.hits().stream()
                     .map(DocumentReference::id)
                     .map(this::stringToKey)
                     .toArray(Object[]::new);
               return new NodeTopDocs(cache.getRpcManager().getAddress(), queryResult.topDocs(), keys, null);
            });
   }

   private CompletionStage<NodeTopDocs> collectProjections(SearchQueryBuilder query) {
      return blockingManager.supplyBlocking(() -> query.build().fetchAll(), "CQCreateEagerQuery#collectProjections").thenApply(queryResult -> {
         if (queryResult.totalHitCount() == 0L) return null;

         List<?> hits = queryResult.hits();
         Object[] projections = hits.toArray(new Object[0]);
         return new NodeTopDocs(cache.getRpcManager().getAddress(), queryResult.topDocs(), null, projections);
      });
   }
}
