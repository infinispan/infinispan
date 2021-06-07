package org.infinispan.query.clustered.commandworkers;

import java.util.BitSet;
import java.util.concurrent.CompletionStage;

import org.infinispan.query.clustered.QueryResponse;
import org.infinispan.query.dsl.embedded.impl.SearchQueryBuilder;

/**
 * Get the result size of this query on current node
 *
 * @author Israel Lacerra &lt;israeldl@gmail.com&gt;
 * @since 5.1
 */
final class CQGetResultSize extends CQWorker {

   @Override
   CompletionStage<QueryResponse> perform(BitSet segments) {
      SearchQueryBuilder query = queryDefinition.getSearchQueryBuilder();
      setFilter(segments);
      return blockingManager.supplyBlocking(() -> query.build().fetchTotalHitCount(), this)
            .thenApply(hitCount -> new QueryResponse(Math.toIntExact(hitCount)));
   }
}
