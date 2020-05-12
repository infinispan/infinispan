package org.infinispan.query.clustered.commandworkers;

import java.util.BitSet;

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
   QueryResponse perform(BitSet segments) {
      SearchQueryBuilder query = queryDefinition.getSearchQuery();
      setFilter(segments);
      long hitCount = query.build().fetchTotalHitCount();
      // or maybe is it better to keep long?
      return new QueryResponse(Math.toIntExact(hitCount));
   }
}
