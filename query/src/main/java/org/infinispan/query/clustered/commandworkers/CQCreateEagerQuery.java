package org.infinispan.query.clustered.commandworkers;

import java.util.BitSet;
import java.util.List;

import org.hibernate.search.backend.lucene.search.query.LuceneSearchResult;
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
   QueryResponse perform(BitSet segments) {
      SearchQueryBuilder query = queryDefinition.getSearchQuery();
      setFilter(segments);

      NodeTopDocs nodeTopDocs = (query.hasEntityProjection()) ? collectKeys(query) : collectProjections(query);
      if (nodeTopDocs == null) {
         return new QueryResponse(0);
      }
      return new QueryResponse(nodeTopDocs);
   }

   private NodeTopDocs collectKeys(SearchQueryBuilder query) {
      LuceneSearchResult<DocumentReference> queryResult = query.documentReference().fetchAll();
      if (queryResult.getTotalHitCount() == 0L) {
         return null;
      }

      Object[] keys = queryResult.getHits().stream()
            .map(hit -> hit.getId())
            .map(id -> stringToKey(id))
            .toArray(Object[]::new);
      return new NodeTopDocs(cache.getRpcManager().getAddress(), queryResult.getTopDocs(), keys, null);
   }

   private NodeTopDocs collectProjections(SearchQueryBuilder query) {
      LuceneSearchResult<?> queryResult = query.build().fetchAll();
      if (queryResult.getTotalHitCount() == 0L) {
         return null;
      }

      List<?> hits = queryResult.getHits();
      Object[] projections = hits.toArray(new Object[hits.size()]);
      return new NodeTopDocs(cache.getRpcManager().getAddress(), queryResult.getTopDocs(), null, projections);
   }
}
