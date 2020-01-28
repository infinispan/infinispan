package org.infinispan.query.clustered.commandworkers;

import java.util.BitSet;

import org.hibernate.search.query.engine.spi.DocumentExtractor;
import org.hibernate.search.query.engine.spi.HSQuery;
import org.infinispan.query.clustered.QueryResponse;

/**
 * Get the result size of this query on current node
 *
 * @author Israel Lacerra &lt;israeldl@gmail.com&gt;
 * @since 5.1
 */
final class CQGetResultSize extends CQWorker {

   @Override
   QueryResponse perform(BitSet segments) {
      HSQuery query = queryDefinition.getHsQuery();
      query.afterDeserialise(getSearchFactory());
      setFilter(segments);
      try (DocumentExtractor ignored = query.queryDocumentExtractor()) {
         int resultSize = query.queryResultSize();
         return new QueryResponse(resultSize);
      }
   }
}
