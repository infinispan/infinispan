package org.infinispan.query.clustered.commandworkers;

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
   QueryResponse perform() {
      HSQuery query = queryDefinition.getHsQuery();
      query.afterDeserialise(getSearchFactory());
      try (DocumentExtractor ignored = query.queryDocumentExtractor()) {
         int resultSize = query.queryResultSize();
         return new QueryResponse(resultSize);
      }
   }
}
