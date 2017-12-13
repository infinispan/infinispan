package org.infinispan.query.clustered.commandworkers;

import org.hibernate.search.query.engine.spi.DocumentExtractor;
import org.hibernate.search.query.engine.spi.HSQuery;
import org.infinispan.query.clustered.QueryResponse;

/**
 * CQGetResultSize.
 *
 * Get the result size of this query on current node
 *
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public class CQGetResultSize extends ClusteredQueryCommandWorker {

   @Override
   public QueryResponse perform() {
      HSQuery query = queryDefinition.getHsQuery();
      query.afterDeserialise(getSearchFactory());
      try (DocumentExtractor ignored = query.queryDocumentExtractor()) {
         int resultSize = query.queryResultSize();
         return new QueryResponse(resultSize);
      }
   }

}
