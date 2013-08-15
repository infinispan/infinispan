package org.infinispan.query.clustered.commandworkers;

import org.hibernate.search.engine.spi.SearchFactoryImplementor;
import org.hibernate.search.query.engine.spi.DocumentExtractor;
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
      query.afterDeserialise((SearchFactoryImplementor) getSearchFactory());
      DocumentExtractor extractor = query.queryDocumentExtractor();
      try {
         int resultSize = query.queryResultSize();
         QueryResponse queryResponse = new QueryResponse(resultSize);
         return queryResponse;
      } finally {
         extractor.close();
      }
   }

}
