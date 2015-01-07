package org.infinispan.query.clustered.commandworkers;

import org.apache.lucene.search.TopDocs;
import org.hibernate.search.query.engine.spi.DocumentExtractor;
import org.infinispan.query.clustered.QueryBox;
import org.infinispan.query.clustered.QueryResponse;

/**
 * CQCreateLazyQuery.
 * 
 * Creates a DocumentExtractor and register it on the node QueryBox.
 *  
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public class CQCreateLazyQuery extends ClusteredQueryCommandWorker {

   @Override
   public QueryResponse perform() {
      query.afterDeserialise(getSearchFactory());
      DocumentExtractor extractor = query.queryDocumentExtractor();
      int resultSize = query.queryResultSize();

      QueryBox box = getQueryBox();
      
      // registering...
      box.put(lazyQueryId, extractor);
      
      // returning the QueryResponse 
      TopDocs topDocs = extractor.getTopDocs();
      QueryResponse queryResponse = new QueryResponse(topDocs, box.getMyId(), resultSize);
      queryResponse.setAddress(cache.getAdvancedCache().getRpcManager().getAddress());
      return queryResponse;
   }

}
