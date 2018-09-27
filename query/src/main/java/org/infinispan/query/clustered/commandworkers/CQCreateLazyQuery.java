package org.infinispan.query.clustered.commandworkers;

import org.apache.lucene.search.TopDocs;
import org.hibernate.search.query.engine.spi.DocumentExtractor;
import org.hibernate.search.query.engine.spi.HSQuery;
import org.infinispan.query.clustered.NodeTopDocs;
import org.infinispan.query.clustered.QueryResponse;

/**
 * Creates a DocumentExtractor and register it on the node QueryBox.
 *
 * @author Israel Lacerra &lt;israeldl@gmail.com&gt;
 * @since 5.1
 */
final class CQCreateLazyQuery extends CQWorker {

   @Override
   QueryResponse perform() {
      HSQuery query = queryDefinition.getHsQuery();
      query.afterDeserialise(getSearchFactory());
      DocumentExtractor extractor = query.queryDocumentExtractor();

      // registering...
      getQueryBox().put(queryId, extractor);

      // returning the QueryResponse
      TopDocs topDocs = extractor.getTopDocs();
      return new QueryResponse(new NodeTopDocs(cache.getRpcManager().getAddress(), topDocs));
   }
}
