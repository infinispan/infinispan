package org.infinispan.query.clustered.commandworkers;

import org.apache.lucene.search.TopDocs;
import org.hibernate.search.query.engine.spi.DocumentExtractor;
import org.infinispan.query.backend.KeyTransformationHandler;
import org.infinispan.query.clustered.NodeTopDocs;
import org.infinispan.query.clustered.QueryResponse;

/**
 * CQCreateEagerQuery.
 *
 * Returns the results of a node to create a eager distributed iterator.
 *
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public class CQCreateEagerQuery extends ClusteredQueryCommandWorker {

   @Override
   public QueryResponse perform() {
      query.afterDeserialise(getSearchFactory());
      DocumentExtractor extractor = query.queryDocumentExtractor();
      try {
         int resultSize = query.queryResultSize();
         NodeTopDocs eagerTopDocs = resultSize == 0 ? null : collectKeys(extractor);
         QueryResponse queryResponse = new QueryResponse(eagerTopDocs, getQueryBox().getMyId(), resultSize);
         queryResponse.setAddress(cache.getAdvancedCache().getRpcManager().getAddress());
         return queryResponse;
      } finally {
         extractor.close();
      }
   }

   private NodeTopDocs collectKeys(DocumentExtractor extractor) {
      TopDocs topDocs = extractor.getTopDocs();

      Object[] keys = new Object[topDocs.scoreDocs.length];
      KeyTransformationHandler keyTransformationHandler = KeyTransformationHandler
            .getInstance(cache.getAdvancedCache());

      // collecting keys (it's a eager query!)
      for (int i = 0; i < topDocs.scoreDocs.length; i++) {
         keys[i] = QueryExtractorUtil.extractKey(extractor, cache,
               keyTransformationHandler, i);
      }

      return new NodeTopDocs(topDocs, keys);
   }

}
