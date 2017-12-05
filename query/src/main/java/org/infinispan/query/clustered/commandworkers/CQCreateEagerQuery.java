package org.infinispan.query.clustered.commandworkers;

import org.apache.lucene.search.TopDocs;
import org.hibernate.search.query.engine.spi.DocumentExtractor;
import org.hibernate.search.query.engine.spi.HSQuery;
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
      HSQuery query = queryDefinition.getHsQuery();
      query.afterDeserialise(getSearchFactory());
      try (DocumentExtractor extractor = query.queryDocumentExtractor()) {
         int resultSize = query.queryResultSize();
         NodeTopDocs eagerTopDocs = resultSize == 0 ? null : collectKeys(extractor, query);
         QueryResponse queryResponse = new QueryResponse(eagerTopDocs, getQueryBox().getMyId(), resultSize);
         queryResponse.setAddress(cache.getAdvancedCache().getRpcManager().getAddress());
         return queryResponse;
      }
   }

   private NodeTopDocs collectKeys(DocumentExtractor extractor, HSQuery query) {
      TopDocs topDocs = extractor.getTopDocs();

      int topDocsLength = topDocs.scoreDocs.length;
      Object[] keys = null;
      Object[] projections = null;
      KeyTransformationHandler keyTransformationHandler = KeyTransformationHandler
            .getInstance(cache.getAdvancedCache());

      if (query.getProjectedFields() == null) {
         keys = new Object[topDocsLength];
         // collecting keys (it's a eager query!)
         for (int i = 0; i < topDocsLength; i++) {
            keys[i] = QueryExtractorUtil.extractKey(extractor, cache,
                  keyTransformationHandler, i);
         }
      } else {
         projections = new Object[topDocsLength];
         for (int i = 0; i < topDocsLength; i++) {
            projections[i] = QueryExtractorUtil.extractProjection(extractor, i);
         }
      }

      return new NodeTopDocs(topDocs, keys, projections);
   }

}
