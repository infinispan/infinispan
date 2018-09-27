package org.infinispan.query.clustered.commandworkers;

import java.io.IOException;

import org.apache.lucene.search.TopDocs;
import org.hibernate.search.exception.SearchException;
import org.hibernate.search.query.engine.spi.DocumentExtractor;
import org.hibernate.search.query.engine.spi.HSQuery;
import org.infinispan.query.clustered.NodeTopDocs;
import org.infinispan.query.clustered.QueryResponse;

/**
 * Returns the results of a node to create an eager distributed iterator.
 *
 * @author Israel Lacerra &lt;israeldl@gmail.com&gt;
 * @since 5.1
 */
final class CQCreateEagerQuery extends CQWorker {

   @Override
   QueryResponse perform() {
      HSQuery query = queryDefinition.getHsQuery();
      query.afterDeserialise(getSearchFactory());
      try (DocumentExtractor extractor = query.queryDocumentExtractor()) {
         int resultSize = query.queryResultSize();
         return resultSize == 0 ? new QueryResponse(0) : new QueryResponse(collectKeys(extractor, query));
      }
   }

   private NodeTopDocs collectKeys(DocumentExtractor extractor, HSQuery query) {
      TopDocs topDocs = extractor.getTopDocs();
      int topDocsLength = topDocs.scoreDocs.length;
      Object[] keys = null;
      Object[] projections = null;

      if (query.getProjectedFields() == null) {
         keys = new Object[topDocsLength];
         // collecting keys (it's a eager query!)
         for (int i = 0; i < topDocsLength; i++) {
            keys[i] = extractKey(extractor, i);
         }
      } else {
         projections = new Object[topDocsLength];
         try {
            for (int docIndex = 0; docIndex < topDocsLength; docIndex++) {
               projections[docIndex] = extractor.extract(docIndex).getProjection();
            }
         } catch (IOException e) {
            throw new SearchException("Error while extracting projection", e);
         }
      }

      return new NodeTopDocs(cache.getRpcManager().getAddress(), topDocs, keys, projections);
   }
}
