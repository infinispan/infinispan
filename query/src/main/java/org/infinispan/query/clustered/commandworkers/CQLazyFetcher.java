package org.infinispan.query.clustered.commandworkers;

import java.io.IOException;
import java.util.BitSet;

import org.hibernate.search.exception.SearchException;
import org.hibernate.search.query.engine.spi.DocumentExtractor;
import org.infinispan.query.clustered.QueryResponse;

/**
 * Fetch a new result for a lazy iterator.
 *
 * @author Israel Lacerra &lt;israeldl@gmail.com&gt;
 * @since 5.1
 */
final class CQLazyFetcher extends CQWorker {

   @Override
   QueryResponse perform(BitSet segments) {
      DocumentExtractor extractor = getQueryBox().get(queryId);
      Object value = extractValue(extractor, docIndex);
      return new QueryResponse(value);
   }

   Object extractValue(DocumentExtractor extractor, int docIndex) {
      Object[] projection;
      try {
         projection = extractor.extract(docIndex).getProjection();
      } catch (IOException e) {
         throw new SearchException("Error while extracting projection", e);
      }
      if (projection == null) {
         Object key = extractKey(extractor, docIndex);
         return cache.get(key);
      }
      return projection;
   }
}
