package org.infinispan.query.clustered.commandworkers;

import java.util.BitSet;

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
      Object key = extractKey(extractor, docIndex);
      Object value = cache.get(key);
      return new QueryResponse(value);
   }
}
