package org.infinispan.query.clustered.commandworkers;

import org.infinispan.query.clustered.QueryBox;
import org.infinispan.query.clustered.QueryResponse;

/**
 * CQLazyFetcher.
 *
 * Fetch a new result for a lazy iterator
 *
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public class CQLazyFetcher extends ClusteredQueryCommandWorker {

   @Override
   public QueryResponse perform() {
      QueryBox box = getQueryBox();
      Object value = box.getValue(lazyQueryId, docIndex);
      return new QueryResponse(value);
   }

}
