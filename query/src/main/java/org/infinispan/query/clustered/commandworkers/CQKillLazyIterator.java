package org.infinispan.query.clustered.commandworkers;

import org.infinispan.query.clustered.QueryResponse;

/**
 * CQKillLazyIterator.
 *
 * Close a distributed lazy iterator...
 *
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public class CQKillLazyIterator extends ClusteredQueryCommandWorker {

   @Override
   public QueryResponse perform() {
      getQueryBox().kill(lazyQueryId);

      // Not ideal, but more sane
      return new QueryResponse(null);
   }

}
