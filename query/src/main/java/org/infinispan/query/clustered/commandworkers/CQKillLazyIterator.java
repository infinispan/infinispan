package org.infinispan.query.clustered.commandworkers;

import org.infinispan.query.clustered.QueryResponse;

/**
 * Close a distributed lazy iterator...
 *
 * @author Israel Lacerra &lt;israeldl@gmail.com&gt;
 * @since 5.1
 */
final class CQKillLazyIterator extends CQWorker {

   @Override
   QueryResponse perform() {
      getQueryBox().kill(queryId);

      // Not ideal, but more sane
      return new QueryResponse();
   }
}
