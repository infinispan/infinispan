package org.infinispan.query.clustered;

import java.util.Map;
import java.util.UUID;

import org.apache.lucene.search.Sort;
import org.infinispan.AdvancedCache;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Lazily iterates on a distributed query.
 *
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
final class DistributedLazyIterator<E> extends DistributedIterator<E> {

   private static final Log log = LogFactory.getLog(DistributedLazyIterator.class);

   private final UUID queryId;

   private final ClusteredQueryInvoker invoker;

   DistributedLazyIterator(Sort sort, int fetchSize, int resultSize, int maxResults, int firstResult, UUID queryId,
                           Map<Address, NodeTopDocs> topDocsResponses, ClusteredQueryInvoker invoker, AdvancedCache<?, ?> cache) {
      super(sort, fetchSize, resultSize, maxResults, firstResult, topDocsResponses, cache);
      this.queryId = queryId;
      this.invoker = invoker;
   }

   @Override
   public void close() {
      try {
         invoker.broadcast(ClusteredQueryCommand.destroyLazyQuery(cache, queryId));
      } catch (Exception e) {
         log.error("Could not close the distributed iterator", e);
      }
   }

   @Override
   protected E fetchValue(int scoreIndex, NodeTopDocs nodeTopDocs) {
      ClusteredQueryCommand cmd = ClusteredQueryCommand.retrieveKeyFromLazyQuery(cache, queryId, scoreIndex);
      return (E) invoker.unicast(nodeTopDocs.address, cmd).getFetchedValue();
   }
}
