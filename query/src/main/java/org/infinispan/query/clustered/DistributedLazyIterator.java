package org.infinispan.query.clustered;

import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import org.apache.lucene.search.Sort;
import org.infinispan.AdvancedCache;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * DistributedLazyIterator.
 * 
 * Lazily iterates on a distributed query
 * 
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public class DistributedLazyIterator extends DistributedIterator {

   private final UUID queryId;
   private final ExecutorService asyncExecutor;
   private final ClusteredQueryInvoker invoker;

   private static final Log log = LogFactory.getLog(DistributedLazyIterator.class);

   public DistributedLazyIterator(Sort sort, int fetchSize, int resultSize, int maxResults, int firstResult, UUID id,
         HashMap<UUID, ClusteredTopDocs> topDocsResponses, ExecutorService asyncExecutor, AdvancedCache<?, ?> cache) {
      super(sort, fetchSize, resultSize, maxResults, firstResult, topDocsResponses, cache);
      this.queryId = id;
      this.asyncExecutor = asyncExecutor;
      this.invoker = new ClusteredQueryInvoker(cache, asyncExecutor);
   }

   @Override
   public void close() {
      ClusteredQueryCommand killQuery = ClusteredQueryCommand.destroyLazyQuery(cache, queryId);
      ClusteredQueryInvoker invoker = new ClusteredQueryInvoker(cache, asyncExecutor);
      try {
         invoker.broadcast(killQuery);
      } catch (Exception e) {
         log.error("Could not close the distributed iterator", e);
      }
   }

   @Override
   public Object fetchValue(int scoreIndex, ClusteredTopDocs topDoc) {
      Object value = null;
      try {
         value = invoker.getValue(scoreIndex, topDoc.getNodeAddress(), queryId);
      } catch (Exception e) {
         log.error("Error while trying to remoting fetch next value: " + e.getMessage());
      }
      return value;
   }

}
