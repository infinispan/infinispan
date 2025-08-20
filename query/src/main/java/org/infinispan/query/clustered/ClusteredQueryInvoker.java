package org.infinispan.query.clustered;

import static org.infinispan.commons.util.concurrent.CompletableFutures.await;
import static org.infinispan.commons.util.concurrent.CompletableFutures.sequence;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.lucene.search.TimeLimitingCollector.TimeExceededException;
import org.hibernate.search.util.common.SearchException;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.TimeoutException;
import org.infinispan.commons.util.Util;
import org.infinispan.query.core.impl.Log;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.SingleResponseCollector;
import org.infinispan.util.logging.LogFactory;

/**
 * Invoke a ClusteredQueryCommand on the cluster, including on own node.
 *
 * @author Israel Lacerra &lt;israeldl@gmail.com&gt;
 * @author Sanne Grinovero &lt;sanne@infinispan.org&gt; (C) 2011 Red Hat Inc.
 * @since 5.1
 */
final class ClusteredQueryInvoker {

   private static final Log log = LogFactory.getLog(ClusteredQueryInvoker.class, Log.class);

   private final RpcManager rpcManager;
   private final LocalQueryStatistics queryStatistics;
   private final AdvancedCache<?, ?> cache;
   private final Address myAddress;
   private final QueryPartitioner partitioner;

   ClusteredQueryInvoker(AdvancedCache<?, ?> cache, LocalQueryStatistics queryStatistics) {
      this.cache = cache;
      this.rpcManager = cache.getRpcManager();
      this.queryStatistics = queryStatistics;
      this.myAddress = rpcManager.getAddress();
      this.partitioner = new QueryPartitioner(cache);
   }

   /**
    * Broadcast this ClusteredQueryOperation to all cluster nodes. Each node will query a specific number of segments,
    * with the local mode having preference to process as much segments as possible. The remainder segments will be
    * processed by the respective primary owners.
    *
    * @param operation The {@link ClusteredQueryOperation} to perform cluster wide.
    * @return the responses
    */
   List<QueryResponse> broadcast(ClusteredQueryOperation operation) {
      long start = queryStatistics.isEnabled() ? System.nanoTime() : 0;

      String queryString = null;
      if (log.isTraceEnabled() || queryStatistics.isEnabled()) {
         queryString = operation.getQueryDefinition().getQueryString();
      }
      if (log.isTraceEnabled()) {
         log.tracef("Broadcast query started: '%s'.", queryString);
      }

      Map<Address, BitSet> split = partitioner.split();
      SegmentsClusteredQueryCommand localCommand = new SegmentsClusteredQueryCommand(cache.getName(), operation, split.get(myAddress));
      // sends the request remotely first
      List<CompletableFuture<QueryResponse>> futureRemoteResponses = split.entrySet().stream()
            .filter(e -> !e.getKey().equals(myAddress)).map(e -> {
               Address address = e.getKey();
               BitSet segments = e.getValue();
               SegmentsClusteredQueryCommand cmd = new SegmentsClusteredQueryCommand(cache.getName(), operation, segments);
               return rpcManager.invokeCommand(address, cmd, SingleResponseCollector.validOnly(),
                                               rpcManager.getSyncRpcOptions()).toCompletableFuture();
            }).map(a -> a.thenApply(r -> (QueryResponse) r.getResponseValue())).collect(Collectors.toList());

      // then, invoke on own node
      CompletionStage<QueryResponse> localResponse = localInvoke(localCommand);

      List<QueryResponse> results = new ArrayList<>();
      try {
         results.add(await(localResponse.toCompletableFuture()));
         results.addAll(await(sequence(futureRemoteResponses)));

         if (queryStatistics.isEnabled()) {
            queryStatistics.distributedIndexedQueryExecuted(queryString, System.nanoTime() - start);
         }
         if (log.isTraceEnabled()) {
            log.tracef("Broadcast query completed: '%s'.", queryString);
         }
      } catch (InterruptedException e) {
         throw new SearchException("Interrupted while searching locally", e);
      } catch (ExecutionException e) {
         Throwable rootCause = Util.getRootCause(e);
         if (rootCause instanceof org.hibernate.search.util.common.SearchTimeoutException ||
               rootCause instanceof TimeExceededException) {
            throw new TimeoutException("Query exceeded timeout");
         }
         throw new SearchException("Exception while searching locally", e);
      }
      return results;
   }

   private CompletionStage<QueryResponse> localInvoke(SegmentsClusteredQueryCommand cmd) {
      return cmd.perform(cache);
   }
}
