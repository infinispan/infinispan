package org.infinispan.query.clustered;

import static org.infinispan.util.concurrent.CompletableFutures.sequence;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.hibernate.search.exception.SearchException;
import org.infinispan.AdvancedCache;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.SingleResponseCollector;

/**
 * Invoke a ClusteredQueryCommand on the cluster, including on own node.
 *
 * @author Israel Lacerra &lt;israeldl@gmail.com&gt;
 * @author Sanne Grinovero &lt;sanne@infinispan.org&gt; (C) 2011 Red Hat Inc.
 * @since 5.1
 */
final class ClusteredQueryInvoker {

   private final RpcManager rpcManager;
   private final AdvancedCache<?, ?> cache;
   private final Address myAddress;
   private final ExecutorService asyncExecutor;
   private final RpcOptions rpcOptions;
   private final QueryPartitioner partitioner;

   ClusteredQueryInvoker(AdvancedCache<?, ?> cache, ExecutorService asyncExecutor) {
      this.cache = cache;
      this.asyncExecutor = asyncExecutor;
      this.rpcManager = cache.getRpcManager();
      this.myAddress = rpcManager.getAddress();
      this.rpcOptions = rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS).timeout(10000, TimeUnit.MILLISECONDS).build();
      this.partitioner = new QueryPartitioner(cache);
   }

   /**
    * Send this ClusteredQueryCommand to a node.
    *
    * @param address               Address of the destination node
    * @param cmd
    * @return the response
    */
   QueryResponse unicast(Address address, SegmentsClusteredQueryCommand cmd) {
      if (address.equals(myAddress)) {
         Future<QueryResponse> localResponse = localInvoke(cmd);
         try {
            return localResponse.get();
         } catch (InterruptedException e) {
            throw new SearchException("Interrupted while searching locally", e);
         } catch (ExecutionException e) {
            throw new SearchException("Exception while searching locally", e);
         }
      } else {
         Map<Address, Response> responses = rpcManager.invokeRemotely(Collections.singletonList(address), cmd, rpcOptions);
         List<QueryResponse> queryResponses = cast(responses);
         return queryResponses.get(0);
      }
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
      Map<Address, BitSet> split = partitioner.split();
      SegmentsClusteredQueryCommand localCommand = new SegmentsClusteredQueryCommand(cache.getName(), operation, split.get(myAddress));
      // invoke on own node
      Future<QueryResponse> localResponse = localInvoke(localCommand);
      List<CompletableFuture<QueryResponse>> futureRemoteResponses = split.entrySet().stream()
            .filter(e -> !e.getKey().equals(myAddress)).map(e -> {
               Address address = e.getKey();
               BitSet segments = e.getValue();
               SegmentsClusteredQueryCommand cmd = new SegmentsClusteredQueryCommand(cache.getName(), operation, segments);
               return rpcManager.invokeCommand(address, cmd, SingleResponseCollector.validOnly(), rpcOptions).toCompletableFuture();
            }).map(a -> a.thenApply(r -> (QueryResponse) r.getResponseValue())).collect(Collectors.toList());

      List<QueryResponse> results = new ArrayList<>();
      try {
         results.add(localResponse.get());
         List<QueryResponse> responseList = sequence(futureRemoteResponses).get();
         results.addAll(responseList);
      } catch (InterruptedException e) {
         throw new SearchException("Interrupted while searching locally", e);
      } catch (ExecutionException e) {
         throw new SearchException("Exception while searching locally", e);
      }
      return results;
   }

   private Future<QueryResponse> localInvoke(SegmentsClusteredQueryCommand cmd) {
      return asyncExecutor.submit(() -> {
         try {
            return cmd.perform(cache);
         } catch (Throwable e) {
            throw new SearchException(e);
         }
      });
   }

   private List<QueryResponse> cast(Map<Address, Response> responses) {
      List<QueryResponse> queryResponses = new LinkedList<>();
      for (Response resp : responses.values()) {
         if (resp instanceof SuccessfulResponse) {
            QueryResponse response = (QueryResponse) ((SuccessfulResponse) resp).getResponseValue();
            queryResponses.add(response);
         } else {
            throw new SearchException("Unexpected response: " + resp);
         }
      }
      return queryResponses;
   }
}
