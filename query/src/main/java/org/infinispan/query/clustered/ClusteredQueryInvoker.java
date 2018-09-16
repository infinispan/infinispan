package org.infinispan.query.clustered;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.hibernate.search.exception.SearchException;
import org.infinispan.AdvancedCache;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;

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

   ClusteredQueryInvoker(AdvancedCache<?, ?> cache, ExecutorService asyncExecutor) {
      this.cache = cache;
      this.asyncExecutor = asyncExecutor;
      this.rpcManager = cache.getRpcManager();
      this.myAddress = rpcManager.getAddress();
      this.rpcOptions = rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS).timeout(10000, TimeUnit.MILLISECONDS).build();
   }

   /**
    * Send this ClusteredQueryCommand to a node.
    *
    * @param address               Address of the destination node
    * @param clusteredQueryCommand
    * @return the response
    */
   QueryResponse unicast(Address address, ClusteredQueryCommand clusteredQueryCommand) {
      if (address.equals(myAddress)) {
         Future<QueryResponse> localResponse = localInvoke(clusteredQueryCommand);
         try {
            return localResponse.get();
         } catch (InterruptedException e) {
            throw new SearchException("Interrupted while searching locally", e);
         } catch (ExecutionException e) {
            throw new SearchException("Exception while searching locally", e);
         }
      } else {
         Map<Address, Response> responses = rpcManager.invokeRemotely(Collections.singletonList(address), clusteredQueryCommand, rpcOptions);
         List<QueryResponse> queryResponses = cast(responses);
         return queryResponses.get(0);
      }
   }

   /**
    * Broadcast this ClusteredQueryCommand to all cluster nodes. The command will be also invoked on local node.
    *
    * @param clusteredQueryCommand
    * @return the responses
    */
   List<QueryResponse> broadcast(ClusteredQueryCommand clusteredQueryCommand) {
      // invoke on own node
      Future<QueryResponse> localResponse = localInvoke(clusteredQueryCommand);
      Map<Address, Response> responses = rpcManager.invokeRemotely(null, clusteredQueryCommand, rpcOptions);
      List<QueryResponse> queryResponses = cast(responses);
      try {
         queryResponses.add(localResponse.get());
      } catch (InterruptedException e) {
         throw new SearchException("Interrupted while searching locally", e);
      } catch (ExecutionException e) {
         throw new SearchException("Exception while searching locally", e);
      }
      return queryResponses;
   }

   private Future<QueryResponse> localInvoke(ClusteredQueryCommand clusteredQuery) {
      return asyncExecutor.submit(() -> {
         try {
            return clusteredQuery.perform(cache);
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
