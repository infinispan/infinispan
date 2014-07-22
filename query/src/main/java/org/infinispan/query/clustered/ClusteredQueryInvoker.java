package org.infinispan.query.clustered;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.hibernate.search.exception.SearchException;
import org.infinispan.Cache;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;

/**
 * Invoke a ClusteredQueryCommand on the cluster, including on own node.
 * 
 * @author Israel Lacerra <israeldl@gmail.com>
 * @author Sanne Grinovero <sanne@infinispan.org> (C) 2011 Red Hat Inc.
 * @since 5.1
 */
public class ClusteredQueryInvoker {

   private final RpcManager rpcManager;
   private final Cache<?, ?> localCacheInstance;
   private final Address myAddress;
   private final ExecutorService asyncExecutor;
   private final RpcOptions rpcOptions;

   ClusteredQueryInvoker(Cache<?, ?> localCacheInstance, ExecutorService asyncExecutor) {
      this.asyncExecutor = asyncExecutor;
      this.rpcManager = localCacheInstance.getAdvancedCache().getComponentRegistry().getLocalComponent(RpcManager.class);
      this.localCacheInstance = localCacheInstance;
      this.myAddress = localCacheInstance.getAdvancedCache().getRpcManager().getAddress();
      this.rpcOptions = rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS).timeout(10000, TimeUnit.MILLISECONDS).build();
   }

   /**
    * Retrieves the value (using doc index) in a remote query instance
    * 
    * @param doc
    *           Doc index of the value on remote query
    * @param address
    *           Address of the node who has the value
    * @param queryId
    *           Id of the query
    * @return The value of index doc of the query with queryId on node at address
    */
   public Object getValue(int doc, Address address, UUID queryId) {
      ClusteredQueryCommand clusteredQuery = ClusteredQueryCommand.retrieveKeyFromLazyQuery(
               localCacheInstance, queryId, doc);

      if (address.equals(myAddress)) {
         Future<QueryResponse> localResponse = localInvoke(clusteredQuery);
         try {
            return localResponse.get().getFetchedValue();
         } catch (InterruptedException e) {
            throw new SearchException("interrupted while searching locally", e);
         } catch (ExecutionException e) {
            throw new SearchException("Exception while searching locally", e);
         }
      } else {
         List<Address> addresss = new ArrayList<Address>(1);
         addresss.add(address);

         Map<Address, Response> responses = rpcManager.invokeRemotely(addresss, clusteredQuery, rpcOptions);
         List<QueryResponse> objects = cast(responses);
         return objects.get(0).getFetchedValue();
      }
   }

   /**
    * Broadcast this ClusteredQueryCommand to all cluster nodes. The command will be also invoked on
    * local node.
    * 
    * @param clusteredQuery
    * @return A list with all responses
    */
   public List<QueryResponse> broadcast(ClusteredQueryCommand clusteredQuery) {
      // invoke on own node
      Future<QueryResponse> localResponse = localInvoke(clusteredQuery);
      Map<Address, Response> responses = rpcManager.invokeRemotely(null, clusteredQuery, rpcOptions);

      List<QueryResponse> objects = cast(responses);
      final QueryResponse localReturnValue;
      try {
         localReturnValue = localResponse.get();
      } catch (InterruptedException e1) {
         throw new SearchException("interrupted while searching locally", e1);
      } catch (ExecutionException e1) {
         throw new SearchException("Exception while searching locally", e1);
      }
      objects.add(localReturnValue);
      return objects;
   }

   private Future<QueryResponse> localInvoke(ClusteredQueryCommand clusteredQuery) {
      ClusteredQueryCallable clusteredQueryCallable = new ClusteredQueryCallable(clusteredQuery,
               localCacheInstance);
      return asyncExecutor.submit(clusteredQueryCallable);
   }

   private List<QueryResponse> cast(Map<Address, Response> responses) {
      List<QueryResponse> objects = new LinkedList<QueryResponse>();
      for (Entry<Address, Response> pair : responses.entrySet()) {
         Response resp = pair.getValue();
         if (resp instanceof SuccessfulResponse) {
            QueryResponse response = (QueryResponse) ((SuccessfulResponse) resp).getResponseValue();
            objects.add(response);
         } else {
            throw new SearchException("Unexpected response: " + resp);
         }
      }

      return objects;
   }

   /**
    * Created to call a ClusteredQueryCommand on own node.
    * 
    * @author Israel Lacerra <israeldl@gmail.com>
    * @since 5.1
    */
   private static final class ClusteredQueryCallable implements Callable<QueryResponse> {

      private final ClusteredQueryCommand clusteredQuery;

      private final Cache<?, ?> localInstance;

      ClusteredQueryCallable(ClusteredQueryCommand clusteredQuery, Cache<?, ?> localInstance) {
         this.clusteredQuery = clusteredQuery;
         this.localInstance = localInstance;
      }

      @Override
      public QueryResponse call() throws Exception {
         try {
            return clusteredQuery.perform(localInstance);
         } catch (Throwable e) {
            throw new Exception(e);
         }
      }

   }

}
