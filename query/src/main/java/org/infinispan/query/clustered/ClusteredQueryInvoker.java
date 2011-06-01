/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.query.clustered;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import org.infinispan.Cache;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;

/**
 * Invoke a CusteredQueryCommand on the cluster, including on own node. 
 * 
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
public class ClusteredQueryInvoker {

   private final RpcManager rpcManager;

   private final Cache localCacheInstance;

   private final Address myAddress;
   
   ClusteredQueryInvoker(Cache localCacheInstance) {
      this.rpcManager = localCacheInstance.getAdvancedCache().getComponentRegistry()
               .getLocalComponent(RpcManager.class);
      this.localCacheInstance = localCacheInstance;
      this.myAddress = localCacheInstance.getAdvancedCache().getRpcManager().getAddress();
   }

   public Object getValue(int doc, Address address, UUID queryId) {
      ClusteredQueryCommand clusteredQuery = ClusteredQueryCommand.retrieveKeyFromLazyQuery(
               localCacheInstance, queryId, doc);

      if (address.equals(myAddress)) {
         FutureTask<Object> localResponse = localInvoke(clusteredQuery);
         try {
            return localResponse.get();
         } catch (InterruptedException e) {
            //FIXME
            e.printStackTrace();
            return null;
         } catch (ExecutionException e) {
            //FIXME
            e.printStackTrace();
            return null;
         }
      } else {

         List<Address> addresss = new ArrayList<Address>(1);
         addresss.add(address);

         try {
            Map<Address, Response> responses = rpcManager.invokeRemotely(addresss, clusteredQuery,
                     ResponseMode.SYNCHRONOUS, 10000);
            List<Object> objects = cast(responses);
            return objects.get(0);
         } catch (Exception e) {
            // FIXME
            e.printStackTrace();
            return null;
         }
      }
   }

   public List<Object> broadcast(ClusteredQueryCommand clusteredQuery) throws Exception {
      // invoke on own node
      FutureTask<Object> localResponse = localInvoke(clusteredQuery);

      List<Object> objects = cast(rpcManager.invokeRemotely(null, clusteredQuery,
               ResponseMode.SYNCHRONOUS, 10000));
      objects.add(localResponse.get());
      return objects;
   }

   private FutureTask<Object> localInvoke(ClusteredQueryCommand clusteredQuery) {
      Executor executor = Executors.newSingleThreadExecutor();
      FutureTask<Object> future = new FutureTask<Object>(new ClusteredQueryCallable(clusteredQuery,
               localCacheInstance));
      executor.execute(future);

      return future;
   }

   private List<Object> cast(Map<Address, Response> responses) {
      List<Object> objects = new LinkedList<Object>();
      for (Entry<Address, Response> pair : responses.entrySet()) {
         Response resp = pair.getValue();
         if (resp instanceof SuccessfulResponse) {
            Object response = ((SuccessfulResponse) resp).getResponseValue();
            objects.add(response);
         }else{
            //TODO
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
   private class ClusteredQueryCallable implements Callable<Object> {

      private final ClusteredQueryCommand clusteredQuery;

      private final Cache localInstance;

      ClusteredQueryCallable(ClusteredQueryCommand clusteredQuery, Cache localInstance) {
         this.clusteredQuery = clusteredQuery;
         this.localInstance = localInstance;
      }

      @Override
      public Object call() throws Exception {
         try {
            return clusteredQuery.perform(localInstance);
         } catch (Throwable e) {
            e.printStackTrace();
            throw new Exception(e.getMessage());
         }
      }

   }

}
