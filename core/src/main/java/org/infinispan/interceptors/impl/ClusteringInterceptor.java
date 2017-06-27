package org.infinispan.interceptors.impl;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.EntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.AllOwnersLostException;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.util.concurrent.locks.LockManager;

/**
 * Base class for distribution interceptors.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
public abstract class ClusteringInterceptor extends BaseRpcInterceptor {

   protected CommandsFactory cf;
   protected EntryFactory entryFactory;
   protected LockManager lockManager;
   protected DataContainer dataContainer;
   protected StateTransferManager stateTransferManager;

   protected static Response getSingleResponse(Map<Address, Response> responseMap) {
      Iterator<Response> it = responseMap.values().iterator();
      if (!it.hasNext()) {
         throw AllOwnersLostException.INSTANCE;
      }
      Response response = it.next();
      if (it.hasNext()) {
         throw new IllegalStateException("Too many responses " + responseMap);
      }
      return response;
   }

   protected static SuccessfulResponse getSuccessfulResponseOrFail(Map<Address, Response> responseMap, CompletableFuture<?> future, Consumer<Response> cacheNotFound) {
      Iterator<Response> it = responseMap.values().iterator();
      if (!it.hasNext()) {
         future.completeExceptionally(AllOwnersLostException.INSTANCE);
         return null;
      }
      Response response = it.next();
      if (it.hasNext()) {
         future.completeExceptionally(new IllegalStateException("Too many responses " + responseMap));
      } else if (response instanceof SuccessfulResponse) {
         return (SuccessfulResponse) response;
      } else if (response instanceof CacheNotFoundResponse || response instanceof UnsureResponse) {
         if (cacheNotFound == null) {
            future.completeExceptionally(unexpected(response));
         } else {
            try {
               cacheNotFound.accept(response);
            } catch (Throwable t) {
               future.completeExceptionally(t);
            }
         }
      } else {
         future.completeExceptionally(unexpected(response));
      }
      return null;
   }

   protected static IllegalArgumentException unexpected(Response response) {
      return new IllegalArgumentException("Unexpected response " + response);
   }

   @Inject
   public void injectDependencies(CommandsFactory cf, EntryFactory entryFactory, LockManager lockManager,
         DataContainer dataContainer, StateTransferManager stateTransferManager) {
      this.cf = cf;
      this.entryFactory = entryFactory;
      this.lockManager = lockManager;
      this.dataContainer = dataContainer;
      this.stateTransferManager = stateTransferManager;
   }

   /**
    * {@link #completeExceptionally(Throwable)} must be called from synchronized block since we must not complete
    * the future (exceptionally) when we're accessing the context - if there was an exception and we would retry,
    * the context could be accessed concurrently by dangling handlers and retry execution (that does not synchronize
    * on the same future).
    */
   protected class ClusteredGetAllFuture extends CompletableFuture<Void> implements InvocationSuccessFunction {
      public GetAllCommand localCommand;
      public int counter;
      public boolean hasUnsureResponse;
      public boolean lostData;

      public ClusteredGetAllFuture(int counter, GetAllCommand localCommand) {
         this.counter = counter;
         this.localCommand = localCommand;
      }

      @Override
      public synchronized boolean completeExceptionally(Throwable ex) {
         return super.completeExceptionally(ex);
      }

      @Override
      public Object apply(InvocationContext rCtx, VisitableCommand rCommand, Object rv) throws Throwable {
         assert rv == null; // value with which the allFuture has been completed
         // If we've lost data but did not get any unsure responses we should return map without some entries.
         // If we've got unsure response but did not lose any data - no problem, there has been another
         // response delivering the results.
         // Only if those two combine we'll rather throw OTE and retry.
         if (hasUnsureResponse && lostData) {
            throw OutdatedTopologyException.INSTANCE;
         }
         return invokeNext(rCtx, localCommand);
      }
   }
}
