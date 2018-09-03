package org.infinispan.interceptors.impl;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.container.impl.EntryFactory;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.AllOwnersLostException;
import org.infinispan.util.concurrent.locks.LockManager;

/**
 * Base class for distribution interceptors.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
public abstract class ClusteringInterceptor extends BaseRpcInterceptor {

   @Inject protected CommandsFactory cf;
   @Inject protected EntryFactory entryFactory;
   @Inject protected LockManager lockManager;
   @Inject protected InternalDataContainer dataContainer;
   @Inject protected DistributionManager distributionManager;

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

   /**
    * {@link #completeExceptionally(Throwable)} must be called from synchronized block since we must not complete
    * the future (exceptionally) when we're accessing the context - if there was an exception and we would retry,
    * the context could be accessed concurrently by dangling handlers and retry execution (that does not synchronize
    * on the same future).
    *
    * When completeExceptionally executes before the other responses processing, the future will be marked as done
    * and as soon as the other responses get into the synchronized block, these will check isDone and return.
    * If the response is being processed in sync block, running completeExceptionally and the related callbacks
    * will be blocked until we finish the processing.
    */
   protected class ClusteredGetAllFuture extends CompletableFuture<Void> {
      public int counter;

      public ClusteredGetAllFuture(int counter) {
         this.counter = counter;
      }

      @Override
      public synchronized boolean completeExceptionally(Throwable ex) {
         return super.completeExceptionally(ex);
      }
   }
}
