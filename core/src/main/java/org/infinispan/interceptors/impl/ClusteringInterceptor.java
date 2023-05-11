package org.infinispan.interceptors.impl;

import static org.infinispan.util.logging.Log.CLUSTER;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.container.impl.EntryFactory;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.expiration.TouchMode;
import org.infinispan.expiration.impl.TouchCommand;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.reactive.publisher.PublisherReducers;
import org.infinispan.reactive.publisher.impl.ClusterPublisherManager;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.UnsureResponse;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ValidResponseCollector;
import org.infinispan.remoting.transport.impl.SingleResponseCollector;
import org.infinispan.statetransfer.AllOwnersLostException;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.util.CacheTopologyUtil;
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
   @Inject ClusterPublisherManager<?, ?> clusterPublisherManager;

   private TouchMode touchMode;

   @Override
   public void init() {
      super.init();

      if (cacheConfiguration.clustering().cacheMode().isSynchronous()) {
         touchMode = cacheConfiguration.expiration().touch();
      } else {
         touchMode = TouchMode.ASYNC;
      }
   }

   protected LocalizedCacheTopology getCacheTopology() {
      return distributionManager.getCacheTopology();
   }

   private static abstract class AbstractTouchResponseCollector extends ValidResponseCollector<Boolean> {
      @Override
      protected Boolean addTargetNotFound(Address sender) {
         throw OutdatedTopologyException.RETRY_NEXT_TOPOLOGY;
      }

      @Override
      protected Boolean addException(Address sender, Exception exception) {
         if (exception instanceof CacheException) {
            throw (CacheException) exception;
         }
         throw new CacheException(exception);
      }

      @Override
      protected final Boolean addValidResponse(Address sender, ValidResponse response) {
         return addBooleanResponse(sender, (Boolean) response.getResponseValue());
      }

      abstract Boolean addBooleanResponse(Address sender, Boolean response);
   }

   private static class TouchResponseCollector extends AbstractTouchResponseCollector {

      private static final TouchResponseCollector INSTANCE = new TouchResponseCollector();

      @Override
      public Boolean finish() {
         // If all were touched, then the value isn't expired
         return Boolean.TRUE;
      }

      @Override
      protected Boolean addBooleanResponse(Address sender, Boolean response) {
         if (response == Boolean.FALSE) {
            // Return early if any value wasn't touched!
            return Boolean.FALSE;
         }
         return null;
      }
   }

   @Override
   public Object visitSizeCommand(InvocationContext ctx, SizeCommand command) throws Throwable {
      if (isLocalModeForced(command)) {
         return invokeNext(ctx, command);
      }

      if (command.hasAnyFlag(FlagBitSets.SKIP_SIZE_OPTIMIZATION)) {
         return asyncValue(clusterPublisherManager.keyReduction(false, null, null, ctx,
               command.getFlagsBitSet(), DeliveryGuarantee.EXACTLY_ONCE, PublisherReducers.count(), PublisherReducers.add()));
      }

      return asyncValue(clusterPublisherManager.sizePublisher(command.getSegments(), ctx, command.getFlagsBitSet()));
   }

   @Override
   public Object visitTouchCommand(InvocationContext ctx, TouchCommand command) throws Throwable {
      if (command.hasAnyFlag(FlagBitSets.CACHE_MODE_LOCAL | FlagBitSets.SKIP_REMOTE_LOOKUP)) {
         return invokeNext(ctx, command);
      }
      LocalizedCacheTopology cacheTopology = CacheTopologyUtil.checkTopology(command, getCacheTopology());
      DistributionInfo info = cacheTopology.getSegmentDistribution(command.getSegment());
      // Scattered any node could be a backup, so we have to touch all members
      List<Address> owners = info.readOwners();

      if (touchMode == TouchMode.ASYNC) {
         if (ctx.isOriginLocal()) {
            // Send to all the owners
            rpcManager.sendToMany(owners, command, DeliverOrder.NONE);
         }
         return invokeNext(ctx, command);
      }

      if (info.isPrimary()) {
         AbstractTouchResponseCollector collector = TouchResponseCollector.INSTANCE;
         CompletionStage<Boolean> remoteInvocation = rpcManager.invokeCommand(owners, command, collector,
               rpcManager.getSyncRpcOptions());
         return invokeNextThenApply(ctx, command, (rCtx, rCommand, rValue) -> {
            Boolean touchedLocally = (Boolean) rValue;
            if (touchedLocally) {
               return asyncValue(remoteInvocation);
            }
            // If primary can't touch - it doesn't matter about others
            return Boolean.FALSE;
         });
      } else if (ctx.isOriginLocal()) {
         // Send to the primary owner
         CompletionStage<ValidResponse> remoteInvocation = rpcManager.invokeCommand(info.primary(), command,
               SingleResponseCollector.validOnly(), rpcManager.getSyncRpcOptions());
         return asyncValue(remoteInvocation)
               .thenApply(ctx, command, (rCtx, rCommand, rResponse) -> ((ValidResponse) rResponse).getResponseValue());
      }
      return invokeNext(ctx, command);
   }

   protected static SuccessfulResponse getSuccessfulResponseOrFail(Map<Address, Response> responseMap, CompletableFuture<?> future, Consumer<Response> cacheNotFound) {
      Iterator<Map.Entry<Address, Response>> it = responseMap.entrySet().iterator();
      if (!it.hasNext()) {
         future.completeExceptionally(AllOwnersLostException.INSTANCE);
         return null;
      }
      Map.Entry<Address, Response> e = it.next();
      Address sender = e.getKey();
      Response response = e.getValue();
      if (it.hasNext()) {
         future.completeExceptionally(new IllegalStateException("Too many responses " + responseMap));
      } else if (response instanceof SuccessfulResponse) {
         return (SuccessfulResponse) response;
      } else if (response instanceof CacheNotFoundResponse || response instanceof UnsureResponse) {
         if (cacheNotFound == null) {
            future.completeExceptionally(unexpected(sender, response));
         } else {
            try {
               cacheNotFound.accept(response);
            } catch (Throwable t) {
               future.completeExceptionally(t);
            }
         }
      } else {
         future.completeExceptionally(unexpected(sender, response));
      }
      return null;
   }

   protected static RuntimeException unexpected(Address sender, Response response) {
      return CLUSTER.unexpectedResponse(sender, response);
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
