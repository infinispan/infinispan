package org.infinispan.interceptors.impl;

import static org.infinispan.util.logging.Log.CLUSTER;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.container.impl.EntryFactory;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.expiration.impl.TouchCommand;
import org.infinispan.factories.annotations.Inject;
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

   protected LocalizedCacheTopology checkTopologyId(TopologyAffectedCommand command) {
      LocalizedCacheTopology cacheTopology = distributionManager.getCacheTopology();
      int currentTopologyId = cacheTopology.getTopologyId();
      int cmdTopology = command.getTopologyId();
      if (command instanceof FlagAffectedCommand && ((((FlagAffectedCommand) command).hasAnyFlag(FlagBitSets.SKIP_OWNERSHIP_CHECK | FlagBitSets.CACHE_MODE_LOCAL)))) {
         getLog().tracef("Skipping topology check for command %s", command);
         return cacheTopology;
      }
      if (getLog().isTraceEnabled()) {
         getLog().tracef("Current topology %d, command topology %d", currentTopologyId, cmdTopology);
      }
      if (cmdTopology >= 0 && currentTopologyId != cmdTopology) {
         throw OutdatedTopologyException.RETRY_NEXT_TOPOLOGY;
      }
      return cacheTopology;
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
         return (Boolean) response.getResponseValue();
      }

      abstract Boolean addBooleanResponse(Address sender, Boolean response);
   }

   private static class ScatteredTouchResponseCollector extends AbstractTouchResponseCollector {

      private static final ScatteredTouchResponseCollector INSTANCE = new ScatteredTouchResponseCollector();

      @Override
      public Boolean finish() {
         // No other node was touched
         return Boolean.FALSE;
      }

      @Override
      protected Boolean addBooleanResponse(Address sender, Boolean response) {
         if (response == Boolean.TRUE) {
            // Return early if any node touched the value - as SCATTERED only exists on a single backup!
            // TODO: what if the read was when one of the backups or primary died?
            return Boolean.TRUE;
         }
         return null;
      }
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
   public Object visitTouchCommand(InvocationContext ctx, TouchCommand command) throws Throwable {
      if (isLocalModeForced(command)) {
         return invokeNext(ctx, command);
      }
      LocalizedCacheTopology cacheTopology = checkTopologyId(command);
      DistributionInfo info = cacheTopology.getSegmentDistribution(command.getSegment());

      if (info.isPrimary()) {
         boolean isScattered = cacheConfiguration.clustering().cacheMode().isScattered();
         // Scattered any node could be a backup, so we have to touch all members
         List<Address> owners = isScattered ? cacheTopology.getActualMembers() : info.readOwners();
         AbstractTouchResponseCollector collector = isScattered ? ScatteredTouchResponseCollector.INSTANCE :
               TouchResponseCollector.INSTANCE;
         CompletionStage<Boolean> remoteInvocation = rpcManager.invokeCommand(owners, command, collector,
               rpcManager.getSyncRpcOptions());
         return invokeNextThenApply(ctx, command, (rCtx, rCommand, rValue) -> {
            Boolean touchedLocally = (Boolean) rValue;
            Boolean collectedResponse = collector.addBooleanResponse(null, touchedLocally);
            if (collectedResponse != null) {
               return asyncValue(remoteInvocation.thenApply(ignore -> touchedLocally));
            }
            return asyncValue(remoteInvocation);
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
