package org.infinispan.xsite;

import static org.infinispan.remoting.transport.impl.MapResponseCollector.validOnly;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.remoting.LocalInvocation;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollector;
import org.infinispan.remoting.transport.ResponseCollectors;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.statetransfer.XSiteState;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
import org.infinispan.xsite.statetransfer.XSiteStateTransferControlCommand;

/**
 * {@link org.infinispan.xsite.BackupReceiver} implementation for clustered caches.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public class ClusteredCacheBackupReceiver extends BaseBackupReceiver {

   private static final Log log = LogFactory.getLog(ClusteredCacheBackupReceiver.class);
   private static final boolean trace = log.isDebugEnabled();

   ClusteredCacheBackupReceiver(Cache<Object, Object> cache) {
      super(cache);
   }

   @Override
   public CompletionStage<Void> handleStateTransferControl(XSiteStateTransferControlCommand command) {
      XSiteStateTransferControlCommand invokeCommand = command;
      if (!command.getCacheName().equals(cacheName)) {
         //copy if the cache name is different
         invokeCommand = command.copyForCache(cacheName);
      }
      invokeCommand.setSiteName(command.getOriginSite());
      return invokeRemotelyInLocalSite(invokeCommand);
   }

   @Override
   public CompletionStage<Void> handleStateTransferState(XSiteStatePushCommand cmd) {
      //split the state and forward it to the primary owners...
      CompletableFuture<Void> allowInvocation = checkInvocationAllowedFuture();
      if (allowInvocation != null) {
         return allowInvocation;
      }

      final long endTime = timeService.expectedEndTime(cmd.getTimeout(), TimeUnit.MILLISECONDS);
      final ClusteringDependentLogic clusteringDependentLogic = cache.getComponentRegistry()
            .getComponent(ClusteringDependentLogic.class);
      final Map<Address, List<XSiteState>> primaryOwnersChunks = new HashMap<>();
      final Address localAddress = clusteringDependentLogic.getAddress();

      if (trace) {
         log.tracef("Received X-Site state transfer '%s'. Splitting by primary owner.", cmd);
      }

      for (XSiteState state : cmd.getChunk()) {
         Address primaryOwner = clusteringDependentLogic.getCacheTopology().getDistribution(state.key()).primary();
         List<XSiteState> primaryOwnerList = primaryOwnersChunks.computeIfAbsent(primaryOwner, k -> new LinkedList<>());
         primaryOwnerList.add(state);
      }

      final List<XSiteState> localChunks = primaryOwnersChunks.remove(localAddress);
      AggregateCompletionStage<Void> cf = CompletionStages.aggregateCompletionStage();

      for (Map.Entry<Address, List<XSiteState>> entry : primaryOwnersChunks.entrySet()) {
         if (entry.getValue() == null || entry.getValue().isEmpty()) {
            continue;
         }
         if (trace) {
            log.tracef("Node '%s' will apply %s", entry.getKey(), entry.getValue());
         }
         StatePushTask task = new StatePushTask(entry.getValue(), entry.getKey(), cache, endTime);
         task.executeRemote();
         cf.dependsOn(task);
      }

      //help gc. this is safe because the chunks was already sent
      primaryOwnersChunks.clear();

      if (trace) {
         log.tracef("Local node '%s' will apply %s", localAddress, localChunks);
      }

      if (localChunks != null) {
         StatePushTask task = new StatePushTask(localChunks, localAddress, cache, endTime);
         task.executeLocal();
         cf.dependsOn(task);
      }

      return cf.freeze().thenApply(this::assertAllowInvocationFunction);
   }

   private CompletionStage<Void> invokeRemotelyInLocalSite(CacheRpcCommand command) {
      final RpcManager rpcManager = cache.getRpcManager();
      CompletionStage<Map<Address, Response>> remote = rpcManager
            .invokeCommandOnAll(command, validOnly(), rpcManager.getSyncRpcOptions());
      CompletableFuture<Response> local = LocalInvocation.newInstanceFromCache(cache, command).callAsync();
      return CompletableFuture.allOf(remote.toCompletableFuture(), local);
   }

   private class StatePushTask extends CompletableFuture<Void>
         implements ResponseCollector<Response>, BiFunction<Response, Throwable, Void> {
      private final List<XSiteState> chunk;
      private final Address address;
      private final AdvancedCache<?, ?> cache;
      private final long endTime;


      private StatePushTask(List<XSiteState> chunk, Address address, AdvancedCache<?, ?> cache, long endTime) {
         this.chunk = chunk;
         this.address = address;
         this.cache = cache;
         this.endTime = endTime;
      }

      @Override
      public Void apply(Response response, Throwable throwable) {
         if (throwable != null) {
            if (isShouldGiveUp()) {
               return null;
            }

            RpcManager rpcManager = cache.getRpcManager();

            if (rpcManager.getMembers().contains(this.address) && !rpcManager.getAddress().equals(this.address)) {
               if (trace) {
                  log.tracef(throwable, "An exception was sent by %s. Retrying!", this.address);
               }
               executeRemote(); //retry remote
            } else {
               if (trace) {
                  log.tracef(throwable, "An exception was sent by %s. Retrying locally!", this.address);
               }
               //if the node left the cluster, we apply the missing state. This avoids the site provider to re-send the
               //full chunk.
               executeLocal(); //retry locally
            }
         } else if (response == CacheNotFoundResponse.INSTANCE) {
            if (trace) {
               log.tracef("Cache not found in node '%s'. Retrying locally!", address);
            }
            if (isShouldGiveUp()) {
               return null;
            }
            executeLocal(); //retry locally
         } else {
            complete(null);
         }
         return null;
      }

      @Override
      public Response addResponse(Address sender, Response response) {
         if (response instanceof ValidResponse || response instanceof CacheNotFoundResponse) {
            return response;
         } else if (response instanceof ExceptionResponse) {
            throw ResponseCollectors.wrapRemoteException(sender, ((ExceptionResponse) response).getException());
         } else {
            throw ResponseCollectors
                  .wrapRemoteException(sender, new RpcException("Unknown response type: " + response));
         }
      }

      @Override
      public Response finish() {
         return null;
      }

      void executeRemote() {
         RpcManager rpcManager = cache.getRpcManager();
         RpcOptions rpcOptions = rpcManager.getSyncRpcOptions();
         rpcManager.invokeCommand(address, newStatePushCommand(cache, chunk), this, rpcOptions)
               .handle(this);
      }

      void executeLocal() {
         LocalInvocation.newInstanceFromCache(cache, newStatePushCommand(cache, chunk)).callAsync()
               .handle(this);
      }

      /**
       * @return {@code null} if it can retry
       */
      private boolean isShouldGiveUp() {
         ComponentStatus status = cache.getStatus();
         if (!status.allowInvocations()) {
            completeExceptionally(new IllegalLifecycleStateException("Cache is stopping or terminated: " + status));
            return true;
         }
         if (timeService.isTimeExpired(endTime)) {
            completeExceptionally(new TimeoutException("Unable to apply state in the time limit."));
            return true;
         }
         return false;
      }
   }
}
