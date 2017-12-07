package org.infinispan.xsite;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.LocalInvocation;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.remoting.transport.impl.SingleResponseCollector;
import org.infinispan.util.concurrent.CompletableFutures;
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

   public ClusteredCacheBackupReceiver(Cache<Object, Object> cache) {
      super(cache);
   }

   @Override
   public void handleStateTransferControl(XSiteStateTransferControlCommand command) throws Exception {
      XSiteStateTransferControlCommand invokeCommand = command;
      if (!command.getCacheName().equals(cacheName)) {
         //copy if the cache name is different
         invokeCommand = command.copyForCache(cacheName);
      }
      invokeCommand.setSiteName(command.getOriginSite());
      invokeRemotelyInLocalSite(invokeCommand);
   }

   @Override
   public void handleStateTransferState(XSiteStatePushCommand cmd) throws Exception {
      //split the state and forward it to the primary owners...
      assertAllowInvocation();
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
      final List<StatePushTask> tasks = new ArrayList<>(primaryOwnersChunks.size());

      for (Map.Entry<Address, List<XSiteState>> entry : primaryOwnersChunks.entrySet()) {
         if (entry.getValue() == null || entry.getValue().isEmpty()) {
            continue;
         }
         if (trace) {
            log.tracef("Node '%s' will apply %s", entry.getKey(), entry.getValue());
         }
         StatePushTask task = new StatePushTask(entry.getValue(), entry.getKey(), cache);
         tasks.add(task);
         task.executeRemote();
      }

      //help gc. this is safe because the chunks was already sent
      primaryOwnersChunks.clear();

      if (trace) {
         log.tracef("Local node '%s' will apply %s", localAddress, localChunks);
      }

      if (localChunks != null) {
         StatePushTask task = new StatePushTask(localChunks, localAddress, cache);
         tasks.add(task);
         task.executeLocal();
      }

      if (trace) {
         log.tracef("Waiting for the remote tasks...");
      }

      while (!tasks.isEmpty() && !timeService.isTimeExpired(endTime)) {
         for (Iterator<StatePushTask> iterator = tasks.iterator(); iterator.hasNext(); ) {
            if (awaitRemoteTask(iterator.next())) {
               iterator.remove();
            }
         }
      }
      //the put operation can fail silently. check in the end and it is better to resend the chunk than to lose keys.
      assertAllowInvocation();
      if (!tasks.isEmpty()) {
         throw new TimeoutException("Unable to apply state in the time limit.");
      }
   }

   private boolean awaitRemoteTask(StatePushTask task) throws Exception {
      try {
         if (trace) {
            log.tracef("Waiting reply from %s", task.address);
         }
         Response response = task.awaitResponse();
         if (trace) {
            log.tracef("Response received is %s", response);
         }
         if (response == CacheNotFoundResponse.INSTANCE) {
            if (trace) {
               log.tracef("Cache not found in node '%s'. Retrying locally!", task.address);
            }
            assertAllowInvocation();
            task.executeLocal();
         }
      } catch (Exception e) {
         assertAllowInvocation();
         RpcManager rpcManager = cache.getRpcManager();
         if (rpcManager.getMembers().contains(task.address) &&
               !rpcManager.getAddress().equals(task.address)) {
            if (trace) {
               log.tracef(e, "An exception was sent by %s. Retrying!", task.address);
            }
            task.executeRemote(); //retry!
            return false;
         } else {
            if (trace) {
               log.tracef(e, "An exception was sent by %s. Retrying locally!", task.address);
            }
            //if the node left the cluster, we apply the missing state. This avoids the site provider to re-send the
            //full chunk.
            task.executeLocal();
            return false;
         }
      }
      return true;
   }

   private Map<Address, Response> invokeRemotelyInLocalSite(CacheRpcCommand command) throws Exception {
      final RpcManager rpcManager = cache.getRpcManager();
      Map<Address, Response> responseMap = rpcManager.blocking(
            rpcManager.invokeCommandOnAll(command, MapResponseCollector.validOnly(), rpcManager.getSyncRpcOptions()));
      responseMap.put(rpcManager.getAddress(), LocalInvocation.newInstanceFromCache(cache, command).call());
      return responseMap;
   }

   private static class StatePushTask {
      private final List<XSiteState> chunk;
      private final Address address;
      private final AdvancedCache<?, ?> cache;
      private volatile CompletableFuture<? extends Response> remoteFuture;


      private StatePushTask(List<XSiteState> chunk, Address address, AdvancedCache<?, ?> cache) {
         this.chunk = chunk;
         this.address = address;
         this.cache = cache;
      }

      public void executeRemote() {
         final RpcManager rpcManager = cache.getRpcManager();
         remoteFuture = rpcManager.invokeCommand(address, newStatePushCommand(cache, chunk),
                                                 SingleResponseCollector.validOnly(), rpcManager.getSyncRpcOptions()).toCompletableFuture();
      }

      public void executeLocal() {
         try {
            final Response response = LocalInvocation.newInstanceFromCache(cache, newStatePushCommand(cache, chunk)).call();
            this.remoteFuture = CompletableFuture.completedFuture(response);
         } catch (final Exception e) {
            this.remoteFuture = CompletableFutures.completedExceptionFuture(new ExecutionException(e));
         }
      }

      public Response awaitResponse() throws Exception {
         return remoteFuture.get();
      }

   }
}
