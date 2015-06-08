package org.infinispan.xsite;

import org.infinispan.Cache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.concurrent.AbstractInProcessFuture;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.LocalInvocation;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.statetransfer.XSiteState;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
import org.infinispan.xsite.statetransfer.XSiteStateTransferControlCommand;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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

   private static boolean awaitRemoteTask(Cache<?, ?> cache, StatePushTask task) throws Exception {
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
            if (!cache.getStatus().allowInvocations()) {
               throw new CacheException("Cache is stopping or terminated: " + cache.getStatus());
            }
            task.executeLocal();
         }
      } catch (Exception e) {
         if (!cache.getStatus().allowInvocations()) {
            throw new CacheException("Cache is stopping or terminated: " + cache.getStatus());
         }
         if (cache.getAdvancedCache().getRpcManager().getMembers().contains(task.address) &&
               !cache.getAdvancedCache().getRpcManager().getAddress().equals(task.address)) {
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

   @Override
   public void handleStateTransferControl(XSiteStateTransferControlCommand command) throws Exception {
      XSiteStateTransferControlCommand invokeCommand = command;
      if (!command.getCacheName().equals(cache.getName())) {
         //copy if the cache name is different
         invokeCommand = command.copyForCache(cache.getName());
      }
      invokeCommand.setSiteName(command.getOriginSite());
      invokeRemotelyInLocalSite(invokeCommand);
   }

   @Override
   public void handleStateTransferState(XSiteStatePushCommand cmd) throws Exception {
      //split the state and forward it to the primary owners...
      if (!cache.getStatus().allowInvocations()) {
         throw new CacheException("Cache is stopping or terminated: " + cache.getStatus());
      }
      final long endTime = timeService.expectedEndTime(cmd.getTimeout(), TimeUnit.MILLISECONDS);
      final ClusteringDependentLogic clusteringDependentLogic = cache.getAdvancedCache().getComponentRegistry()
            .getComponent(ClusteringDependentLogic.class);
      final Map<Address, List<XSiteState>> primaryOwnersChunks = new HashMap<>();
      final Address localAddress = clusteringDependentLogic.getAddress();

      if (trace) {
         log.tracef("Received X-Site state transfer '%s'. Splitting by primary owner.", cmd);
      }

      for (XSiteState state : cmd.getChunk()) {
         final Address primaryOwner = clusteringDependentLogic.getPrimaryOwner(state.key());
         List<XSiteState> primaryOwnerList = primaryOwnersChunks.get(primaryOwner);
         if (primaryOwnerList == null) {
            primaryOwnerList = new LinkedList<>();
            primaryOwnersChunks.put(primaryOwner, primaryOwnerList);
         }
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
            if (awaitRemoteTask(cache, iterator.next())) {
               iterator.remove();
            }
         }
      }
      //the put operation can fail silently. check in the end and it is better to resend the chunk than to lose keys.
      if (!cache.getStatus().allowInvocations()) {
         throw new CacheException("Cache is stopping or terminated: " + cache.getStatus());
      }
      if (!tasks.isEmpty()) {
         throw new TimeoutException("Unable to apply state in the time limit.");
      }
   }

   private Map<Address, Response> invokeRemotelyInLocalSite(CacheRpcCommand command) throws Exception {
      final RpcManager rpcManager = cache.getAdvancedCache().getRpcManager();
      CompletableFuture<Map<Address, Response>> remoteFuture = rpcManager
            .invokeRemotelyAsync(null, command, rpcManager.getDefaultRpcOptions(true, DeliverOrder.NONE));
      final Map<Address, Response> responseMap = new HashMap<>();
      responseMap.put(rpcManager.getAddress(), LocalInvocation.newInstanceFromCache(cache, command).call());
      responseMap.putAll(remoteFuture.get());
      return responseMap;
   }

   private static class StatePushTask {
      private final List<XSiteState> chunk;
      private final Address address;
      private final Cache<?, ?> cache;
      private volatile Future<Map<Address, Response>> remoteFuture;


      private StatePushTask(List<XSiteState> chunk, Address address, Cache<?, ?> cache) {
         this.chunk = chunk;
         this.address = address;
         this.cache = cache;
      }

      public void executeRemote() {
         final RpcManager rpcManager = cache.getAdvancedCache().getRpcManager();
         remoteFuture = rpcManager.invokeRemotelyAsync(Collections.singletonList(address),
               newStatePushCommand(cache, chunk), rpcManager.getDefaultRpcOptions(true));
      }

      public void executeLocal() {
         try {
            final Response response = LocalInvocation.newInstanceFromCache(cache, newStatePushCommand(cache, chunk)).call();
            this.remoteFuture = new AbstractInProcessFuture<Map<Address, Response>>() {
               @Override
               public Map<Address, Response> get() throws InterruptedException, ExecutionException {
                  return Collections.singletonMap(address, response);
               }
            };
         } catch (final Exception e) {
            this.remoteFuture = new AbstractInProcessFuture<Map<Address, Response>>() {
               @Override
               public Map<Address, Response> get() throws InterruptedException, ExecutionException {
                  throw new ExecutionException(e);
               }
            };
         }
      }

      public Response awaitResponse() throws Exception {
         Future<Map<Address, Response>> future = remoteFuture;
         if (future == null) {
            throw new NullPointerException("Should not happen!");
         }
         Map<Address, Response> responseMap = future.get();
         if (responseMap.size() != 1 || !responseMap.containsKey(address)) {
            throw new IllegalStateException("Shouldn't happen. Map is " + responseMap);
         }
         return responseMap.values().iterator().next();
      }

   }
}
