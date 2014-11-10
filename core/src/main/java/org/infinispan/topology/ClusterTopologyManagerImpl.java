package org.infinispan.topology;


import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.partionhandling.AvailabilityMode;
import org.infinispan.partionhandling.impl.AvailabilityStrategy;
import org.infinispan.partionhandling.impl.PreferAvailabilityStrategy;
import org.infinispan.partionhandling.impl.PreferConsistencyStrategy;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.infinispan.util.logging.LogFactory.CLUSTER;
import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;

/**
 * The {@code ClusterTopologyManager} implementation.
 *
 * @author Dan Berindei
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ClusterTopologyManagerImpl implements ClusterTopologyManager {
   private static final Log log = LogFactory.getLog(ClusterTopologyManagerImpl.class);

   private Transport transport;
   private GlobalConfiguration globalConfiguration;
   private GlobalComponentRegistry gcr;
   private CacheManagerNotifier cacheManagerNotifier;
   private EmbeddedCacheManager cacheManager;
   private ExecutorService asyncTransportExecutor;


   private volatile boolean isCoordinator;
   private volatile boolean isShuttingDown;
   private volatile int viewId = -1;
   private final Object viewUpdateLock = new Object();
   private final Object viewHandlingLock = new Object();


   private final ConcurrentMap<String, ClusterCacheStatus> cacheStatusMap = CollectionFactory.makeConcurrentMap();
   private ClusterViewListener viewListener;

   private volatile boolean isRebalancingEnabled = true;

   @Inject
   public void inject(Transport transport,
                      @ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService asyncTransportExecutor,
                      GlobalConfiguration globalConfiguration, GlobalComponentRegistry gcr,
                      CacheManagerNotifier cacheManagerNotifier, EmbeddedCacheManager cacheManager) {
      this.transport = transport;
      this.asyncTransportExecutor = asyncTransportExecutor;
      this.globalConfiguration = globalConfiguration;
      this.gcr = gcr;
      this.cacheManagerNotifier = cacheManagerNotifier;
      this.cacheManager = cacheManager;
   }

   @Start(priority = 100)
   public void start() {
      isShuttingDown = false;
      isCoordinator = transport.isCoordinator();

      viewListener = new ClusterViewListener();
      cacheManagerNotifier.addListener(viewListener);
      // The listener already missed the initial view
      handleClusterView(false, transport.getViewId());
   }

   @Stop(priority = 100)
   public void stop() {
      isShuttingDown = true;
      cacheManagerNotifier.removeListener(viewListener);

      // Stop blocking cache topology commands.
      // The synchronization also ensures that the listener has finished executing
      // so we don't get InterruptedExceptions when the notification thread pool shuts down
      synchronized (viewUpdateLock) {
         viewId = Integer.MAX_VALUE;
         viewUpdateLock.notifyAll();
      }
   }

   @Override
   public CacheStatusResponse handleJoin(String cacheName, Address joiner, CacheJoinInfo joinInfo, int viewId) throws Exception {
      waitForView(viewId);
      if (isShuttingDown) {
         log.debugf("Ignoring join request from %s for cache %s, the local cache manager is shutting down",
               joiner, cacheName);
         return null;
      }

      ClusterCacheStatus cacheStatus = initCacheStatusIfAbsent(cacheName);
      return cacheStatus.doJoin(joiner, joinInfo);
   }

   @Override
   public void handleLeave(String cacheName, Address leaver, int viewId) throws Exception {
      if (isShuttingDown) {
         log.debugf("Ignoring leave request from %s for cache %s, the local cache manager is shutting down",
               leaver, cacheName);
         return;
      }

      ClusterCacheStatus cacheStatus = cacheStatusMap.get(cacheName);
      if (cacheStatus == null) {
         // This can happen if we've just become coordinator
         log.tracef("Ignoring leave request from %s for cache %s because it doesn't have a cache status entry");
         return;
      }
      cacheStatus.doLeave(leaver);
   }

   @Override
   public void handleRebalanceCompleted(String cacheName, Address node, int topologyId, Throwable throwable, int viewId) throws Exception {
      if (throwable != null) {
         // TODO We could try to update the pending CH such that nodes reporting errors are not considered to hold any state
         // For now we are just logging the error and proceeding as if the rebalance was successful everywhere
         log.rebalanceError(cacheName, node, throwable);
      }

      CLUSTER.rebalanceCompleted(cacheName, node, topologyId);

      ClusterCacheStatus cacheStatus = cacheStatusMap.get(cacheName);
      if (cacheStatus == null || !cacheStatus.isRebalanceInProgress()) {
         log.debugf("Ignoring rebalance confirmation from %s " +
               "for cache %s because it doesn't have a cache status entry", node, cacheName);
         return;
      }

      cacheStatus.doConfirmRebalance(node, topologyId);
   }

   @Override
   public void handleClusterView(boolean mergeView, int newViewId) {
      synchronized (viewHandlingLock) {
         // check to ensure this is not an older view
         if (newViewId <= viewId) {
            log.tracef("Ignoring old cluster view notification: %s", newViewId);
            return;
         }

         boolean becameCoordinator = !isCoordinator && transport.isCoordinator();
         isCoordinator = transport.isCoordinator();
         log.tracef("Received new cluster view: %s, isCoordinator = %s, becameCoordinator = %s", newViewId,
               isCoordinator, becameCoordinator);
         if (!isCoordinator)
            return;

         if (mergeView || becameCoordinator) {
            try {
               recoverClusterStatus(newViewId, mergeView, transport.getMembers());
            } catch (InterruptedException e) {
               log.tracef("Cluster state recovery interrupted because the coordinator is shutting down");
               // the CTMI has already stopped, no need to update the view id or notify waiters
               return;
            } catch (Exception e) {
               // TODO Retry?
               log.failedToRecoverClusterState(e);
            }
         } else {
            try {
               updateCacheMembers(transport.getMembers());
            } catch (Exception e) {
               log.errorUpdatingMembersList(e);
            }
         }

         // update the view id last, so join requests from other nodes wait until we recovered existing members' info
         synchronized (viewUpdateLock) {
            viewId = newViewId;
            viewUpdateLock.notifyAll();
         }
      }
   }

   private ClusterCacheStatus initCacheStatusIfAbsent(String cacheName) {
      ClusterCacheStatus cacheStatus = cacheStatusMap.get(cacheName);
      if (cacheStatus == null) {
         // We assume that any cache with partition handling configured is already defined on all the nodes
         // (including the coordinator) before it starts on any node.
         Configuration cacheConfiguration = cacheManager.getCacheConfiguration(cacheName);
         AvailabilityStrategy availabilityStrategy;
         if (cacheConfiguration != null && cacheConfiguration.clustering().partitionHandling().enabled()) {
            availabilityStrategy = new PreferConsistencyStrategy();
         } else {
            availabilityStrategy = new PreferAvailabilityStrategy();
         }
         ClusterCacheStatus newCacheStatus = new ClusterCacheStatus(cacheName, availabilityStrategy, this
         );
         cacheStatus = cacheStatusMap.putIfAbsent(cacheName, newCacheStatus);
         if (cacheStatus == null) {
            cacheStatus = newCacheStatus;
         }
      }
      return cacheStatus;
   }

   @Override
   public void broadcastRebalanceStart(String cacheName, CacheTopology cacheTopology, boolean totalOrder, boolean distributed) {
      CLUSTER.startRebalance(cacheName, cacheTopology);
      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
            CacheTopologyControlCommand.Type.REBALANCE_START, transport.getAddress(), cacheTopology, null,
            transport.getViewId());
      executeOnClusterAsync(command, getGlobalTimeout(), totalOrder, distributed);
   }

   private void recoverClusterStatus(int newViewId, boolean isMergeView, List<Address> clusterMembers) throws Exception {
      ReplicableCommand command = new CacheTopologyControlCommand(null,
            CacheTopologyControlCommand.Type.GET_STATUS, transport.getAddress(), newViewId);
      Map<Address, Object> statusResponses = executeOnClusterSync(command, getGlobalTimeout(), false, false);

      log.debugf("Got %d status responses. members are %s", statusResponses.size(), clusterMembers);
      Map<String, Map<Address, CacheStatusResponse>> responsesByCache = new HashMap<>();
      for (Map.Entry<Address, Object> responseEntry : statusResponses.entrySet()) {
         Address sender = responseEntry.getKey();
         Map<String, CacheStatusResponse> nodeStatus = (Map<String, CacheStatusResponse>) responseEntry.getValue();
         for (Map.Entry<String, CacheStatusResponse> statusEntry : nodeStatus.entrySet()) {
            String cacheName = statusEntry.getKey();

            Map<Address, CacheStatusResponse> cacheResponses = responsesByCache.get(cacheName);
            if (cacheResponses == null) {
               cacheResponses = new HashMap<>();
               responsesByCache.put(cacheName, cacheResponses);
            }
            cacheResponses.put(sender, statusEntry.getValue());
         }
      }

      for (Map.Entry<String, Map<Address, CacheStatusResponse>> e : responsesByCache.entrySet()) {
         ClusterCacheStatus cacheStatus = initCacheStatusIfAbsent(e.getKey());
         cacheStatus.doMergePartitions(e.getValue(), clusterMembers, isMergeView);
      }
   }

   public void updateCacheMembers(List<Address> newClusterMembers) throws Exception {
      log.tracef("Updating cluster members for all the caches. New list is %s", newClusterMembers);

      for (ClusterCacheStatus cacheStatus : cacheStatusMap.values()) {
         cacheStatus.doHandleClusterView(newClusterMembers);
      }
   }

   private void waitForView(int viewId) throws InterruptedException {
      if (this.viewId < viewId) {
         log.tracef("Received a cache topology command with a higher view id: %s, our view id is %s", viewId, this.viewId);
      }
      synchronized (viewUpdateLock) {
         while (this.viewId < viewId) {
            // TODO break out of the loop after state transfer timeout expires
            viewUpdateLock.wait(1000);
         }
      }
   }

   private Map<Address, Object> executeOnClusterSync(final ReplicableCommand command, final int timeout,
                                                     boolean totalOrder, boolean distributed)
         throws Exception {
      // first invoke remotely

      if (totalOrder) {
         Map<Address, Response> responseMap = transport.invokeRemotely(transport.getMembers(), command,
                                                                       ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS,
                                                                       timeout, false, null, totalOrder, distributed);
         Map<Address, Object> responseValues = new HashMap<Address, Object>(transport.getMembers().size());
         for (Map.Entry<Address, Response> entry : responseMap.entrySet()) {
            Address address = entry.getKey();
            Response response = entry.getValue();
            if (!response.isSuccessful()) {
               Throwable cause = response instanceof ExceptionResponse ? ((ExceptionResponse) response).getException() : null;
               throw new CacheException("Unsuccessful response received from node " + address + ": " + response, cause);
            }
            responseValues.put(address, ((SuccessfulResponse) response).getResponseValue());
         }
         return responseValues;
      }

      Future<Map<Address, Response>> remoteFuture = asyncTransportExecutor.submit(new Callable<Map<Address, Response>>() {
         @Override
         public Map<Address, Response> call() throws Exception {
            return transport.invokeRemotely(null, command,
                  ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, timeout, true, null, false, false);
         }
      });

      // invoke the command on the local node
      gcr.wireDependencies(command);
      Response localResponse;
      try {
         if (log.isTraceEnabled()) log.tracef("Attempting to execute command on self: %s", command);
         localResponse = (Response) command.perform(null);
      } catch (Throwable throwable) {
         throw new Exception(throwable);
      }
      if (!localResponse.isSuccessful()) {
         throw new CacheException("Unsuccessful local response: " + localResponse);
      }

      // wait for the remote commands to finish
      Map<Address, Response> responseMap = remoteFuture.get(timeout, TimeUnit.MILLISECONDS);

      // parse the responses
      Map<Address, Object> responseValues = new HashMap<Address, Object>(transport.getMembers().size());
      for (Map.Entry<Address, Response> entry : responseMap.entrySet()) {
         Address address = entry.getKey();
         Response response = entry.getValue();
         if (!response.isSuccessful()) {
            Throwable cause = response instanceof ExceptionResponse ? ((ExceptionResponse) response).getException() : null;
            throw new CacheException("Unsuccessful response received from node " + address + ": " + response, cause);
         }
         responseValues.put(address, ((SuccessfulResponse) response).getResponseValue());
      }

      responseValues.put(transport.getAddress(), ((SuccessfulResponse) localResponse).getResponseValue());

      return responseValues;
   }

   private int getGlobalTimeout() {
      // TODO Rename setting to something like globalRpcTimeout
      return (int) globalConfiguration.transport().distributedSyncTimeout();
   }

   public void executeOnClusterAsync(final ReplicableCommand command, final int timeout, boolean totalOrder, boolean distributed) {
      if (!totalOrder) {
         // invoke the command on the local node
         asyncTransportExecutor.submit(new Runnable() {
            @Override
            public void run() {
               gcr.wireDependencies(command);
               try {
                  if (log.isTraceEnabled()) log.tracef("Attempting to execute command on self: %s", command);
                  command.perform(null);
               } catch (Throwable throwable) {
                  // The command already logs any exception in perform()
               }
            }
         });
      }

      // invoke remotely
      try {
         transport.invokeRemotely(null, command, ResponseMode.ASYNCHRONOUS_WITH_SYNC_MARSHALLING, timeout, true, null,
               totalOrder, distributed);
      } catch (Exception e) {
         throw new CacheException("Failed to broadcast asynchronous command: " + command);
      }
   }

   @Override
   public void broadcastTopologyUpdate(String cacheName, CacheTopology cacheTopology, AvailabilityMode availabilityMode, boolean totalOrder, boolean distributed) {
      log.debugf("Updating cluster-wide current topology for cache %s, topology = %s, availability mode = %s",
            cacheName, cacheTopology, availabilityMode);
      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
            CacheTopologyControlCommand.Type.CH_UPDATE, transport.getAddress(), cacheTopology, availabilityMode, transport.getViewId());
      executeOnClusterAsync(command, getGlobalTimeout(), totalOrder, distributed);
   }

   @Override
   public void broadcastStableTopologyUpdate(String cacheName, CacheTopology cacheTopology, boolean totalOrder, boolean distributed) {
      log.debugf("Updating cluster-wide stable topology for cache %s, topology = %s", cacheName, cacheTopology);
      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
            CacheTopologyControlCommand.Type.STABLE_TOPOLOGY_UPDATE, transport.getAddress(), cacheTopology, null, transport.getViewId());
      executeOnClusterAsync(command, getGlobalTimeout(), totalOrder, distributed);
   }

   @Override
   public boolean isRebalancingEnabled() {
      return isRebalancingEnabled;
   }

   @Override
   public void setRebalancingEnabled(boolean enabled) {
      isRebalancingEnabled = enabled;
      for (ClusterCacheStatus cacheStatus : cacheStatusMap.values()) {
         cacheStatus.setRebalanceEnabled(enabled);
      }
   }

   @Override
   public void forceRebalance(String cacheName) {
      ClusterCacheStatus cacheStatus = cacheStatusMap.get(cacheName);
      if (cacheStatus != null) {
         cacheStatus.forceRebalance();
      }
   }

   @Override
   public void forceAvailabilityMode(String cacheName, AvailabilityMode availabilityMode) {
      ClusterCacheStatus cacheStatus = cacheStatusMap.get(cacheName);
      if (cacheStatus != null) {
         cacheStatus.forceAvailabilityMode(availabilityMode);
      }
   }

   @Listener(sync = true)
   public class ClusterViewListener {
      @SuppressWarnings("unused")
      @Merged
      @ViewChanged
      public void handleViewChange(final ViewChangedEvent e) {
         // Need to recover existing caches asynchronously (in case we just became the coordinator).
         // Cannot use the async notification thread pool, by default it only has 1 thread.
         asyncTransportExecutor.submit(new Runnable() {
            @Override
            public void run() {
               handleClusterView(e.isMergeView(), e.getViewId());
            }
         });
      }
   }

}