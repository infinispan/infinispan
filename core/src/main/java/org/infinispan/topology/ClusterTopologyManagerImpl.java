package org.infinispan.topology;


import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.executors.SemaphoreCompletionService;
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
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.impl.AvailabilityStrategy;
import org.infinispan.partitionhandling.impl.PreferAvailabilityStrategy;
import org.infinispan.partitionhandling.impl.PreferConsistencyStrategy;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.EventLogger;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;
import static org.infinispan.util.logging.LogFactory.CLUSTER;
import static org.infinispan.util.logging.events.Messages.MESSAGES;

/**
 * The {@code ClusterTopologyManager} implementation.
 *
 * @author Dan Berindei
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ClusterTopologyManagerImpl implements ClusterTopologyManager {
   private enum ClusterManagerStatus {
      INITIALIZING,
      REGULAR_MEMBER,
      COORDINATOR,
      RECOVERING_CLUSTER,
      STOPPING;

      boolean isRunning() {
         return this != STOPPING;
      }

      boolean isCoordinator() {
         return this == COORDINATOR || this == RECOVERING_CLUSTER;
      }
   }

   private static final Log log = LogFactory.getLog(ClusterTopologyManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   private Transport transport;
   private GlobalConfiguration globalConfiguration;
   private GlobalComponentRegistry gcr;
   private CacheManagerNotifier cacheManagerNotifier;
   private EmbeddedCacheManager cacheManager;
   private TimeService timeService;
   private ExecutorService asyncTransportExecutor;
   private SemaphoreCompletionService<Void> viewHandlingCompletionService;
   private EventLogger eventLog;

   // These need to be volatile because they are sometimes read without holding the view handling lock.
   private volatile int viewId = -1;
   private volatile ClusterManagerStatus clusterManagerStatus = ClusterManagerStatus.INITIALIZING;
   private final Object clusterManagerLock = new Object();


   private final ConcurrentMap<String, ClusterCacheStatus> cacheStatusMap = CollectionFactory.makeConcurrentMap();
   private ClusterViewListener viewListener;

   // The global rebalancing status
   private volatile boolean globalRebalancingEnabled = true;

   @Inject
   public void inject(Transport transport,
                      @ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService asyncTransportExecutor,
                      GlobalConfiguration globalConfiguration, GlobalComponentRegistry gcr,
                      CacheManagerNotifier cacheManagerNotifier, EmbeddedCacheManager cacheManager,
                      TimeService timeService) {
      this.transport = transport;
      this.asyncTransportExecutor = asyncTransportExecutor;
      this.globalConfiguration = globalConfiguration;
      this.gcr = gcr;
      this.cacheManagerNotifier = cacheManagerNotifier;
      this.cacheManager = cacheManager;
      this.timeService = timeService;
   }

   @Start(priority = 100)
   public void start() {
      viewHandlingCompletionService = new SemaphoreCompletionService<>(asyncTransportExecutor, 1);
      eventLog = EventLogManager.getEventLogger(cacheManager);

      viewListener = new ClusterViewListener();
      cacheManagerNotifier.addListener(viewListener);
      // The listener already missed the initial view
      viewHandlingCompletionService.submit(() -> {
         handleClusterView(false, transport.getViewId());
         return null;
      });

      fetchRebalancingStatusFromCoordinator();
   }

   protected void fetchRebalancingStatusFromCoordinator() {
      if (!transport.isCoordinator()) {
         ReplicableCommand command = new CacheTopologyControlCommand(null,
               CacheTopologyControlCommand.Type.POLICY_GET_STATUS, transport.getAddress(), -1);
         Address coordinator = transport.getCoordinator();
         try {
            Map<Address, Response> responseMap = transport.invokeRemotely(Collections.singleton(coordinator),
                  command, ResponseMode.SYNCHRONOUS, getGlobalTimeout(), null, DeliverOrder.NONE, false);
            Response response = responseMap.get(coordinator);
            if (response instanceof SuccessfulResponse) {
               globalRebalancingEnabled = ((Boolean) ((SuccessfulResponse) response).getResponseValue());
            } else {
               log.errorReadingRebalancingStatus(coordinator, null);
            }
         } catch (Exception e) {
            log.errorReadingRebalancingStatus(coordinator, e);
         }
      }
   }

   @Stop(priority = 100)
   public void stop() {
      // Stop blocking cache topology commands.
      synchronized (clusterManagerLock) {
         clusterManagerStatus = ClusterManagerStatus.STOPPING;
         clusterManagerLock.notifyAll();
      }

      cacheManagerNotifier.removeListener(viewListener);
   }

   @Override
   public CacheStatusResponse handleJoin(String cacheName, Address joiner, CacheJoinInfo joinInfo,
         int joinerViewId) throws Exception {
      ClusterCacheStatus cacheStatus;
      synchronized (clusterManagerLock) {
         waitForView(joinerViewId, joinInfo.getTimeout());

         if (!clusterManagerStatus.isRunning()) {
            log.debugf("Ignoring join request from %s for cache %s, the local cache manager is shutting down",
                  joiner, cacheName);
            return null;
         }
         if (joinerViewId < viewId) {
            log.debugf("Ignoring join request from %s for cache %s, joiner's view id is too old: %d", joiner,
                  cacheName, joinerViewId);
            return null;
         }

         cacheStatus = initCacheStatusIfAbsent(cacheName);
      }
      return cacheStatus.doJoin(joiner, joinInfo);
   }

   @Override
   public void handleLeave(String cacheName, Address leaver, int viewId) throws Exception {
      if (!clusterManagerStatus.isRunning()) {
         log.debugf("Ignoring leave request from %s for cache %s, the local cache manager is shutting down",
               leaver, cacheName);
         return;
      }

      ClusterCacheStatus cacheStatus = cacheStatusMap.get(cacheName);
      if (cacheStatus == null) {
         // This can happen if we've just become coordinator
         log.tracef("Ignoring leave request from %s for cache %s because it doesn't have a cache status entry", leaver, cacheName);
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
      eventLog.context(cacheName).scope(node.toString()).info(EventLogCategory.CLUSTER, MESSAGES.rebalanceCompleted());


      ClusterCacheStatus cacheStatus = cacheStatusMap.get(cacheName);
      if (cacheStatus == null || !cacheStatus.isRebalanceInProgress()) {
         log.debugf("Ignoring rebalance confirmation from %s " +
               "for cache %s because it doesn't have a cache status entry", node, cacheName);
         return;
      }

      cacheStatus.doConfirmRebalance(node, topologyId);
   }

   private static class CacheTopologyFilterReuser implements ResponseFilter {
      Map<CacheTopology, CacheTopology> seenTopologies = new HashMap<>();
      Map<CacheJoinInfo, CacheJoinInfo> seenInfos = new HashMap<>();

      @Override
      public boolean isAcceptable(Response response, Address sender) {
         if (response.isSuccessful() && response.isValid()) {
            ManagerStatusResponse value = (ManagerStatusResponse) ((SuccessfulResponse)response).getResponseValue();
            for (Entry<String, CacheStatusResponse> entry : value.getCaches().entrySet()) {
               CacheStatusResponse csr = entry.getValue();
               CacheTopology cacheTopology = csr.getCacheTopology();
               CacheTopology stableTopology = csr.getStableTopology();

               CacheTopology replaceCacheTopology = seenTopologies.get(cacheTopology);
               if (replaceCacheTopology == null) {
                  seenTopologies.put(cacheTopology, cacheTopology);
                  replaceCacheTopology = cacheTopology;
               }

               CacheTopology replaceStableTopology = null;
               // If the don't equal check if we replace - note stableTopology can be null
               if (!cacheTopology.equals(stableTopology)) {
                  replaceStableTopology = seenTopologies.get(stableTopology);
                  if (replaceStableTopology == null) {
                     seenTopologies.put(stableTopology, stableTopology);
                  }

               } else {
                  // Since they were equal replace it with the cache topology we are going to use
                  replaceStableTopology = replaceCacheTopology != null ? replaceCacheTopology : cacheTopology;
               }

               CacheJoinInfo info = csr.getCacheJoinInfo();
               CacheJoinInfo replaceInfo = seenInfos.get(info);
               if (replaceInfo == null) {
                  seenInfos.put(info, info);
               }

               if (replaceCacheTopology != null || replaceStableTopology != null || replaceInfo != null) {
                  entry.setValue(new CacheStatusResponse(replaceInfo != null ? replaceInfo : info,
                        replaceCacheTopology != null ? replaceCacheTopology : cacheTopology,
                        replaceStableTopology != null ? replaceStableTopology : stableTopology, csr.getAvailabilityMode()));
               }
            }
         }
         return true;
      }

      @Override
      public boolean needMoreResponses() {
         return true;
      }
   }

   @Override
   public void handleClusterView(boolean mergeView, int newViewId) {
      synchronized (clusterManagerLock) {
         if (newViewId < transport.getViewId()) {
            log.tracef("Ignoring old cluster view notification: %s", newViewId);
            return;
         }

         boolean isCoordinator = transport.isCoordinator();
         boolean becameCoordinator = isCoordinator && !clusterManagerStatus.isCoordinator();
         if (trace) {
            log.tracef("Received new cluster view: %d, isCoordinator = %s, old status = %s", (Object) newViewId,
                  isCoordinator, clusterManagerStatus);
         }

         if (!isCoordinator) {
            clusterManagerStatus = ClusterManagerStatus.REGULAR_MEMBER;
            return;
         }
         if (becameCoordinator || mergeView) {
            clusterManagerStatus = ClusterManagerStatus.RECOVERING_CLUSTER;
         }

         // notify threads that might be waiting to join
         viewId = newViewId;
         clusterManagerLock.notifyAll();
      }

      // The SemaphoreCompletionService acts as a critical section, so we don't need to worry about
      // multiple threads.
      if (clusterManagerStatus == ClusterManagerStatus.RECOVERING_CLUSTER) {
         // Clean up leftover cache status information from the last time we were coordinator.
         // E.g. if the local node was coordinator, started a rebalance, and then lost coordinator
         // status because of a merge, the existing cache statuses may have a rebalance in progress.
         cacheStatusMap.clear();
         try {
            recoverClusterStatus(newViewId, mergeView, transport.getMembers());

            synchronized (clusterManagerLock) {
               if (viewId != newViewId) {
                  log.debugf("View updated while we were recovering the cluster for view %d", newViewId);
                  return;
               }
               clusterManagerStatus = ClusterManagerStatus.COORDINATOR;
               // notify threads that might be waiting to join
               clusterManagerLock.notifyAll();
            }
         } catch (InterruptedException e) {
            if (trace)
               log.tracef("Cluster state recovery interrupted because the coordinator is shutting down");
         } catch (SuspectException e) {
            if (trace)
               log.tracef("Cluster state recovery interrupted because a member was lost. Will retry.");
         } catch (Exception e) {
            if (clusterManagerStatus.isRunning()) {
               log.failedToRecoverClusterState(e);
            } else {
               log.tracef("Cluster state recovery failed because the coordinator is shutting down");
            }
         }
      }

      if (clusterManagerStatus == ClusterManagerStatus.COORDINATOR) {
         // If we have recovered the cluster status, we rebalance the caches to include minor partitions
         // If we processed a regular view, we prune members that left.
         try {
            updateCacheMembers(transport.getMembers());
         } catch (Exception e) {
            log.errorUpdatingMembersList(e);
         }
      }
   }

   private ClusterCacheStatus initCacheStatusIfAbsent(String cacheName) {
      return cacheStatusMap.computeIfAbsent(cacheName, (name) -> {
         // We assume that any cache with partition handling configured is already defined on all the nodes
         // (including the coordinator) before it starts on any node.
         Configuration cacheConfiguration = cacheManager.getCacheConfiguration(cacheName);
         AvailabilityStrategy availabilityStrategy;
         if (cacheConfiguration != null && cacheConfiguration.clustering().partitionHandling().enabled()) {
            availabilityStrategy = new PreferConsistencyStrategy(eventLog);
         } else {
            availabilityStrategy = new PreferAvailabilityStrategy(eventLog);
         }
         return new ClusterCacheStatus(cacheName, availabilityStrategy, this, transport);
      });
   }

   @Override
   public void broadcastRebalanceStart(String cacheName, CacheTopology cacheTopology, boolean totalOrder, boolean distributed) {
      CLUSTER.startRebalance(cacheName, cacheTopology);
      eventLog.context(cacheName).info(EventLogCategory.CLUSTER, MESSAGES.rebalanceStarted());
      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
            CacheTopologyControlCommand.Type.REBALANCE_START, transport.getAddress(), cacheTopology, null,
            viewId);
      executeOnClusterAsync(command, getGlobalTimeout(), totalOrder, distributed);
   }

   private void recoverClusterStatus(int newViewId, final boolean isMergeView, final List<Address> clusterMembers) throws Exception {
      log.debugf("Recovering cluster status for view %d", newViewId);
      ReplicableCommand command = new CacheTopologyControlCommand(null,
            CacheTopologyControlCommand.Type.GET_STATUS, transport.getAddress(), newViewId);
      Map<Address, Object> statusResponses = executeOnClusterSync(command, getGlobalTimeout(), false, false,
            new CacheTopologyFilterReuser());

      log.debugf("Got %d status responses. members are %s", statusResponses.size(), clusterMembers);
      Map<String, Map<Address, CacheStatusResponse>> responsesByCache = new HashMap<>();
      boolean recoveredRebalancingStatus = true;
      for (Map.Entry<Address, Object> responseEntry : statusResponses.entrySet()) {
         Address sender = responseEntry.getKey();
         ManagerStatusResponse nodeStatus = (ManagerStatusResponse) responseEntry.getValue();
         recoveredRebalancingStatus &= nodeStatus.isRebalancingEnabled();
         for (Map.Entry<String, CacheStatusResponse> statusEntry : nodeStatus.getCaches().entrySet()) {
            String cacheName = statusEntry.getKey();
            Map<Address, CacheStatusResponse> cacheResponses = responsesByCache.get(cacheName);
            if (cacheResponses == null) {
               cacheResponses = new HashMap<>();
               responsesByCache.put(cacheName, cacheResponses);
            }
            cacheResponses.put(sender, statusEntry.getValue());
         }
      }

      globalRebalancingEnabled = recoveredRebalancingStatus;
      // Compute the new consistent hashes on separate threads
      int maxThreads = Runtime.getRuntime().availableProcessors() / 2 + 1;
      CompletionService<Void> cs = new SemaphoreCompletionService<>(asyncTransportExecutor, maxThreads);
      for (final Map.Entry<String, Map<Address, CacheStatusResponse>> e : responsesByCache.entrySet()) {
         final ClusterCacheStatus cacheStatus = initCacheStatusIfAbsent(e.getKey());
         cs.submit(() -> {
            cacheStatus.doMergePartitions(e.getValue(), clusterMembers, isMergeView);
            return null;
         });
      }
      for (int i = 0; i < responsesByCache.size(); i++) {
         cs.take();
      }
   }

   public void updateCacheMembers(List<Address> newClusterMembers) throws Exception {
      log.tracef("Updating cluster members for all the caches. New list is %s", newClusterMembers);
      try {
         // If we get a SuspectException here, it means we will have a new view soon and we can ignore this one.
         confirmMembersAvailable();
      } catch (SuspectException e) {
         log.tracef("Node %s left while updating cache members", e.getSuspect());
         return;
      }

      for (ClusterCacheStatus cacheStatus : cacheStatusMap.values()) {
         cacheStatus.doHandleClusterView();
      }
   }

   private void confirmMembersAvailable() throws Exception {
      ReplicableCommand heartbeatCommand = new CacheTopologyControlCommand(null, CacheTopologyControlCommand.Type.POLICY_GET_STATUS, transport.getAddress(), -1);
      transport.invokeRemotely(null, heartbeatCommand, ResponseMode.SYNCHRONOUS, getGlobalTimeout(), null, DeliverOrder.NONE, false);
   }

   /**
    * Wait until we have received view {@code joinerViewId} and we have finished recovering the cluster state.
    * <p>
    * Returns early if the node is shutting down.
    *
    * @throws TimeoutException if the timeout expired.
    */
   private void waitForView(int joinerViewId, long timeout) throws InterruptedException {
      if (joinerViewId > viewId) {
         log.tracef("Received a cache topology command with a higher view id: %s, our view id is %s",
               joinerViewId, viewId);
         long endTime = timeService.expectedEndTime(timeout, TimeUnit.MILLISECONDS);
         synchronized (clusterManagerLock) {
            while (viewId < joinerViewId || clusterManagerStatus == ClusterManagerStatus.RECOVERING_CLUSTER) {
               if (timeService.isTimeExpired(endTime) || !clusterManagerStatus.isRunning())
                  break;
               clusterManagerLock.wait(timeService.remainingTime(endTime, TimeUnit.MILLISECONDS));
            }
         }
         if (timeService.isTimeExpired(endTime)) {
            throw new TimeoutException("Timed out waiting for view " + joinerViewId);
         }
      }
   }

   private Map<Address, Object> executeOnClusterSync(final ReplicableCommand command, final int timeout,
                                                     boolean totalOrder, boolean distributed, final ResponseFilter filter)
         throws Exception {
      // first invoke remotely

      if (totalOrder) {
         Map<Address, Response> responseMap = transport.invokeRemotely(transport.getMembers(), command,
                                                                       ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS,
                                                                       timeout, filter, DeliverOrder.TOTAL, distributed);
         return extractResponseValues(responseMap, null);
      }

      CompletableFuture<Map<Address, Response>> remoteFuture = transport.invokeRemotelyAsync(null, command,
            ResponseMode.SYNCHRONOUS, timeout, filter, DeliverOrder.NONE, false);

      // invoke the command on the local node
      gcr.wireDependencies(command);
      Response localResponse;
      try {
         if (trace) log.tracef("Attempting to execute command on self: %s", command);
         localResponse = (Response) command.perform(null);
      } catch (Throwable throwable) {
         throw new Exception(throwable);
      }

      return extractResponseValues(remoteFuture.get(timeout, TimeUnit.MILLISECONDS), localResponse);
   }

   private int getGlobalTimeout() {
      // TODO Rename setting to something like globalRpcTimeout
      return (int) globalConfiguration.transport().distributedSyncTimeout();
   }

   private void executeOnClusterAsync(final ReplicableCommand command, final int timeout, boolean totalOrder, boolean distributed) {
      if (!totalOrder) {
         // invoke the command on the local node
         asyncTransportExecutor.submit((Runnable) () -> {
            try {
               if (trace) log.tracef("Attempting to execute command on self: %s", command);
               gcr.wireDependencies(command);
               command.perform(null);
            } catch (Throwable throwable) {
               // The command already logs any exception in perform()
            }
         });
      }

      // invoke remotely
      try {
         DeliverOrder deliverOrder = totalOrder ? DeliverOrder.TOTAL : DeliverOrder.NONE;
         transport.invokeRemotely(null, command, ResponseMode.ASYNCHRONOUS, timeout, null,
                                  deliverOrder, distributed);
      } catch (Exception e) {
         throw new CacheException("Failed to broadcast asynchronous command: " + command, e);
      }
   }

   @Override
   public void broadcastTopologyUpdate(String cacheName, CacheTopology cacheTopology, AvailabilityMode availabilityMode, boolean totalOrder, boolean distributed) {
      log.debugf("Updating cluster-wide current topology for cache %s, topology = %s, availability mode = %s",
            cacheName, cacheTopology, availabilityMode);
      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
            CacheTopologyControlCommand.Type.CH_UPDATE, transport.getAddress(), cacheTopology,
            availabilityMode, viewId);
      executeOnClusterAsync(command, getGlobalTimeout(), totalOrder, distributed);
   }

   @Override
   public void broadcastStableTopologyUpdate(String cacheName, CacheTopology cacheTopology, boolean totalOrder, boolean distributed) {
      log.debugf("Updating cluster-wide stable topology for cache %s, topology = %s", cacheName, cacheTopology);
      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
            CacheTopologyControlCommand.Type.STABLE_TOPOLOGY_UPDATE, transport.getAddress(), cacheTopology,
            null, viewId);
      executeOnClusterAsync(command, getGlobalTimeout(), totalOrder, distributed);
   }

   @Override
   public boolean isRebalancingEnabled() {
      return globalRebalancingEnabled;
   }

   @Override
   public boolean isRebalancingEnabled(String cacheName) {
      if (cacheName ==null) {
         return isRebalancingEnabled();
      } else {
         return cacheStatusMap.get(cacheName).isRebalanceEnabled();
      }
   }

   @Override
   public void setRebalancingEnabled(String cacheName, boolean enabled) {
      if (cacheName == null) {
         setRebalancingEnabled(enabled);
      } else {
         ClusterCacheStatus clusterCacheStatus = cacheStatusMap.get(cacheName);
         if (clusterCacheStatus != null)
            clusterCacheStatus.setRebalanceEnabled(enabled);
      }
   }

   @Override
   public void setRebalancingEnabled(boolean enabled) {
      if (enabled) {
         if (!globalRebalancingEnabled) {
            CLUSTER.rebalancingEnabled();
         }
      } else {
         if (globalRebalancingEnabled) {
            CLUSTER.rebalancingSuspended();
         }
      }
      globalRebalancingEnabled = enabled;
      cacheStatusMap.values().forEach(ClusterCacheStatus::startQueuedRebalance);
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

   @Override
   public RebalancingStatus getRebalancingStatus(String cacheName) {
      ClusterCacheStatus cacheStatus = cacheStatusMap.get(cacheName);
      if (cacheStatus != null) {
         return cacheStatus.getRebalancingStatus();
      } else {
         return RebalancingStatus.PENDING;
      }
   }

   @Listener(sync = true)
   public class ClusterViewListener {
      @Merged
      @ViewChanged
      public void handleViewChange(final ViewChangedEvent e) {
         // Need to recover existing caches asynchronously (in case we just became the coordinator).
         // Cannot use the async notification thread pool, by default it only has 1 thread.
         viewHandlingCompletionService.submit(() -> {
            handleClusterView(e.isMergeView(), e.getViewId());
            return null;
         });
      }
   }

   private Map<Address, Object> extractResponseValues(Map<Address, Response> remoteResponses, Response localResponse) {
      // parse the responses
      Map<Address, Object> responseValues = new HashMap<>(transport.getMembers().size());
      for (Map.Entry<Address, Response> entry : remoteResponses.entrySet()) {
         addResponseValue(entry.getKey(), entry.getValue(), responseValues);
      }

      if (localResponse != null) {
         addResponseValue(transport.getAddress(), localResponse, responseValues);
      }
      return responseValues;
   }

   private static void addResponseValue(Address origin, Response response, Map<Address, Object> values) {
      if (!response.isSuccessful()) {
         Throwable cause = response instanceof ExceptionResponse ? ((ExceptionResponse) response).getException() : null;
         throw new CacheException(format("Unsuccessful response received from node '%s': %s", origin, response), cause);
      } else if (response == CacheNotFoundResponse.INSTANCE) {
         return;
      }
      values.put(origin, ((SuccessfulResponse) response).getResponseValue());
   }

}
