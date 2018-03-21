package org.infinispan.topology;


import static java.lang.String.format;
import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;
import static org.infinispan.util.logging.LogFactory.CLUSTER;
import static org.infinispan.util.logging.events.Messages.MESSAGES;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.executors.LimitedExecutor;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.globalstate.GlobalStateManager;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.partitionhandling.impl.AvailabilityStrategy;
import org.infinispan.partitionhandling.impl.LostDataCheck;
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
import org.infinispan.statetransfer.RebalanceType;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.EventLogger;

import net.jcip.annotations.GuardedBy;

/**
 * The {@code ClusterTopologyManager} implementation.
 *
 * @author Dan Berindei
 * @author Pedro Ruivo
 * @since 5.2
 */
public class ClusterTopologyManagerImpl implements ClusterTopologyManager {

   public static final int INITIAL_CONNECTION_ATTEMPTS = 10;
   public static final int CLUSTER_RECOVERY_ATTEMPTS = 10;

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

   @Inject private Transport transport;
   @Inject private GlobalConfiguration globalConfiguration;
   @Inject private GlobalComponentRegistry gcr;
   @Inject private CacheManagerNotifier cacheManagerNotifier;
   @Inject private EmbeddedCacheManager cacheManager;
   @Inject @ComponentName(ASYNC_TRANSPORT_EXECUTOR)
   private ExecutorService asyncTransportExecutor;
   @Inject private EventLogManager eventLogManager;
   @Inject private PersistentUUIDManager persistentUUIDManager;

   // These need to be volatile because they are sometimes read without holding the view handling lock.
   private volatile int viewId = -1;
   private volatile ClusterManagerStatus clusterManagerStatus = ClusterManagerStatus.INITIALIZING;
   private final Lock clusterManagerLock = new ReentrantLock();
   private final Condition clusterStateChanged = clusterManagerLock.newCondition();

   private final ConcurrentMap<String, ClusterCacheStatus> cacheStatusMap = CollectionFactory.makeConcurrentMap();
   private ClusterViewListener viewListener;
   private LimitedExecutor viewHandlingExecutor;

   // The global rebalancing status
   private volatile boolean globalRebalancingEnabled = true;

   @Start(priority = 100)
   public void start() {
      viewHandlingExecutor = new LimitedExecutor("ViewHandling", asyncTransportExecutor, 1);

      viewListener = new ClusterViewListener();
      cacheManagerNotifier.addListener(viewListener);
      // The listener already missed the initial view
      viewHandlingExecutor.execute(() -> handleClusterView(false, transport.getViewId()));

      fetchRebalancingStatusFromCoordinator();
   }

   protected void fetchRebalancingStatusFromCoordinator() {
      if (!transport.isCoordinator()) {
         // Assume any timeout is because the coordinator doesn't have a CommandAwareRpcDispatcher yet
         // (possible with a JGroupsChannelLookup and shouldConnect = false), and retry.
         ReplicableCommand command = new CacheTopologyControlCommand(null,
               CacheTopologyControlCommand.Type.POLICY_GET_STATUS, transport.getAddress(), -1);
         Address coordinator = null;
         Response response = null;
         for (int i = INITIAL_CONNECTION_ATTEMPTS - 1; i >= 0; i--) {
            try {
               coordinator = transport.getCoordinator();
               Map<Address, Response> responseMap = transport
                     .invokeRemotely(Collections.singleton(coordinator), command, ResponseMode.SYNCHRONOUS,
                           getGlobalTimeout() / INITIAL_CONNECTION_ATTEMPTS, null, DeliverOrder.NONE, false);
               response = responseMap.get(coordinator);
               break;
            } catch (Exception e) {
               if (i == 0 || !(e instanceof TimeoutException)) {
                  log.errorReadingRebalancingStatus(coordinator, e);
                  response = SuccessfulResponse.create(Boolean.TRUE);
               }
               log.debug("Timed out waiting for rebalancing status from coordinator, trying again");
            }
         }

         if (response instanceof SuccessfulResponse) {
            globalRebalancingEnabled = ((Boolean) ((SuccessfulResponse) response).getResponseValue());
         } else {
            log.errorReadingRebalancingStatus(coordinator, new CacheException(Objects.toString(response)));
         }
      }
   }

   @Stop(priority = 100)
   public void stop() {
      // Stop blocking cache topology commands.
      clusterManagerLock.lock();
      try {
         clusterManagerStatus = ClusterManagerStatus.STOPPING;
         clusterStateChanged.signalAll();
      } finally {
         clusterManagerLock.unlock();
      }

      if (viewListener != null) {
         cacheManagerNotifier.removeListener(viewListener);
      }
      if (viewHandlingExecutor != null) {
         viewHandlingExecutor.cancelQueuedTasks();
      }
   }

   @Override
   public CacheStatusResponse handleJoin(String cacheName, Address joiner, CacheJoinInfo joinInfo,
         int joinerViewId) throws Exception {
      ClusterCacheStatus cacheStatus;
      clusterManagerLock.lock();
      try {
         waitForJoinerView(joiner, joinerViewId, joinInfo.getTimeout());

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

         cacheStatus = initCacheStatusIfAbsent(cacheName, joinInfo.getCacheMode());
      } finally {
         clusterManagerLock.unlock();
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
      if (cacheStatus.doLeave(leaver)) {
         cacheStatusMap.remove(cacheName);
      }
   }

   @Override
   public void handleRebalancePhaseConfirm(String cacheName, Address node, int topologyId, Throwable throwable, int viewId) throws Exception {
      if (throwable != null) {
         // TODO We could try to update the pending CH such that nodes reporting errors are not considered to hold any state
         // For now we are just logging the error and proceeding as if the rebalance was successful everywhere
         log.rebalanceError(cacheName, node, topologyId, throwable);
      }

      CLUSTER.rebalanceCompleted(cacheName, node, topologyId);
      eventLogManager.getEventLogger().context(cacheName).scope(node.toString()).info(EventLogCategory.CLUSTER, MESSAGES.rebalancePhaseConfirmed(node, topologyId));


      ClusterCacheStatus cacheStatus = cacheStatusMap.get(cacheName);
      if (cacheStatus == null) {
         log.debugf("Ignoring rebalance confirmation from %s " +
               "for cache %s because it doesn't have a cache status entry", node, cacheName);
         return;
      }

      cacheStatus.confirmRebalancePhase(node, topologyId);
   }

   private static class CacheTopologyFilterReuser implements ResponseFilter {
      Map<CacheTopology, CacheTopology> seenTopologies = new HashMap<>();
      Map<CacheJoinInfo, CacheJoinInfo> seenInfos = new HashMap<>();

      @Override
      public boolean isAcceptable(Response response, Address sender) {
         if (response.isSuccessful()) {
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

               CacheTopology replaceStableTopology;
               // If the don't equal check if we replace - note stableTopology can be null
               if (!Objects.equals(cacheTopology, stableTopology)) {
                  replaceStableTopology = seenTopologies.get(stableTopology);
                  if (replaceStableTopology == null) {
                     seenTopologies.put(stableTopology, stableTopology);
                     replaceStableTopology = stableTopology;
                  }
               } else {
                  // Since they were equal replace it with the cache topology we are going to use
                  replaceStableTopology = replaceCacheTopology;
               }

               CacheJoinInfo info = csr.getCacheJoinInfo();
               CacheJoinInfo replaceInfo = seenInfos.get(info);
               if (replaceInfo == null) {
                  seenInfos.put(info, info);
               }

               if (replaceCacheTopology != null || replaceStableTopology != null || replaceInfo != null) {
                  entry.setValue(new CacheStatusResponse(replaceInfo != null ? replaceInfo : info,
                        replaceCacheTopology, replaceStableTopology, csr.getAvailabilityMode()));
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

   private void handleClusterView(boolean mergeView, int newViewId) {
      try {
         if (!updateClusterState(mergeView, newViewId)) {
            return;
         }

         // The LimitedExecutor acts as a critical section, so we don't need to worry about multiple threads.
         if (clusterManagerStatus == ClusterManagerStatus.RECOVERING_CLUSTER) {
            if (!becomeCoordinator(newViewId)) {
               return;
            }
         }

         if (clusterManagerStatus == ClusterManagerStatus.COORDINATOR) {
            // If we have recovered the cluster status, we rebalance the caches to include minor partitions
            // If we processed a regular view, we prune members that left.
            updateCacheMembers(transport.getMembers());
         }
      } catch (Throwable t) {
         log.viewHandlingError(newViewId, t);
      }
   }

   private boolean becomeCoordinator(int newViewId) {
      // Clean up leftover cache status information from the last time we were coordinator.
      // E.g. if the local node was coordinator, started a rebalance, and then lost coordinator
      // status because of a merge, the existing cache statuses may have a rebalance in progress.
      cacheStatusMap.clear();
      try {
         recoverClusterStatus(newViewId, transport.getMembers());

         clusterManagerLock.lock();
         try {
            if (viewId != newViewId) {
               log.debugf("View updated while we were recovering the cluster for view %d", newViewId);
               return false;
            }
            clusterManagerStatus = ClusterManagerStatus.COORDINATOR;
            // notify threads that might be waiting to join
            clusterStateChanged.signalAll();
         } finally {
            clusterManagerLock.unlock();
         }
      } catch (InterruptedException e) {
         if (trace)
            log.tracef("Cluster state recovery interrupted because the coordinator is shutting down");
      } catch (SuspectException e) {
         if (trace)
            log.tracef("Cluster state recovery interrupted because a member was lost. Will retry.");
      } catch (Exception e) {
         if (clusterManagerStatus.isRunning()) {
            CLUSTER.failedToRecoverClusterState(e);
            eventLogManager.getEventLogger().detail(e)
                  .fatal(EventLogCategory.CLUSTER, MESSAGES.clusterRecoveryFailed(transport.getMembers()));
         } else {
            log.tracef("Cluster state recovery failed because the coordinator is shutting down");
         }
      }
      return true;
   }

   private boolean updateClusterState(boolean mergeView, int newViewId) {
      clusterManagerLock.lock();
      try {
         if (newViewId < transport.getViewId()) {
            log.tracef("Ignoring old cluster view notification: %s", newViewId);
            return false;
         }

         boolean isCoordinator = transport.isCoordinator();
         boolean becameCoordinator = isCoordinator && !clusterManagerStatus.isCoordinator();
         if (trace) {
            log.tracef("Received new cluster view: %d, isCoordinator = %s, old status = %s", (Object) newViewId,
                  isCoordinator, clusterManagerStatus);
         }

         if (!isCoordinator) {
            clusterManagerStatus = ClusterManagerStatus.REGULAR_MEMBER;
            return false;
         }
         if (becameCoordinator || mergeView) {
            clusterManagerStatus = ClusterManagerStatus.RECOVERING_CLUSTER;
         }

         // notify threads that might be waiting to join
         viewId = newViewId;
         clusterStateChanged.signalAll();
      } finally {
         clusterManagerLock.unlock();
      }
      return true;
   }

   private ClusterCacheStatus initCacheStatusIfAbsent(String cacheName, CacheMode cacheMode) {
      return cacheStatusMap.computeIfAbsent(cacheName, (name) -> {
         // We assume that any cache with partition handling configured is already defined on all the nodes
         // (including the coordinator) before it starts on any node.
         LostDataCheck lostDataCheck;
         if (cacheMode.isScattered()) {
            lostDataCheck = ClusterTopologyManagerImpl::scatteredLostDataCheck;
         } else {
            lostDataCheck = ClusterTopologyManagerImpl::distLostDataCheck;
         }
         AvailabilityStrategy availabilityStrategy;
         Configuration config = cacheManager.getCacheConfiguration(cacheName);
         PartitionHandling partitionHandling = config != null ? config.clustering().partitionHandling().whenSplit() : null;
         boolean resolveConflictsOnMerge = resolveConflictsOnMerge(config, cacheMode);
         if (partitionHandling != null && partitionHandling != PartitionHandling.ALLOW_READ_WRITES) {
            availabilityStrategy = new PreferConsistencyStrategy(eventLogManager, persistentUUIDManager, lostDataCheck);
         } else {
            availabilityStrategy = new PreferAvailabilityStrategy(eventLogManager, persistentUUIDManager, lostDataCheck);
         }
         Optional<GlobalStateManager> globalStateManager = gcr.getOptionalComponent(GlobalStateManager.class);
         Optional<ScopedPersistentState> persistedState = globalStateManager.flatMap(gsm -> gsm.readScopedState(cacheName));
         return new ClusterCacheStatus(cacheManager, cacheName, availabilityStrategy, RebalanceType.from(cacheMode),
               this, transport,
               persistedState, persistentUUIDManager, resolveConflictsOnMerge);
      });
   }

   private boolean resolveConflictsOnMerge(Configuration config, CacheMode cacheMode) {
      if (config == null || cacheMode.isScattered() || cacheMode.isInvalidation())
         return false;

      return config.clustering().partitionHandling().resolveConflictsOnMerge();
   }

   @Override
   public void broadcastRebalanceStart(String cacheName, CacheTopology cacheTopology, boolean totalOrder, boolean distributed) {
      CLUSTER.startRebalance(cacheName, cacheTopology);
      eventLogManager.getEventLogger().context(cacheName).scope(transport.getAddress()).info(EventLogCategory.CLUSTER, MESSAGES.rebalanceStarted(cacheTopology.getTopologyId()));
      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
            CacheTopologyControlCommand.Type.REBALANCE_START, transport.getAddress(), cacheTopology, null,
            viewId);
      executeOnClusterAsync(command, getGlobalTimeout(), totalOrder, distributed);
   }

   private void recoverClusterStatus(int newViewId, final List<Address> clusterMembers) throws Exception {
      log.debugf("Recovering cluster status for view %d", newViewId);
      ReplicableCommand command = new CacheTopologyControlCommand(null,
            CacheTopologyControlCommand.Type.GET_STATUS, transport.getAddress(), newViewId);
      Map<Address, Object> statusResponses = null;
      // Assume any timeout is because one of the nodes didn't have a CommandAwareRpcDispatcher
      // installed at the time (possible with JGroupsChannelLookup and shouldConnect == false), and retry.
      for (int i = CLUSTER_RECOVERY_ATTEMPTS - 1; i >= 0; i--) {
         try {
            statusResponses =
                  executeOnClusterSync(command, getGlobalTimeout() / CLUSTER_RECOVERY_ATTEMPTS, false, false,
                        new CacheTopologyFilterReuser());
            break;
         } catch (ExecutionException e) {
            if (i != 0) {
               if (e.getCause() instanceof TimeoutException) {
                  log.debug("Timed out waiting for cluster status responses, trying again");
               } else if (e.getCause() instanceof SuspectException) {
                  if (transport.getMembers().containsAll(clusterMembers)) {
                     int sleepTime = getGlobalTimeout() / CLUSTER_RECOVERY_ATTEMPTS / 2;
                     log.debugf(e, "Received an exception from one of the members, will try again after %d ms", sleepTime);
                     Thread.sleep(sleepTime);
                  }
               }
               continue;
            }
            throw e;
         }
      }

      log.debugf("Got %d status responses. members are %s", statusResponses.size(), clusterMembers);
      Map<String, Map<Address, CacheStatusResponse>> responsesByCache = new HashMap<>();
      boolean recoveredRebalancingStatus = true;
      for (Map.Entry<Address, Object> responseEntry : statusResponses.entrySet()) {
         Address sender = responseEntry.getKey();
         ManagerStatusResponse nodeStatus = (ManagerStatusResponse) responseEntry.getValue();
         recoveredRebalancingStatus &= nodeStatus.isRebalancingEnabled();
         for (Map.Entry<String, CacheStatusResponse> statusEntry : nodeStatus.getCaches().entrySet()) {
            String cacheName = statusEntry.getKey();
            Map<Address, CacheStatusResponse> cacheResponses = responsesByCache.computeIfAbsent(cacheName, k -> new HashMap<>());
            cacheResponses.put(sender, statusEntry.getValue());
         }
      }

      globalRebalancingEnabled = recoveredRebalancingStatus;
      // Compute the new consistent hashes on separate threads
      int maxThreads = Runtime.getRuntime().availableProcessors() / 2 + 1;
      CountDownLatch latch = new CountDownLatch(responsesByCache.size());
      LimitedExecutor cs = new LimitedExecutor("Merge-" + newViewId, asyncTransportExecutor, maxThreads);
      for (final Map.Entry<String, Map<Address, CacheStatusResponse>> e : responsesByCache.entrySet()) {
         CacheJoinInfo joinInfo = e.getValue().values().stream().findAny().get().getCacheJoinInfo();
         ClusterCacheStatus cacheStatus = initCacheStatusIfAbsent(e.getKey(), joinInfo.getCacheMode());
         cs.execute(() -> {
            try {
               cacheStatus.doMergePartitions(e.getValue());
            } finally {
               latch.countDown();
            }
         });
      }
      latch.await(getGlobalTimeout(), TimeUnit.MILLISECONDS);
   }

   public void updateCacheMembers(List<Address> newClusterMembers) {
      try {
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
      } catch (Exception e) {
         if (clusterManagerStatus.isRunning()) {
            log.errorUpdatingMembersList(e);
         }
      }
   }

   private void confirmMembersAvailable() throws Exception {
      transport.invokeRemotely(null, HeartBeatCommand.INSTANCE, ResponseMode.SYNCHRONOUS, getGlobalTimeout(), null, DeliverOrder.NONE, false);
   }

   /**
    * Wait until we have received view {@code joinerViewId} and we have finished recovering the cluster state.
    * <p>
    * Returns early if the node is shutting down.
    * <p>
    * This method should be invoked with the lock hold.
    *
    * @throws TimeoutException if the timeout expired.
    */
   @GuardedBy("clusterManagerLock")
   private void waitForJoinerView(Address joiner, int joinerViewId, long timeout)
         throws InterruptedException {
      if (joinerViewId > viewId || clusterManagerStatus == ClusterManagerStatus.RECOVERING_CLUSTER) {
         if (trace) {
            if (joinerViewId > viewId) {
               log.tracef("Waiting to install view %s before processing join request from %s",
                     joinerViewId, joiner);
            } else {
               log.tracef("Waiting to recover cluster status before processing join request from %s", joiner);
            }
         }
         long nanosTimeout = TimeUnit.MILLISECONDS.toNanos(timeout);
         while ((viewId < joinerViewId || clusterManagerStatus == ClusterManagerStatus.RECOVERING_CLUSTER) &&
               clusterManagerStatus.isRunning()) {
            if (nanosTimeout <= 0) {
               throw log.coordinatorTimeoutWaitingForView(joinerViewId, transport.getViewId(), clusterManagerStatus);
            }
            nanosTimeout = clusterStateChanged.awaitNanos(nanosTimeout);
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
         localResponse = (Response) command.invoke();
      } catch (Throwable throwable) {
         throw new Exception(throwable);
      }

      return extractResponseValues(CompletableFutures.await(remoteFuture), localResponse);
   }

   private int getGlobalTimeout() {
      // TODO Rename setting to something like globalRpcTimeout
      return (int) globalConfiguration.transport().distributedSyncTimeout();
   }

   private void executeOnClusterAsync(final ReplicableCommand command, final int timeout, boolean totalOrder, boolean distributed) {
      if (!totalOrder) {
         // invoke the command on the local node
         asyncTransportExecutor.submit(() -> {
            try {
               if (trace) log.tracef("Attempting to execute command on self: %s", command);
               gcr.wireDependencies(command);
               command.invoke();
            } catch (Throwable throwable) {
               // The command already logs any exception in invoke()
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
      if (cacheName == null) {
         return isRebalancingEnabled();
      } else {
         ClusterCacheStatus s = cacheStatusMap.get(cacheName);
         return s != null ? s.isRebalanceEnabled() : isRebalancingEnabled();
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

   @Override
   public void broadcastShutdownCache(String cacheName, CacheTopology cacheTopology, boolean totalOrder, boolean distributed) throws Exception {
      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
            CacheTopologyControlCommand.Type.SHUTDOWN_PERFORM, transport.getAddress(), cacheTopology, null, viewId);
      executeOnClusterSync(command, getGlobalTimeout(), totalOrder, distributed, null);
   }

   @Override
   public void setInitialCacheTopologyId(String cacheName, int topologyId) {
      Configuration configuration = cacheManager.getCacheConfiguration(cacheName);
      ClusterCacheStatus cacheStatus = initCacheStatusIfAbsent(cacheName, configuration.clustering().cacheMode());
      cacheStatus.setInitialTopologyId(topologyId);
   }

   @Override
   public void handleShutdownRequest(String cacheName) throws Exception {
      ClusterCacheStatus cacheStatus = cacheStatusMap.get(cacheName);
      cacheStatus.shutdownCache();
   }

   @Listener(sync = true)
   public class ClusterViewListener {
      @Merged
      @ViewChanged
      public void handleViewChange(final ViewChangedEvent e) {
         // Need to recover existing caches asynchronously (in case we just became the coordinator).
         // Cannot use the async notification thread pool, by default it only has 1 thread.
         viewHandlingExecutor.execute(() -> handleClusterView(e.isMergeView(), e.getViewId()));
         EventLogger eventLogger = eventLogManager.getEventLogger().scope(e.getLocalAddress());
         logNodeJoined(eventLogger, e.getNewMembers(), e.getOldMembers());
         logNodeLeft(eventLogger, e.getNewMembers(), e.getOldMembers());
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
      if (response == CacheNotFoundResponse.INSTANCE) {
         return;
      } else if (!response.isSuccessful()) {
         Throwable cause = response instanceof ExceptionResponse ? ((ExceptionResponse) response).getException() : null;
         throw new CacheException(format("Unsuccessful response received from node '%s': %s", origin, response), cause);
      }
      values.put(origin, ((SuccessfulResponse) response).getResponseValue());
   }

   private static void logNodeJoined(EventLogger logger, List<Address> newMembers, List<Address> oldMembers) {
      newMembers.stream()
            .filter(address -> !oldMembers.contains(address))
            .forEach(address -> logger.info(EventLogCategory.CLUSTER, MESSAGES.nodeJoined(address)));
   }

   private static void logNodeLeft(EventLogger logger, List<Address> newMembers, List<Address> oldMembers) {
      oldMembers.stream()
            .filter(address -> !newMembers.contains(address))
            .forEach(address -> logger.info(EventLogCategory.CLUSTER, MESSAGES.nodeLeft(address)));
   }

   public static boolean scatteredLostDataCheck(ConsistentHash stableCH, List<Address> newMembers) {
      Set<Address> lostMembers = new HashSet<>(stableCH.getMembers());
      lostMembers.removeAll(newMembers);
      log.debugf("Stable CH members: %s, actual members: %s, lost members: %s",
                 stableCH.getMembers(), newMembers, lostMembers);
      return lostMembers.size() > 1;
   }

   public static boolean distLostDataCheck(ConsistentHash stableCH, List<Address> newMembers) {
      for (int i = 0; i < stableCH.getNumSegments(); i++) {
         if (!InfinispanCollections.containsAny(newMembers, stableCH.locateOwnersForSegment(i)))
            return true;
      }
      return false;
   }
}
