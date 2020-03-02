package org.infinispan.topology;


import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;
import static org.infinispan.factories.KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR;
import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.infinispan.util.logging.Log.CLUSTER;
import static org.infinispan.util.logging.events.Messages.MESSAGES;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.commons.util.ProcessorInfo;
import org.infinispan.configuration.ConfigurationManager;
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
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
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
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.ResponseCollectors;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.ValidResponseCollector;
import org.infinispan.remoting.transport.impl.VoidResponseCollector;
import org.infinispan.statetransfer.RebalanceType;
import org.infinispan.util.concurrent.ActionSequencer;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.concurrent.ConditionFuture;
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
@Scope(Scopes.GLOBAL)
public class ClusterTopologyManagerImpl implements ClusterTopologyManager {

   public static final int INITIAL_CONNECTION_ATTEMPTS = 10;
   public static final int CLUSTER_RECOVERY_ATTEMPTS = 10;

   private static final Log log = LogFactory.getLog(ClusterTopologyManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final CompletableFuture<CacheStatusResponseCollector> SKIP_RECOVERY_FUTURE =
         CompletableFutures.completedExceptionFuture(new IllegalStateException());

   @Inject Transport transport;
   @Inject GlobalConfiguration globalConfiguration;
   @Inject ConfigurationManager configurationManager;
   @Inject GlobalComponentRegistry gcr;
   @Inject CacheManagerNotifier cacheManagerNotifier;
   @Inject EmbeddedCacheManager cacheManager;
   @Inject
   @ComponentName(ASYNC_TRANSPORT_EXECUTOR)
   ExecutorService transportExecutor;
   @Inject
   @ComponentName(TIMEOUT_SCHEDULE_EXECUTOR)
   ScheduledExecutorService timeoutScheduledExecutor;
   @Inject EventLogManager eventLogManager;
   @Inject PersistentUUIDManager persistentUUIDManager;
   @Inject TimeService timeService;

   private TopologyManagementHelper helper;
   private ConditionFuture<ClusterTopologyManagerImpl> joinViewFuture;
   private ActionSequencer actionSequencer;

   private final Lock updateLock = new ReentrantLock();
   @GuardedBy("updateLock")
   private int viewId = -1;
   @GuardedBy("updateLock")
   private ClusterManagerStatus clusterManagerStatus = ClusterManagerStatus.INITIALIZING;
   @GuardedBy("updateLock")
   private final ConcurrentMap<String, ClusterCacheStatus> cacheStatusMap = new ConcurrentHashMap<>();
   private AtomicInteger recoveryAttemptCount = new AtomicInteger();

   // The global rebalancing status
   private boolean globalRebalancingEnabled = true;

   private final ClusterViewListener viewListener = new ClusterViewListener();

   @Start(priority = 100)
   public void start() {
      helper = new TopologyManagementHelper(gcr.getComponent(BasicComponentRegistry.class));
      joinViewFuture = new ConditionFuture<>(timeoutScheduledExecutor);
      actionSequencer = new ActionSequencer(transportExecutor, true, timeService);

      cacheManagerNotifier.addListener(viewListener);
      // The listener already missed the initial view
      handleClusterView(false, transport.getViewId());

      globalRebalancingEnabled = join(fetchRebalancingStatusFromCoordinator(INITIAL_CONNECTION_ATTEMPTS));
   }

   private CompletionStage<Boolean> fetchRebalancingStatusFromCoordinator(int attempts) {
      if (transport.isCoordinator()) {
         return CompletableFutures.completedTrue();
      }
      ReplicableCommand command =
            new CacheTopologyControlCommand(null, CacheTopologyControlCommand.Type.POLICY_GET_STATUS,
                                            transport.getAddress(), -1);
      Address coordinator = transport.getCoordinator();
      return helper.executeOnCoordinator(transport, command, getGlobalTimeout() / INITIAL_CONNECTION_ATTEMPTS)
                   .handle((value, throwable) -> {
                      if (throwable == null)
                         return CompletableFuture.completedFuture((Boolean) value);

                      if (attempts == 1 || !(throwable instanceof TimeoutException)) {
                         log.errorReadingRebalancingStatus(coordinator, throwable);
                         return CompletableFutures.completedTrue();
                      }
                      // Assume any timeout is because the coordinator doesn't have a CommandAwareRpcDispatcher yet
                      // (possible with ForkChannels or JGroupsChannelLookup and shouldConnect = false), and retry.
                      log.debug("Timed out waiting for rebalancing status from coordinator, trying again");
                      return fetchRebalancingStatusFromCoordinator(attempts - 1);
                   }).thenCompose(Function.identity());
   }

   @Stop(priority = 100)
   public void stop() {
      // Stop blocking cache topology commands.
      updateLock.lock();
      try {
         clusterManagerStatus = ClusterManagerStatus.STOPPING;
         joinViewFuture.stop();
      } finally {
         updateLock.unlock();
      }

      cacheManagerNotifier.removeListener(viewListener);
   }

   @Override
   public ClusterManagerStatus getStatus() {
      return clusterManagerStatus;
   }

   @Override
   public CompletionStage<CacheStatusResponse> handleJoin(String cacheName, Address joiner, CacheJoinInfo joinInfo,
                                                          int joinerViewId) {
      CompletionStage<Void> viewStage;
      if (joinerViewId <= viewId && clusterManagerStatus != ClusterManagerStatus.RECOVERING_CLUSTER) {
         viewStage = CompletableFutures.completedNull();
      } else {
         if (trace) {
            log.tracef("Delaying join request from %s until view %s is installed (and cluster status is recovered)",
                       joiner, joinerViewId);
         }
         viewStage = joinViewFuture.newConditionStage(ctmi -> ctmi.canHandleJoin(joinerViewId),
                                                      joinInfo.getTimeout(), MILLISECONDS)
                                   .exceptionally(e -> wrapJoinTimeoutException(joinerViewId, e));
      }

      // After we have the right view, obtain the ClusterCacheStatus
      return viewStage.thenApply(v -> {
         ClusterCacheStatus cacheStatus = prepareJoin(cacheName, joiner, joinInfo, joinerViewId);
         if (cacheStatus == null) {
            // We have a newer view
            // Return null so that the joiner is forced to retry
            return null;
         }
         return cacheStatus.doJoin(joiner, joinInfo);
      });
   }

   private Void wrapJoinTimeoutException(int joinerViewId, Throwable e) {
      if (e instanceof TimeoutException) {
         throw log.coordinatorTimeoutWaitingForView(joinerViewId, viewId, clusterManagerStatus);
      }
      throw CompletableFutures.asCompletionException(e);
   }

   private ClusterCacheStatus prepareJoin(String cacheName, Address joiner, CacheJoinInfo joinInfo,
                                          int joinerViewId) {
      updateLock.lock();
      try {
         if (!clusterManagerStatus.isRunning()) {
            log.debugf("Ignoring join request from %s for cache %s, the local cache manager is shutting down",
                       joiner, cacheName);
            throw new IllegalLifecycleStateException();
         }
         if (joinerViewId < viewId) {
            log.debugf("Ignoring join request from %s for cache %s, joiner's view id is too old: %d",
                       joiner, cacheName, joinerViewId);
            return null;
         }

         return initCacheStatusIfAbsent(cacheName, joinInfo.getCacheMode());
      } finally {
         updateLock.unlock();
      }
   }

   private boolean canHandleJoin(int joinerViewId) {
      updateLock.lock();
      try {
         return joinerViewId <= viewId &&
                clusterManagerStatus != ClusterManagerStatus.RECOVERING_CLUSTER &&
                clusterManagerStatus != ClusterManagerStatus.INITIALIZING;
      } finally {
         updateLock.unlock();
      }
   }

   @Override
   public CompletionStage<Void> handleLeave(String cacheName, Address leaver, int viewId) throws Exception {
      if (!clusterManagerStatus.isRunning()) {
         log.debugf("Ignoring leave request from %s for cache %s, the local cache manager is shutting down",
                    leaver, cacheName);
         return CompletableFutures.completedNull();
      }

      ClusterCacheStatus cacheStatus = cacheStatusMap.get(cacheName);
      if (cacheStatus == null) {
         // This can happen if we've just become coordinator
         log.tracef("Ignoring leave request from %s for cache %s because it doesn't have a cache status entry",
                    leaver, cacheName);
         return CompletableFutures.completedNull();
      }
      return cacheStatus.doLeave(leaver);
   }

   synchronized void removeCacheStatus(String cacheName) {
      cacheStatusMap.remove(cacheName);
   }

   @Override
   public CompletionStage<Void> handleRebalancePhaseConfirm(String cacheName, Address node, int topologyId,
                                                            Throwable throwable, int viewId) throws Exception {
      if (throwable != null) {
         // TODO We could try to update the pending CH such that nodes reporting errors are not considered to hold
         //  any state
         // For now we are just logging the error and proceeding as if the rebalance was successful everywhere
         log.rebalanceError(cacheName, node, topologyId, throwable);
      }

      ClusterCacheStatus cacheStatus = cacheStatusMap.get(cacheName);
      if (cacheStatus == null) {
         log.debugf("Ignoring rebalance confirmation from %s " +
                    "for cache %s because it doesn't have a cache status entry", node, cacheName);
         return CompletableFutures.completedNull();
      }

      cacheStatus.confirmRebalancePhase(node, topologyId);
      return CompletableFutures.completedNull();
   }

   private static class CacheStatusResponseCollector extends ValidResponseCollector<CacheStatusResponseCollector> {
      private final Map<String, Map<Address, CacheStatusResponse>> responsesByCache = new HashMap<>();
      private final List<Address> suspectedMembers = new ArrayList<>();
      private final Map<CacheTopology, CacheTopology> seenTopologies = new HashMap<>();
      private final Map<CacheJoinInfo, CacheJoinInfo> seenInfos = new HashMap<>();
      private boolean rebalancingEnabled = true;

      @Override
      protected CacheStatusResponseCollector addValidResponse(Address sender, ValidResponse response) {
         if (response.isSuccessful()) {
            ManagerStatusResponse nodeStatus = (ManagerStatusResponse) response.getResponseValue();
            rebalancingEnabled &= nodeStatus.isRebalancingEnabled();

            for (Entry<String, CacheStatusResponse> entry : nodeStatus.getCaches().entrySet()) {
               String cacheName = entry.getKey();
               CacheStatusResponse csr = entry.getValue();
               CacheTopology cacheTopology = intern(seenTopologies, csr.getCacheTopology());
               CacheTopology stableTopology = intern(seenTopologies, csr.getStableTopology());
               CacheJoinInfo info = intern(seenInfos, csr.getCacheJoinInfo());

               Map<Address, CacheStatusResponse> cacheResponses =
                     responsesByCache.computeIfAbsent(cacheName, k -> new HashMap<>());
               cacheResponses.put(sender, new CacheStatusResponse(info, cacheTopology, stableTopology,
                                                                  csr.getAvailabilityMode()));
            }
         }
         return null;
      }

      private <T> T intern(Map<T, T> internMap, T value) {
         T replacementValue = internMap.get(value);
         if (replacementValue == null) {
            internMap.put(value, value);
            replacementValue = value;
         }
         return replacementValue;
      }

      @Override
      protected CacheStatusResponseCollector addTargetNotFound(Address sender) {
         suspectedMembers.add(sender);
         return null;
      }

      @Override
      protected CacheStatusResponseCollector addException(Address sender, Exception exception) {
         throw ResponseCollectors.wrapRemoteException(sender, exception);
      }

      @Override
      public CacheStatusResponseCollector finish() {
         return this;
      }

      public Map<String, Map<Address, CacheStatusResponse>> getResponsesByCache() {
         return responsesByCache;
      }


      public boolean getRebalancingEnabled() {
         return rebalancingEnabled;
      }

      public List<Address> getSuspectedMembers() {
         return suspectedMembers;
      }
   }

   private void handleClusterView(boolean mergeView, int newViewId) {
      orderOnManager(() -> {
         try {
            if (!updateClusterState(mergeView, newViewId))
               return CompletableFutures.completedNull();
            if (clusterManagerStatus == ClusterManagerStatus.RECOVERING_CLUSTER) {
               return recoverClusterStatus(newViewId);
            } else if (clusterManagerStatus == ClusterManagerStatus.COORDINATOR) {
               // Unblock any joiners waiting for the view
               joinViewFuture.updateAsync(this, transportExecutor);

               // If we have recovered the cluster status, we rebalance the caches to include minor partitions
               // If we processed a regular view, we prune members that left.
               return updateCacheMembers(newViewId);
            }
         } catch (Throwable t) {
            log.viewHandlingError(newViewId, t);
         }
         return CompletableFutures.completedNull();
      });
   }

   private <T> CompletionStage<T> orderOnManager(Callable<CompletionStage<T>> action) {
      return actionSequencer.orderOnKey(ClusterTopologyManagerImpl.class, action);
   }

   private <T> CompletionStage<T> orderOnCache(String cacheName, Callable<CompletionStage<T>> action) {
      return actionSequencer.orderOnKey(cacheName, action);
   }

   private CompletionStage<Void> orderOnCache(String cacheName, Runnable action) {
      return actionSequencer.orderOnKey(cacheName, () -> {
         action.run();
         return CompletableFutures.completedNull();
      });
   }

   private CompletionStage<Void> recoverClusterStatus(int newViewId) {
      // Clean up leftover cache status information from the last time we were coordinator.
      // E.g. if the local node was coordinator, started a rebalance, and then lost coordinator
      // status because of a merge, the existing cache statuses may have a rebalance in progress.
      cacheStatusMap.clear();

      recoveryAttemptCount.set(0);

      return fetchClusterStatus(newViewId).thenCompose(responseCollector -> {
         Map<String, Map<Address, CacheStatusResponse>> responsesByCache =
               responseCollector.getResponsesByCache();
         log.debugf("Cluster recovery found %d caches, members are %s", responsesByCache.size(),
                    transport.getMembers());

         // Compute the new consistent hashes on separate threads
         int maxThreads = ProcessorInfo.availableProcessors() / 2 + 1;
         AggregateCompletionStage<Void> mergeStage = CompletionStages.aggregateCompletionStage();
         LimitedExecutor cs = new LimitedExecutor("Merge-" + newViewId, transportExecutor, maxThreads);
         for (final Entry<String, Map<Address, CacheStatusResponse>> e : responsesByCache.entrySet()) {
            CacheJoinInfo joinInfo = e.getValue().values().iterator().next().getCacheJoinInfo();
            ClusterCacheStatus cacheStatus = initCacheStatusIfAbsent(e.getKey(), joinInfo.getCacheMode());
            mergeStage.dependsOn(runAsync(() -> {
               cacheStatus.doMergePartitions(e.getValue());
            }, cs));
         }
         return mergeStage.freeze().thenRun(() -> {
            updateLock.lock();
            try {
               if (viewId != newViewId) {
                  log.debugf("View updated while we were recovering the cluster for view %d", newViewId);
                  return;
               }
               clusterManagerStatus = ClusterManagerStatus.COORDINATOR;
               globalRebalancingEnabled = responseCollector.getRebalancingEnabled();
            } finally {
               updateLock.unlock();
            }

            for (ClusterCacheStatus cacheStatus : cacheStatusMap.values()) {
               orderOnCache(cacheStatus.getCacheName(), () -> {
                  try {
                     cacheStatus.doHandleClusterView(newViewId);
                  } catch (Throwable throwable) {
                     if (clusterManagerStatus.isRunning()) {
                        log.errorUpdatingMembersList(newViewId, throwable);
                     }
                  }
               });
            }

            // Unblock any joiners waiting for the view
            joinViewFuture.updateAsync(this, transportExecutor);
         });
      });
   }

   private boolean updateClusterState(boolean mergeView, int newViewId) {
      updateLock.lock();
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
      } finally {
         updateLock.unlock();
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
         // TODO Partition handling config should be part of the join info
         AvailabilityStrategy availabilityStrategy;
         Configuration config = configurationManager.getConfiguration(cacheName, true);
         PartitionHandling partitionHandling =
               config != null ? config.clustering().partitionHandling().whenSplit() : null;
         boolean resolveConflictsOnMerge = resolveConflictsOnMerge(config, cacheMode);
         if (partitionHandling != null && partitionHandling != PartitionHandling.ALLOW_READ_WRITES) {
            availabilityStrategy = new PreferConsistencyStrategy(eventLogManager, persistentUUIDManager, lostDataCheck);
         } else {
            availabilityStrategy = new PreferAvailabilityStrategy(eventLogManager, persistentUUIDManager,
                                                                  lostDataCheck);
         }
         Optional<GlobalStateManager> globalStateManager = gcr.getOptionalComponent(GlobalStateManager.class);
         Optional<ScopedPersistentState> persistedState =
               globalStateManager.flatMap(gsm -> gsm.readScopedState(cacheName));
         return new ClusterCacheStatus(cacheManager, cacheName, availabilityStrategy, RebalanceType.from(cacheMode),
                                       this, transport,
                                       persistentUUIDManager, eventLogManager, persistedState, resolveConflictsOnMerge);
      });
   }

   private boolean resolveConflictsOnMerge(Configuration config, CacheMode cacheMode) {
      if (config == null || cacheMode.isScattered() || cacheMode.isInvalidation())
         return false;

      return config.clustering().partitionHandling().resolveConflictsOnMerge();
   }

   void broadcastRebalanceStart(String cacheName, CacheTopology cacheTopology, boolean totalOrder) {
      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
                                                                  CacheTopologyControlCommand.Type.REBALANCE_START,
                                                                  transport.getAddress(), cacheTopology, null,
                                                                  viewId);
      helper.executeOnClusterAsync(transport, command, totalOrder);
   }

   private CompletionStage<CacheStatusResponseCollector> fetchClusterStatus(int newViewId) {
      int attemptCount = recoveryAttemptCount.getAndIncrement();
      if (trace)
         log.debugf("Recovering cluster status for view %d, attempt %d", newViewId, attemptCount);
      ReplicableCommand command = new CacheTopologyControlCommand(null,
                                                                  CacheTopologyControlCommand.Type.GET_STATUS,
                                                                  transport.getAddress(), newViewId);
      CacheStatusResponseCollector responseCollector = new CacheStatusResponseCollector();
      int timeout = getGlobalTimeout() / CLUSTER_RECOVERY_ATTEMPTS;
      CompletionStage<CacheStatusResponseCollector> remoteStage =
            helper.executeOnClusterSync(transport, command, timeout, false, responseCollector);
      return CompletionStages.handleAndCompose(remoteStage, (collector, throwable) -> {
         if (newViewId < transport.getViewId()) {
            if (trace)
               log.tracef("Ignoring cluster state responses for view %d, we already have view %d",
                          newViewId, transport.getViewId());
            return SKIP_RECOVERY_FUTURE;
         } else if (throwable == null) {
            if (trace)
               log.tracef("Received valid cluster state responses for view %d", newViewId);
            if (!collector.getSuspectedMembers().isEmpty()) {
               // We got a CacheNotFoundResponse but the view is still the same, assume the JGroups stack
               // includes FORK and the suspected node hasn't connected its ForkChannel yet.
               // That means the node doesn't have any caches running yet, so we can ignore it.
               log.debugf("Missing cache status responses from nodes %s", collector.getSuspectedMembers());
            }
            return CompletableFuture.completedFuture(collector);
         }

         Throwable t = CompletableFutures.extractException(throwable);
         if (t instanceof IllegalLifecycleStateException) {
            // Stop retrying, we are shutting down
            return SKIP_RECOVERY_FUTURE;
         }

         // If we got a TimeoutException, assume JGroupsChannelLookup and shouldConnect == false,
         // and the node that timed out hasn't installed its UpHandler yet.
         // Retry at most CLUSTER_RECOVERY_ATTEMPTS times, then throw the timeout exception
         log.failedToRecoverClusterState(t);
         if (t instanceof TimeoutException && attemptCount < CLUSTER_RECOVERY_ATTEMPTS) {
            return fetchClusterStatus(newViewId);
         }

         throw CompletableFutures.asCompletionException(t);
      });
   }

   private CompletionStage<Void> updateCacheMembers(int viewId) {
      // Confirm that view's members are all available first, so in a network split scenario
      // we can enter degraded mode without starting a rebalance first
      // We don't really need to run on the view handling executor because ClusterCacheStatus
      // has its own synchronization
      return confirmMembersAvailable().whenComplete((ignored, throwable) -> {
         if (throwable == null) {
            try {
               int newViewId = transport.getViewId();
               if (newViewId != viewId) {
                  log.debugf("Skipping cache members update for view %d, newer view received: %d", viewId, newViewId);
                  return;
               }
               for (ClusterCacheStatus cacheStatus : cacheStatusMap.values()) {
                  cacheStatus.doHandleClusterView(viewId);
               }
            } catch (Throwable t) {
               throwable = t;
            }
         }

         if (throwable != null && clusterManagerStatus.isRunning()) {
            log.errorUpdatingMembersList(viewId, throwable);
         }
      });
   }

   private CompletionStage<Void> confirmMembersAvailable() {
      try {
         Set<Address> expectedMembers = new HashSet<>();
         for (ClusterCacheStatus cacheStatus : cacheStatusMap.values()) {
            expectedMembers.addAll(cacheStatus.getExpectedMembers());
         }
         expectedMembers.retainAll(transport.getMembers());
         return transport.invokeCommandOnAll(expectedMembers, HeartBeatCommand.INSTANCE,
                                             VoidResponseCollector.validOnly(),
                                             DeliverOrder.NONE, getGlobalTimeout() / CLUSTER_RECOVERY_ATTEMPTS,
                                             MILLISECONDS);
      } catch (Exception e) {
         return CompletableFutures.completedExceptionFuture(e);
      }
   }

   private int getGlobalTimeout() {
      // TODO Rename setting to something like globalRpcTimeout
      return (int) globalConfiguration.transport().distributedSyncTimeout();
   }

   void broadcastTopologyUpdate(String cacheName, CacheTopology cacheTopology, AvailabilityMode availabilityMode,
                                boolean totalOrder) {
      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
                                                                  CacheTopologyControlCommand.Type.CH_UPDATE,
                                                                  transport.getAddress(), cacheTopology,
                                                                  availabilityMode, viewId);
      helper.executeOnClusterAsync(transport, command, totalOrder);
   }

   void broadcastStableTopologyUpdate(String cacheName, CacheTopology cacheTopology, boolean totalOrder) {
      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
                                                                  CacheTopologyControlCommand.Type.STABLE_TOPOLOGY_UPDATE,
                                                                  transport.getAddress(), cacheTopology,
                                                                  null, viewId);
      helper.executeOnClusterAsync(transport, command, totalOrder);
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
   public CompletionStage<Void> setRebalancingEnabled(String cacheName, boolean enabled) {
      if (cacheName == null) {
         return setRebalancingEnabled(enabled);
      } else {
         ClusterCacheStatus clusterCacheStatus = cacheStatusMap.get(cacheName);
         if (clusterCacheStatus != null) {
            return clusterCacheStatus.setRebalanceEnabled(enabled);
         } else {
            log.debugf("Trying to enable rebalancing for inexistent cache %s", cacheName);
            return CompletableFutures.completedNull();
         }
      }
   }

   @Override
   public CompletionStage<Void> setRebalancingEnabled(boolean enabled) {
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
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> forceRebalance(String cacheName) {
      ClusterCacheStatus cacheStatus = cacheStatusMap.get(cacheName);
      if (cacheStatus != null) {
         cacheStatus.forceRebalance();
      }
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> forceAvailabilityMode(String cacheName, AvailabilityMode availabilityMode) {
      ClusterCacheStatus cacheStatus = cacheStatusMap.get(cacheName);
      if (cacheStatus != null) {
         return cacheStatus.forceAvailabilityMode(availabilityMode);
      }
      return CompletableFutures.completedNull();
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

   public CompletionStage<Void> broadcastShutdownCache(String cacheName, CacheTopology cacheTopology,
                                                       boolean totalOrder) {
      ReplicableCommand command =
            new CacheTopologyControlCommand(cacheName, CacheTopologyControlCommand.Type.SHUTDOWN_PERFORM,
                                            transport.getAddress(), cacheTopology, null, viewId);
      return helper.executeOnClusterSync(transport, command, getGlobalTimeout(), totalOrder,
                                         VoidResponseCollector.validOnly());
   }

   @Override
   public void setInitialCacheTopologyId(String cacheName, int topologyId) {
      // TODO Include cache mode in join info
      Configuration configuration = configurationManager.getConfiguration(cacheName, true);
      ClusterCacheStatus cacheStatus = initCacheStatusIfAbsent(cacheName, configuration.clustering().cacheMode());
      cacheStatus.setInitialTopologyId(topologyId);
   }

   @Override
   public CompletionStage<Void> handleShutdownRequest(String cacheName) throws Exception {
      ClusterCacheStatus cacheStatus = cacheStatusMap.get(cacheName);
      return cacheStatus.shutdownCache();
   }

   @Listener(sync = true)
   public class ClusterViewListener {
      @Merged
      @ViewChanged
      public void handleViewChange(final ViewChangedEvent e) {
         EventLogger eventLogger = eventLogManager.getEventLogger().scope(e.getLocalAddress());
         logNodeJoined(eventLogger, e.getNewMembers(), e.getOldMembers());
         logNodeLeft(eventLogger, e.getNewMembers(), e.getOldMembers());

         handleClusterView(e.isMergeView(), e.getViewId());
      }
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
      log.tracef("Stable CH members: %s, actual members: %s, lost members: %s",
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
