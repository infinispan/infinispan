package org.infinispan.topology;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.factories.KnownComponentNames.NON_BLOCKING_EXECUTOR;
import static org.infinispan.factories.KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR;
import static org.infinispan.commons.util.concurrent.CompletableFutures.completedNull;
import static org.infinispan.util.concurrent.CompletionStages.handleAndCompose;
import static org.infinispan.util.logging.Log.CLUSTER;
import static org.infinispan.util.logging.Log.CONFIG;
import static org.infinispan.util.logging.events.Messages.MESSAGES;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.topology.CacheAvailabilityUpdateCommand;
import org.infinispan.commands.topology.CacheJoinCommand;
import org.infinispan.commands.topology.CacheLeaveCommand;
import org.infinispan.commands.topology.CacheShutdownRequestCommand;
import org.infinispan.commands.topology.RebalancePhaseConfirmCommand;
import org.infinispan.commands.topology.RebalancePolicyUpdateCommand;
import org.infinispan.commands.topology.RebalanceStatusRequestCommand;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.Version;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.globalstate.GlobalStateManager;
import org.infinispan.globalstate.GlobalStateProvider;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.globalstate.impl.GlobalStateManagerImpl;
import org.infinispan.globalstate.impl.ScopedPersistentStateImpl;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.impl.VoidResponseCollector;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.util.concurrent.ActionSequencer;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import net.jcip.annotations.GuardedBy;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogManager;
import org.infinispan.util.logging.events.EventLogger;

/**
 * The {@code LocalTopologyManager} implementation.
 *
 * @author Dan Berindei
 * @since 5.2
 */
@MBean(objectName = "LocalTopologyManager", description = "Controls the cache membership and state transfer")
@Scope(Scopes.GLOBAL)
public class LocalTopologyManagerImpl implements LocalTopologyManager, GlobalStateProvider {
   private static final Log log = LogFactory.getLog(LocalTopologyManagerImpl.class);

   @Inject Transport transport;
   @Inject
   @ComponentName(NON_BLOCKING_EXECUTOR)
   ExecutorService nonBlockingExecutor;
   @Inject
   BlockingManager blockingManager;
   @Inject
   @ComponentName(TIMEOUT_SCHEDULE_EXECUTOR)
   ScheduledExecutorService timeoutExecutor;
   @Inject GlobalComponentRegistry gcr;
   @Inject TimeService timeService;
   @Inject GlobalStateManager globalStateManager;
   @Inject PersistentUUIDManager persistentUUIDManager;
   @Inject EventLogManager eventLogManager;
   @Inject CacheManagerNotifier cacheManagerNotifier;
   // Not used directly, but we have to start the ClusterTopologyManager before sending the join request
   @Inject ClusterTopologyManager clusterTopologyManager;

   private TopologyManagementHelper helper;
   private ActionSequencer actionSequencer;
   private EventLogger eventLogger;

   // We synchronize on the entire map while handling a status request, to make sure there are no concurrent topology
   // updates from the old coordinator.
   private final Map<String, LocalCacheStatus> runningCaches =
         Collections.synchronizedMap(new HashMap<>());
   private volatile boolean running;
   @GuardedBy("runningCaches")
   private int latestStatusResponseViewId;
   private PersistentUUID persistentUUID;

   private EventLoggerViewListener viewListener;

   // This must be invoked before GlobalStateManagerImpl.start
   @Start(priority = 0)
   public void preStart() {
      helper = new TopologyManagementHelper(gcr);
      actionSequencer = new ActionSequencer(nonBlockingExecutor, true, timeService);

      if (globalStateManager != null) {
         globalStateManager.registerStateProvider(this);
      }
   }

   // Arbitrary value, only need to start after the (optional) GlobalStateManager and JGroupsTransport
   @Start(priority = 100)
   public void start() {
      if (log.isTraceEnabled()) {
         log.tracef("Starting LocalTopologyManager on %s", transport.getAddress());
      }
      if (persistentUUID == null) {
         persistentUUID = PersistentUUID.randomUUID();
         globalStateManager.writeGlobalState();
      }
      persistentUUIDManager.addPersistentAddressMapping(transport.getAddress(), persistentUUID);
      eventLogger = eventLogManager.getEventLogger()
            .scope(transport.getAddress())
            .context(this.getClass().getName());
      viewListener = new EventLoggerViewListener(eventLogManager);
      cacheManagerNotifier.addListener(viewListener);

      synchronized (runningCaches) {
         latestStatusResponseViewId = transport.getViewId();
      }
      running = true;
   }

   // Need to stop after ClusterTopologyManagerImpl and before the JGroupsTransport
   @Stop(priority = 110)
   public void stop() {
      if (log.isTraceEnabled()) {
         log.tracef("Stopping LocalTopologyManager on %s", transport.getAddress());
      }
      cacheManagerNotifier.removeListener(viewListener);
      running = false;
   }

   @Override
   public CompletionStage<CacheTopology> join(String cacheName, CacheJoinInfo joinInfo, CacheTopologyHandler stm,
                                              PartitionHandlingManager phm) {
      // Use the action sequencer for the initial join request
      // This ensures that all topology updates from the coordinator will be delayed
      // until the join and the GET_CACHE_LISTENERS request are done
      return orderOnCache(cacheName, () -> {
         log.debugf("Node %s joining cache %s", transport.getAddress(), cacheName);
         LocalCacheStatus cacheStatus = new LocalCacheStatus(joinInfo, stm, phm);
         LocalCacheStatus previousStatus = runningCaches.put(cacheName, cacheStatus);
         if (previousStatus != null) {
            throw new IllegalStateException("A cache can only join once");
         }

         long timeout = joinInfo.getTimeout();
         long endTime = timeService.expectedEndTime(timeout, MILLISECONDS);
         return sendJoinRequest(cacheName, joinInfo, timeout, endTime)
                      .thenCompose(joinResponse -> handleJoinResponse(cacheName, cacheStatus, joinResponse));
      });
   }

   public CompletionStage<CacheStatusResponse> sendJoinRequest(String cacheName, CacheJoinInfo joinInfo, long timeout,
                                                               long endTime) {
      int viewId = transport.getViewId();
      ReplicableCommand command = new CacheJoinCommand(cacheName, transport.getAddress(), joinInfo, viewId);
      return handleAndCompose(helper.executeOnCoordinator(transport, command, timeout), (response, throwable) -> {
         int currentViewId = transport.getViewId();
         if (viewId != currentViewId) {
            log.tracef("Received new view %d before join response for cache %s, retrying", currentViewId, cacheName);
            return sendJoinRequest(cacheName, joinInfo, timeout, endTime);
         }

         if (throwable == null) {
            if (response != null) {
               return CompletableFuture.completedFuture(((CacheStatusResponse) response));
            } else {
               log.debugf("Coordinator sent a null join response, retrying in view %d", viewId + 1);
               return retryJoinInView(cacheName, joinInfo, timeout, endTime, viewId + 1);
            }
         }

         Throwable t = CompletableFutures.extractException(throwable);
         if (t instanceof SuspectException) {
            // Either the coordinator is shutting down
            // Or the JGroups stack includes FORK and the coordinator hasn't connected its ForkChannel yet.
            log.debugf("Join request received CacheNotFoundResponse for cache %s, retrying", cacheName);
         } else {
            log.debugf(t, "Join request failed for cache %s", cacheName);
            if (t instanceof TimeoutException) {
               throw (TimeoutException) t;
            }
            throw (CacheJoinException) t.getCause();
         }

         // Can't use a value based on the state transfer timeout because cache org.infinispan.CONFIG
         // uses the default timeout, which is too long for tests (4 minutes)
         long delay = 100;
         return CompletionStages.scheduleNonBlocking(
               () -> sendJoinRequest(cacheName, joinInfo, timeout, endTime),
               timeoutExecutor, delay, MILLISECONDS);
      });
   }

   private CompletionStage<CacheStatusResponse> retryJoinInView(String cacheName, CacheJoinInfo joinInfo,
                                                                long timeout, long endTime, int viewId) {
      return withView(viewId, timeout, MILLISECONDS)
                   .thenCompose(v -> sendJoinRequest(cacheName, joinInfo, timeout, endTime));
   }

   public CompletionStage<CacheTopology> handleJoinResponse(String cacheName, LocalCacheStatus cacheStatus,
                                                            CacheStatusResponse initialStatus) {
      int viewId = transport.getViewId();
      return doHandleTopologyUpdate(cacheName, initialStatus.getCacheTopology(), initialStatus.getAvailabilityMode(),
                                    viewId, transport.getCoordinator(), cacheStatus)
                   .thenCompose(applied -> {
                      if (!applied) {
                         throw new IllegalStateException(
                               "We already had a newer topology by the time we received the join response");
                      }

                      if (cacheStatus.getJoinInfo().getPersistentUUID() != null) {
                         // Don't use the current CH state for the next restart
                         deleteCHState(cacheName);
                      }

                      return doHandleStableTopologyUpdate(cacheName, initialStatus.getStableTopology(), viewId,
                                                          transport.getCoordinator(), cacheStatus);
                   })
                   .thenApply(ignored -> initialStatus.getCacheTopology());
   }

   @Override
   public void leave(String cacheName, long timeout) {
      log.debugf("Node %s leaving cache %s", transport.getAddress(), cacheName);
      runningCaches.remove(cacheName);

      ReplicableCommand command = new CacheLeaveCommand(cacheName, transport.getAddress(), transport.getViewId());
      try {
         CompletionStages.join(helper.executeOnCoordinator(transport, command, timeout));
      } catch (Exception e) {
         log.debugf(e, "Error sending the leave request for cache %s to coordinator", cacheName);
      }
   }

   @Override
   public void confirmRebalancePhase(String cacheName, int topologyId, int rebalanceId, Throwable throwable) {
      try {
         // Note that if the coordinator changes again after we sent the command, we will get another
         // query for the status of our running caches. So we don't need to retry if the command failed.
         helper.executeOnCoordinatorAsync(transport,
               new RebalancePhaseConfirmCommand(cacheName, transport.getAddress(), throwable, topologyId, transport.getViewId()));
      } catch (Exception e) {
         log.debugf(e, "Error sending the rebalance completed notification for cache %s to the coordinator",
                    cacheName);
      }
   }

   // called by the coordinator
   @Override
   public CompletionStage<ManagerStatusResponse> handleStatusRequest(int viewId) {
      // As long as we have an older view, we can still process topologies from the old coordinator
      return withView(viewId, getGlobalTimeout(), MILLISECONDS).thenApply(ignored -> {
         Map<String, CacheStatusResponse> caches = new HashMap<>();
         synchronized (runningCaches) {
            latestStatusResponseViewId = viewId;

            for (Map.Entry<String, LocalCacheStatus> e : runningCaches.entrySet()) {
               String cacheName = e.getKey();
               LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
               // Ignore caches that haven't finished joining yet.
               // They will either wait for recovery to finish (if started in the current view)
               // or retry (if started in a previous view).
               if (cacheStatus.getCurrentTopology() == null)
                  continue;

               caches.put(e.getKey(), new CacheStatusResponse(cacheStatus.getJoinInfo(),
                                                              cacheStatus.getCurrentTopology(),
                                                              cacheStatus.getStableTopology(),
                                                              cacheStatus.getPartitionHandlingManager()
                                                                         .getAvailabilityMode()));
            }
         }

         log.debugf("Sending cluster status response for view %d", viewId);
         return new ManagerStatusResponse(caches, gcr.getClusterTopologyManager().isRebalancingEnabled());
      });
   }

   @Override
   public CompletionStage<Void> handleTopologyUpdate(final String cacheName, final CacheTopology cacheTopology,
                                                     final AvailabilityMode availabilityMode, final int viewId,
                                                     final Address sender) {
      if (!running) {
         log.tracef("Ignoring consistent hash update %s for cache %s, the local cache manager is not running",
                    cacheTopology.getTopologyId(), cacheName);
         return CompletableFutures.completedNull();
      }

      LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
      if (cacheStatus == null) {
         log.tracef("Ignoring consistent hash update %s for cache %s that doesn't exist locally",
                    cacheTopology.getTopologyId(), cacheName);
         return CompletableFutures.completedNull();
      }

      return withView(viewId, cacheStatus.getJoinInfo().getTimeout(), MILLISECONDS)
            .thenCompose(ignored -> orderOnCache(cacheName, () -> doHandleTopologyUpdate(cacheName, cacheTopology, availabilityMode, viewId, sender,
                                          cacheStatus)))
            .handle((ignored, throwable) -> {
               if (throwable != null && !(throwable instanceof IllegalLifecycleStateException)) {
                  log.topologyUpdateError(cacheName, throwable);
               }
               return null;
            });
   }

   /**
    * Update the cache topology in the LocalCacheStatus and pass it to the CacheTopologyHandler.
    *
    * @return {@code true} if the topology was applied, {@code false} if it was ignored.
    */
   private CompletionStage<Boolean> doHandleTopologyUpdate(String cacheName, CacheTopology cacheTopology,
                                                           AvailabilityMode availabilityMode, int viewId,
                                                           Address sender, LocalCacheStatus cacheStatus) {
      CacheTopology existingTopology;
      synchronized (cacheStatus) {
         if (cacheTopology == null) {
            // No topology yet: happens when a cache is being restarted from state.
            // Still, return true because we don't want to re-send the join request.
            return CompletableFutures.completedTrue();
         }
         // Register all persistent UUIDs locally
         registerPersistentUUID(cacheTopology);
         existingTopology = cacheStatus.getCurrentTopology();
         if (existingTopology != null && cacheTopology.getTopologyId() <= existingTopology.getTopologyId()) {
            log.debugf("Ignoring late consistent hash update for cache %s, current topology is %s: %s",
                       cacheName, existingTopology.getTopologyId(), cacheTopology);
            return CompletableFutures.completedFalse();
         }

         if (!updateCacheTopology(cacheName, cacheTopology, viewId, sender, cacheStatus))
            return CompletableFutures.completedFalse();
      }

      CacheTopologyHandler handler = cacheStatus.getHandler();
      ConsistentHash currentCH = cacheTopology.getCurrentCH();
      ConsistentHash pendingCH = cacheTopology.getPendingCH();
      ConsistentHash unionCH;
      if (pendingCH != null) {
         ConsistentHashFactory chf = cacheStatus.getJoinInfo().getConsistentHashFactory();
         switch (cacheTopology.getPhase()) {
            case READ_NEW_WRITE_ALL:
               // When removing members from topology, we have to make sure that the unionCH has
               // owners from pendingCH (which is used as the readCH in this phase) before
               // owners from currentCH, as primary owners must match in readCH and writeCH.
               unionCH = chf.union(pendingCH, currentCH);
               break;
            default:
               unionCH = chf.union(currentCH, pendingCH);
         }
      } else {
         unionCH = null;
      }

      List<PersistentUUID> persistentUUIDs = persistentUUIDManager.mapAddresses(cacheTopology.getActualMembers());
      CacheTopology unionTopology = new CacheTopology(cacheTopology.getTopologyId(), cacheTopology.getRebalanceId(),
                                                      currentCH, pendingCH, unionCH, cacheTopology.getPhase(),
                                                      cacheTopology.getActualMembers(), persistentUUIDs);
      boolean updateAvailabilityModeFirst = availabilityMode != AvailabilityMode.AVAILABLE;

      CompletionStage<Void> stage =
            resetLocalTopologyBeforeRebalance(cacheName, cacheTopology, existingTopology, handler);

      stage = stage.thenCompose(ignored -> {
         unionTopology.logRoutingTableInformation(cacheName);

         if (updateAvailabilityModeFirst && availabilityMode != null) {
            return cacheStatus.getPartitionHandlingManager().setAvailabilityMode(availabilityMode);
         }
         return CompletableFutures.completedNull();
      });

      stage = stage.thenCompose(ignored -> {
         boolean startConflictResolution =
               cacheTopology.getPhase() == CacheTopology.Phase.CONFLICT_RESOLUTION;
         if (!startConflictResolution && unionCH != null &&
             (existingTopology == null ||
              existingTopology.getRebalanceId() != cacheTopology.getRebalanceId())) {
            // This CH_UPDATE command was sent after a REBALANCE_START command, but arrived first.
            // We will start the rebalance now and ignore the REBALANCE_START command when it arrives.
            log.tracef("This topology update has a pending CH, starting the rebalance now");
            return handler.rebalance(unionTopology);
         } else {
            return handler.updateConsistentHash(unionTopology);
         }
      });

      if (!updateAvailabilityModeFirst) {
         stage = stage.thenCompose(ignored -> cacheStatus.getPartitionHandlingManager().setAvailabilityMode(availabilityMode));
      }
      return stage.thenApply(ignored -> true);
   }

   private void registerPersistentUUID(CacheTopology cacheTopology) {
      int count = cacheTopology.getActualMembers().size();
      for (int i = 0; i < count; i++) {
         persistentUUIDManager.addPersistentAddressMapping(
               cacheTopology.getActualMembers().get(i),
               cacheTopology.getMembersPersistentUUIDs().get(i)
         );
      }
   }

   private boolean updateCacheTopology(String cacheName, CacheTopology cacheTopology, int viewId,
                                       Address sender, LocalCacheStatus cacheStatus) {
      synchronized (runningCaches) {
         if (!validateCommandViewId(cacheTopology, viewId, sender, cacheName))
            return false;

         log.debugf("Updating local topology for cache %s: %s", cacheName, cacheTopology);
         cacheStatus.setCurrentTopology(cacheTopology);
         return true;
      }
   }

   /**
    * Synchronization is required to prevent topology updates while preparing the status response.
    */
   @GuardedBy("runningCaches")
   private boolean validateCommandViewId(CacheTopology cacheTopology, int viewId, Address sender,
                                         String cacheName) {
      if (!sender.equals(transport.getCoordinator())) {
         log.debugf("Ignoring topology %d for cache %s from old coordinator %s",
                    cacheTopology.getTopologyId(), cacheName, sender);
         return false;
      }
      if (viewId < latestStatusResponseViewId) {
         log.debugf(
               "Ignoring topology %d for cache %s from view %d received after status request from view %d",
               cacheTopology.getTopologyId(), cacheName, viewId, latestStatusResponseViewId);
         return false;
      }
      return true;
   }

   private CompletionStage<Void> resetLocalTopologyBeforeRebalance(String cacheName, CacheTopology newCacheTopology,
                                                                   CacheTopology oldCacheTopology, CacheTopologyHandler handler) {
      // Cannot rely on the pending CH, because it is also used for conflict resolution
      boolean newRebalance = newCacheTopology.getPhase() != CacheTopology.Phase.NO_REBALANCE &&
                             newCacheTopology.getPhase() != CacheTopology.Phase.CONFLICT_RESOLUTION;
      if (newRebalance) {
         // The initial topology doesn't need a reset because we are guaranteed not to be a member
         if (oldCacheTopology == null)
            return CompletableFutures.completedNull();

         // We only need a reset if we missed a topology update
         if (newCacheTopology.getTopologyId() <= oldCacheTopology.getTopologyId() + 1)
            return CompletableFutures.completedNull();

         // We have missed a topology update, and that topology might have removed some of our segments.
         // If this rebalance adds those same segments, we need to remove the old data/inbound transfers first.
         // This can happen when the coordinator changes, either because the old one left or because there was a merge,
         // and the rebalance after merge arrives before the merged topology update.
         if (newCacheTopology.getRebalanceId() != oldCacheTopology.getRebalanceId()) {
            // The currentCH changed, we need to install a "reset" topology with the new currentCH first
            registerPersistentUUID(newCacheTopology);
            CacheTopology resetTopology = new CacheTopology(newCacheTopology.getTopologyId() - 1,
                                                            newCacheTopology.getRebalanceId() - 1,
                                                            newCacheTopology.getCurrentCH(), null,
                                                            CacheTopology.Phase.NO_REBALANCE,
                                                            newCacheTopology.getActualMembers(), persistentUUIDManager
                                                                  .mapAddresses(
                                                                        newCacheTopology
                                                                              .getActualMembers()));
            log.debugf("Installing fake cache topology %s for cache %s", resetTopology, cacheName);
            return handler.updateConsistentHash(resetTopology);
         }
      }
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> handleStableTopologyUpdate(final String cacheName,
                                                           final CacheTopology newStableTopology,
                                                           final Address sender, final int viewId) {
      final LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
      if (cacheStatus != null) {
         return orderOnCache(cacheName, () -> doHandleStableTopologyUpdate(cacheName, newStableTopology, viewId, sender, cacheStatus));
      }
      return completedNull();
   }

   private CompletionStage<Void> doHandleStableTopologyUpdate(String cacheName, CacheTopology newStableTopology,
                                                              int viewId,
                                                              Address sender, LocalCacheStatus cacheStatus) {
      synchronized (runningCaches) {
         if (!validateCommandViewId(newStableTopology, viewId, sender, cacheName))
            return completedNull();

         CacheTopology stableTopology = cacheStatus.getStableTopology();
         if (stableTopology == null || stableTopology.getTopologyId() < newStableTopology.getTopologyId()) {
            log.tracef("Updating stable topology for cache %s: %s", cacheName, newStableTopology);
            cacheStatus.setStableTopology(newStableTopology);
         }
      }
      return completedNull();
   }

   @Override
   public CompletionStage<Void> handleRebalance(final String cacheName, final CacheTopology cacheTopology,
                                                final int viewId, final Address sender) {
      if (!running) {
         log.debugf("Ignoring rebalance request %s for cache %s, the local cache manager is not running",
                    cacheTopology.getTopologyId(), cacheName);
         return CompletableFutures.completedNull();
      }

      final LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
      if (cacheStatus == null) {
         log.tracef("Ignoring rebalance %s for cache %s that doesn't exist locally",
                    cacheTopology.getTopologyId(), cacheName);
         return CompletableFutures.completedNull();
      }

      eventLogger.context(cacheName)
            .info(EventLogCategory.LIFECYCLE, MESSAGES.cacheRebalanceStart(cacheTopology.getMembers(), cacheTopology.getPhase(), cacheTopology.getTopologyId()));
      return withView(viewId, cacheStatus.getJoinInfo().getTimeout(), MILLISECONDS)
            .thenCompose(ignored -> orderOnCache(cacheName, () -> {
               return doHandleRebalance(viewId, cacheStatus, cacheTopology, cacheName, sender);
            }))
            .handle((ignore, throwable) -> {
               Collection<Address> members = cacheTopology.getMembers();
               int topologyId = cacheTopology.getTopologyId();

               if (throwable != null) {
                  Throwable t = CompletableFutures.extractException(throwable);
                  // Ignore errors when the cache is shutting down
                  if (!(t instanceof IllegalLifecycleStateException)) {
                     log.rebalanceStartError(cacheName, throwable);
                     eventLogger.context(cacheName)
                           .error(EventLogCategory.LIFECYCLE, MESSAGES.rebalanceFinishedWithFailure(members, topologyId, t));
                  }
               } else {
                  eventLogger.context(cacheName)
                        .info(EventLogCategory.LIFECYCLE, MESSAGES.rebalanceFinished(members, topologyId));
               }

               return null;
            });
   }

   private CompletionStage<Void> doHandleRebalance(int viewId, LocalCacheStatus cacheStatus,
                                                   CacheTopology cacheTopology,
                                                   String cacheName, Address sender) {
      CacheTopology existingTopology;
      synchronized (cacheStatus) {
         existingTopology = cacheStatus.getCurrentTopology();
         if (existingTopology != null && cacheTopology.getTopologyId() <= existingTopology.getTopologyId()) {
            // Start rebalance commands are sent asynchronously to the entire cluster
            // So it's possible to receive an old one on a joiner after the joiner has already become a member.
            log.debugf("Ignoring old rebalance for cache %s, current topology is %s: %s", cacheName,
                       existingTopology.getTopologyId(), cacheTopology);
            return CompletableFutures.completedNull();
         }

         if (!updateCacheTopology(cacheName, cacheTopology, viewId, sender, cacheStatus))
            return CompletableFutures.completedNull();
      }

      CacheTopologyHandler handler = cacheStatus.getHandler();
      ConsistentHash unionCH = cacheStatus.getJoinInfo().getConsistentHashFactory().union(
            cacheTopology.getCurrentCH(), cacheTopology.getPendingCH());
      CacheTopology newTopology = new CacheTopology(cacheTopology.getTopologyId(), cacheTopology.getRebalanceId(),
                                                    cacheTopology.getCurrentCH(), cacheTopology.getPendingCH(), unionCH,
                                                    cacheTopology.getPhase(),
                                                    cacheTopology.getActualMembers(),
                                                    cacheTopology.getMembersPersistentUUIDs());

      CompletionStage<Void> stage =
            resetLocalTopologyBeforeRebalance(cacheName, cacheTopology, existingTopology, handler);

      return stage.thenCompose(ignored -> {
         log.debugf("Starting local rebalance for cache %s, topology = %s", cacheName, cacheTopology);
         cacheTopology.logRoutingTableInformation(cacheName);

         return handler.rebalance(newTopology);
      });
   }

   @Override
   public CacheTopology getCacheTopology(String cacheName) {
      LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
      return cacheStatus != null ? cacheStatus.getCurrentTopology() : null;
   }

   @Override
   public CacheTopology getStableCacheTopology(String cacheName) {
      LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
      return cacheStatus != null ? cacheStatus.getStableTopology() : null;
   }

   private CompletionStage<Void> withView(int viewId, long timeout, TimeUnit timeUnit) {
      CompletableFuture<Void> viewFuture = transport.withView(viewId);
      ScheduledFuture<Boolean> cancelTask = timeoutExecutor.schedule(
            () -> viewFuture.completeExceptionally(CLUSTER.timeoutWaitingForView(viewId, transport.getViewId())),
            timeout, timeUnit);
      viewFuture.whenComplete((v, throwable) -> cancelTask.cancel(false));
      return viewFuture;
   }

   @ManagedAttribute(description = "Rebalancing enabled", displayName = "Rebalancing enabled",
                     dataType = DataType.TRAIT, writable = true)
   @Override
   public boolean isRebalancingEnabled() {
      return isCacheRebalancingEnabled(null);
   }

   @Override
   public void setRebalancingEnabled(boolean enabled) {
      setCacheRebalancingEnabled(null, enabled);
   }

   @Override
   public boolean isCacheRebalancingEnabled(String cacheName) {
      int viewId = transport.getViewId();
      ReplicableCommand command = new RebalanceStatusRequestCommand(cacheName);
      RebalancingStatus status = (RebalancingStatus) CompletionStages.join(
            executeOnCoordinatorRetry(command, viewId, timeService.expectedEndTime(getGlobalTimeout(), MILLISECONDS)));
      return status != RebalancingStatus.SUSPENDED;
   }

   public CompletionStage<Object> executeOnCoordinatorRetry(ReplicableCommand command, int viewId, long endNanos) {
      long remainingMillis = timeService.remainingTime(endNanos, MILLISECONDS);
      return CompletionStages.handleAndCompose(
            helper.executeOnCoordinator(transport, command, remainingMillis),
            (o, throwable) -> {
               if (throwable == null) {
                  return CompletableFuture.completedFuture(o);
               }

               Throwable t = CompletableFutures.extractException(throwable);
               if (t instanceof SuspectException) {
                  if (log.isTraceEnabled()) log.tracef("Coordinator left the cluster while querying rebalancing status, retrying");
                  int newViewId = Math.max(viewId + 1, transport.getViewId());
                  return executeOnCoordinatorRetry(command, newViewId, endNanos);
               } else {
                  return CompletableFutures.completedExceptionFuture(t);
               }
            });
   }

   @Override
   public void setCacheRebalancingEnabled(String cacheName, boolean enabled) {
      ReplicableCommand command = new RebalancePolicyUpdateCommand(cacheName, enabled);
      CompletionStages.join(helper.executeOnClusterSync(transport, command, getGlobalTimeout(),
                                                        VoidResponseCollector.ignoreLeavers()));
   }

   @Override
   public RebalancingStatus getRebalancingStatus(String cacheName) {
      ReplicableCommand command = new RebalanceStatusRequestCommand(cacheName);
      return (RebalancingStatus) CompletionStages.join(
            executeOnCoordinatorRetry(command, transport.getViewId(), timeService.expectedEndTime(getGlobalTimeout(), MILLISECONDS)));
   }

   @ManagedAttribute(description = "Cluster availability", displayName = "Cluster availability",
                     dataType = DataType.TRAIT, writable = false)
   public String getClusterAvailability() {
      AvailabilityMode clusterAvailability = AvailabilityMode.AVAILABLE;
      synchronized (runningCaches) {
         for (LocalCacheStatus cacheStatus : runningCaches.values()) {
            AvailabilityMode availabilityMode = cacheStatus.getPartitionHandlingManager().getAvailabilityMode();
            clusterAvailability = clusterAvailability.min(availabilityMode);
         }
      }
      return clusterAvailability.toString();
   }

   @Override
   public AvailabilityMode getCacheAvailability(String cacheName) {
      LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
      return cacheStatus.getPartitionHandlingManager().getAvailabilityMode();
   }

   @Override
   public void setCacheAvailability(String cacheName, AvailabilityMode availabilityMode) {
      ReplicableCommand command = new CacheAvailabilityUpdateCommand(cacheName, availabilityMode);
      CompletionStages.join(helper.executeOnCoordinator(transport, command, getGlobalTimeout()));
   }

   @Override
   public void cacheShutdown(String name) {
      ReplicableCommand command = new CacheShutdownRequestCommand(name);
      CompletionStages.join(helper.executeOnCoordinator(transport, command, getGlobalTimeout()));
   }

   @Override
   public CompletionStage<Void> handleCacheShutdown(String cacheName) {
      // The cache has shutdown, write the CH state
      writeCHState(cacheName);
      return completedNull();
   }

   private void writeCHState(String cacheName) {
      ScopedPersistentState cacheState = new ScopedPersistentStateImpl(cacheName);
      cacheState.setProperty(GlobalStateManagerImpl.VERSION, Version.getVersion());
      cacheState.setProperty(GlobalStateManagerImpl.TIMESTAMP, timeService.instant().toString());
      cacheState.setProperty(GlobalStateManagerImpl.VERSION_MAJOR, Version.getMajor());
      LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
      ConsistentHash remappedCH = cacheStatus.getCurrentTopology().getCurrentCH()
                                             .remapAddresses(persistentUUIDManager.addressToPersistentUUID());
      remappedCH.toScopedState(cacheState);
      globalStateManager.writeScopedState(cacheState);
      if (log.isTraceEnabled()) log.tracef("Written CH state for cache %s, checksum=%s: %s", cacheName, cacheState.getChecksum(), remappedCH);
   }

   private void deleteCHState(String cacheName) {
      globalStateManager.deleteScopedState(cacheName);
      if (log.isTraceEnabled()) log.tracef("Removed CH state for cache %s", cacheName);
   }

   private int getGlobalTimeout() {
      // TODO Rename setting to something like globalRpcTimeout
      return (int) gcr.getGlobalConfiguration().transport().distributedSyncTimeout();
   }

   @Override
   public void prepareForPersist(ScopedPersistentState state) {
      if (persistentUUID != null) {
         state.setProperty("uuid", persistentUUID.toString());
      }
   }

   @Override
   public void prepareForRestore(ScopedPersistentState state) {
      if (!state.containsProperty("uuid")) {
         throw CONFIG.invalidPersistentState(ScopedPersistentState.GLOBAL_SCOPE);
      }
      persistentUUID = PersistentUUID.fromString(state.getProperty("uuid"));
   }

   @Override
   public PersistentUUID getPersistentUUID() {
      return persistentUUID;
   }

   private <T> CompletionStage<T> orderOnCache(String cacheName, Callable<CompletionStage<T>> action) {
      return actionSequencer.orderOnKey(cacheName, () -> {
         log.tracef("Acquired cache status %s", cacheName);
         return action.call().whenComplete((v, t) -> log.tracef("Released cache status %s", cacheName));
      });
   }
}

class LocalCacheStatus {
   private final CacheJoinInfo joinInfo;
   private final CacheTopologyHandler handler;
   private final PartitionHandlingManager partitionHandlingManager;
   private volatile CacheTopology currentTopology;
   private volatile CacheTopology stableTopology;

   LocalCacheStatus(CacheJoinInfo joinInfo, CacheTopologyHandler handler,
         PartitionHandlingManager phm) {
      this.joinInfo = joinInfo;
      this.handler = handler;
      this.partitionHandlingManager = phm;
   }

   public CacheJoinInfo getJoinInfo() {
      return joinInfo;
   }

   public CacheTopologyHandler getHandler() {
      return handler;
   }

   public PartitionHandlingManager getPartitionHandlingManager() {
      return partitionHandlingManager;
   }

   CacheTopology getCurrentTopology() {
      return currentTopology;
   }

   void setCurrentTopology(CacheTopology currentTopology) {
      this.currentTopology = currentTopology;
   }

   CacheTopology getStableTopology() {
      return stableTopology;
   }

   void setStableTopology(CacheTopology stableTopology) {
      this.stableTopology = stableTopology;
      partitionHandlingManager.onTopologyUpdate(currentTopology);
   }
}
