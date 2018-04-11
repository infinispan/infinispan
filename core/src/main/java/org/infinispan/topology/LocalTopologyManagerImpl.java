package org.infinispan.topology;

import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.IllegalLifecycleStateException;
import org.infinispan.Version;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.NotSerializableException;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.executors.LimitedExecutor;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.globalstate.GlobalStateManager;
import org.infinispan.globalstate.GlobalStateProvider;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.globalstate.impl.GlobalStateManagerImpl;
import org.infinispan.globalstate.impl.ScopedPersistentStateImpl;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import net.jcip.annotations.GuardedBy;

/**
 * The {@code LocalTopologyManager} implementation.
 *
 * @author Dan Berindei
 * @since 5.2
 */
@MBean(objectName = "LocalTopologyManager", description = "Controls the cache membership and state transfer")
public class LocalTopologyManagerImpl implements LocalTopologyManager, GlobalStateProvider {
   private static Log log = LogFactory.getLog(LocalTopologyManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   @Inject private Transport transport;
   @Inject @ComponentName(ASYNC_TRANSPORT_EXECUTOR)
   private ExecutorService asyncTransportExecutor;
   @Inject private GlobalComponentRegistry gcr;
   @Inject private TimeService timeService;
   @Inject private GlobalStateManager globalStateManager;
   @Inject private PersistentUUIDManager persistentUUIDManager;

   private final WithinThreadExecutor withinThreadExecutor = new WithinThreadExecutor();

   // We synchronize on the entire map while handling a status request, to make sure there are no concurrent topology
   // updates from the old coordinator.
   private final Map<String, LocalCacheStatus> runningCaches =
         Collections.synchronizedMap(new HashMap<>());
   private volatile boolean running;
   @GuardedBy("runningCaches")
   private int latestStatusResponseViewId;
   private PersistentUUID persistentUUID;

   // This must be invoked before GlobalStateManagerImpl.start
   @Start(priority = 0)
   public void preStart() {
      if (globalStateManager != null) {
         globalStateManager.registerStateProvider(this);
      }
   }

   // Arbitrary value, only need to start after the (optional) GlobalStateManager and JGroupsTransport
   @Start(priority = 100)
   // Start isn't called with any locks, but it runs before the component is accessible from other threads
   @GuardedBy("runningCaches")
   public void start() {
      if (trace) {
         log.tracef("Starting LocalTopologyManager on %s", transport.getAddress());
      }
      if (persistentUUID == null) {
         persistentUUID = PersistentUUID.randomUUID();
         globalStateManager.writeGlobalState();
      }
      persistentUUIDManager.addPersistentAddressMapping(transport.getAddress(), persistentUUID);
      running = true;
      latestStatusResponseViewId = transport.getViewId();
   }

   // Need to stop after ClusterTopologyManagerImpl and before the JGroupsTransport
   @Stop(priority = 110)
   public void stop() {
      if (trace) {
         log.tracef("Stopping LocalTopologyManager on %s", transport.getAddress());
      }
      running = false;
      for (LocalCacheStatus cache : runningCaches.values()) {
         cache.getTopologyUpdatesExecutor().shutdownNow();
      }
      withinThreadExecutor.shutdown();
   }

   @Override
   public CacheTopology join(String cacheName, CacheJoinInfo joinInfo, CacheTopologyHandler stm,
         PartitionHandlingManager phm) throws Exception {
      log.debugf("Node %s joining cache %s", transport.getAddress(), cacheName);

      // For Total Order caches, we must not move the topology updates to another thread
      ExecutorService topologyUpdatesExecutor = joinInfo.isTotalOrder() ? withinThreadExecutor : asyncTransportExecutor;
      LocalCacheStatus cacheStatus = new LocalCacheStatus(cacheName, joinInfo, stm, phm, topologyUpdatesExecutor);

      // Pretend the join is using up a thread from the topology updates executor.
      // This ensures that the initial topology and the GET_CACHE_LISTENERS request will happen on this thread,
      // and other topology updates are only handled after we complete joinFuture.
      CompletableFuture<Void> joinFuture = new CompletableFuture<>();
      cacheStatus.getTopologyUpdatesExecutor().executeAsync(() -> joinFuture);

      runningCaches.put(cacheName, cacheStatus);

      long timeout = joinInfo.getTimeout();
      long endTime = timeService.expectedEndTime(timeout, TimeUnit.MILLISECONDS);
      try {
         while (true) {
            int viewId = transport.getViewId();
            try {
               ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
                     CacheTopologyControlCommand.Type.JOIN, transport.getAddress(), joinInfo, viewId);
               CacheStatusResponse initialStatus = (CacheStatusResponse) executeOnCoordinator(command, timeout);
               if (initialStatus == null) {
                  log.debug("Ignoring null join response, coordinator is probably shutting down");
                  waitForView(viewId + 1, cacheStatus.getJoinInfo().getTimeout(), TimeUnit.MILLISECONDS);
                  continue;
               }

               if (!doHandleTopologyUpdate(cacheName, initialStatus.getCacheTopology(),
                                           initialStatus.getAvailabilityMode(), viewId, transport.getCoordinator(),
                                           cacheStatus)) {
                  throw new IllegalStateException(
                        "We already had a newer topology by the time we received the join response");
               }

               doHandleStableTopologyUpdate(cacheName, initialStatus.getStableTopology(), viewId,
                                            transport.getCoordinator(), cacheStatus);
               return initialStatus.getCacheTopology();
            } catch (NotSerializableException e) {
               // There's no point in retrying if the cache join info is not serializable
               throw new CacheJoinException(e);
            } catch (Exception e) {
               log.debugf(e, "Error sending join request for cache %s to coordinator", cacheName);
               if (e.getCause() != null && e.getCause() instanceof CacheJoinException) {
                  throw (CacheJoinException)e.getCause();
               }
               if (timeService.isTimeExpired(endTime)) {
                  throw e;
               }
               // TODO Add some configuration for this, or use a fraction of state transfer timeout
               Thread.sleep(100);
            }
         }
      } finally {
         joinFuture.complete(null);
      }
   }

   @Override
   public void leave(String cacheName) {
      log.debugf("Node %s leaving cache %s", transport.getAddress(), cacheName);
      LocalCacheStatus cacheStatus = runningCaches.remove(cacheName);

      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
            CacheTopologyControlCommand.Type.LEAVE, transport.getAddress(), transport.getViewId());
      try {
         executeOnCoordinator(command, cacheStatus.getJoinInfo().getTimeout());
      } catch (Exception e) {
         log.debugf(e, "Error sending the leave request for cache %s to coordinator", cacheName);
      }
   }

   @Override
   public void confirmRebalancePhase(String cacheName, int topologyId, int rebalanceId, Throwable throwable) {
      // Note that if the coordinator changes again after we sent the command, we will get another
      // query for the status of our running caches. So we don't need to retry if the command failed.
      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
            CacheTopologyControlCommand.Type.REBALANCE_PHASE_CONFIRM, transport.getAddress(),
            topologyId, rebalanceId, throwable, transport.getViewId());
      try {
         executeOnCoordinatorAsync(command);
      } catch (Exception e) {
         log.debugf(e, "Error sending the rebalance completed notification for cache %s to the coordinator",
               cacheName);
      }
   }

   // called by the coordinator
   @Override
   public ManagerStatusResponse handleStatusRequest(int viewId) {
      try {
         // As long as we have an older view, we can still process topologies from the old coordinator
         waitForView(viewId, getGlobalTimeout(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
         // Shutting down, send back an empty status
         Thread.currentThread().interrupt();
         return new ManagerStatusResponse(Collections.emptyMap(), true);
      }

      Map<String, CacheStatusResponse> caches = new HashMap<>();
      synchronized (runningCaches) {
         latestStatusResponseViewId = viewId;

         for (Map.Entry<String, LocalCacheStatus> e : runningCaches.entrySet()) {
            String cacheName = e.getKey();
            LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
            caches.put(e.getKey(), new CacheStatusResponse(cacheStatus.getJoinInfo(),
                    cacheStatus.getCurrentTopology(), cacheStatus.getStableTopology(),
                    cacheStatus.getPartitionHandlingManager().getAvailabilityMode()));
         }
      }

      boolean rebalancingEnabled = true;
      // Avoid adding a direct dependency to the ClusterTopologyManager
      ReplicableCommand command = new CacheTopologyControlCommand(null,
            CacheTopologyControlCommand.Type.POLICY_GET_STATUS, transport.getAddress(),
            transport.getViewId());
      try {
         gcr.wireDependencies(command);
         SuccessfulResponse response = (SuccessfulResponse) command.invoke();
         rebalancingEnabled = (Boolean) response.getResponseValue();
      } catch (Throwable t) {
         log.warn("Failed to obtain the rebalancing status", t);
      }
      log.debugf("Sending cluster status response for view %d", viewId);
      return new ManagerStatusResponse(caches, rebalancingEnabled);
   }

   @Override
   public void handleTopologyUpdate(final String cacheName, final CacheTopology cacheTopology,
         final AvailabilityMode availabilityMode, final int viewId, final Address sender) throws InterruptedException {
      if (!running) {
         log.tracef("Ignoring consistent hash update %s for cache %s, the local cache manager is not running",
                    cacheTopology.getTopologyId(), cacheName);
         return;
      }

      LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
      if (cacheStatus == null) {
         log.tracef("Ignoring consistent hash update %s for cache %s that doesn't exist locally",
                    cacheTopology.getTopologyId(), cacheName);
         return;
      }

      cacheStatus.getTopologyUpdatesExecutor().execute(() -> {
         try {
            doHandleTopologyUpdate(cacheName, cacheTopology, availabilityMode, viewId, sender, cacheStatus);
         } catch (Throwable t) {
            log.topologyUpdateError(cacheName, t);
         }
      });
   }

   /**
    * Update the cache topology in the LocalCacheStatus and pass it to the CacheTopologyHandler.
    *
    * @return {@code true} if the topology was applied, {@code false} if it was ignored.
    */
   private boolean doHandleTopologyUpdate(String cacheName, CacheTopology cacheTopology,
                                          AvailabilityMode availabilityMode, int viewId, Address sender,
                                          LocalCacheStatus cacheStatus) {
      try {
         waitForView(viewId, cacheStatus.getJoinInfo().getTimeout(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
         // Shutting down, ignore the exception and the rebalance
         return false;
      }

      synchronized (cacheStatus) {
         if (cacheTopology == null) {
            // No topology yet: happens when a cache is being restarted from state.
            // Still, return true because we don't want to re-send the join request.
            return true;
         }
         // Register all persistent UUIDs locally
         registerPersistentUUID(cacheTopology);
         CacheTopology existingTopology = cacheStatus.getCurrentTopology();
         if (existingTopology != null && cacheTopology.getTopologyId() <= existingTopology.getTopologyId()) {
            log.debugf("Ignoring late consistent hash update for cache %s, current topology is %s: %s",
                  cacheName, existingTopology.getTopologyId(), cacheTopology);
            return false;
         }

         if (!updateCacheTopology(cacheName, cacheTopology, viewId, sender, cacheStatus))
            return false;

         CacheTopologyHandler handler = cacheStatus.getHandler();
         resetLocalTopologyBeforeRebalance(cacheName, cacheTopology, existingTopology, handler);

         ConsistentHash currentCH = cacheTopology.getCurrentCH();
         ConsistentHash pendingCH = cacheTopology.getPendingCH();
         ConsistentHash unionCH = null;
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
         }

         CacheTopology unionTopology = new CacheTopology(cacheTopology.getTopologyId(), cacheTopology.getRebalanceId(),
               currentCH, pendingCH, unionCH, cacheTopology.getPhase(),
               cacheTopology.getActualMembers(), persistentUUIDManager.mapAddresses(cacheTopology.getActualMembers()));
         unionTopology.logRoutingTableInformation();

         boolean updateAvailabilityModeFirst = availabilityMode != AvailabilityMode.AVAILABLE;
         if (updateAvailabilityModeFirst && availabilityMode != null) {
            cacheStatus.getPartitionHandlingManager().setAvailabilityMode(availabilityMode);
         }

         boolean startConflictResolution = cacheTopology.getPhase() == CacheTopology.Phase.CONFLICT_RESOLUTION;
         if (!startConflictResolution && (existingTopology == null || existingTopology.getRebalanceId() != cacheTopology.getRebalanceId())
               && unionCH != null) {
            // This CH_UPDATE command was sent after a REBALANCE_START command, but arrived first.
            // We will start the rebalance now and ignore the REBALANCE_START command when it arrives.
            log.tracef("This topology update has a pending CH, starting the rebalance now");
            handler.rebalance(unionTopology);
         } else {
            handler.updateConsistentHash(unionTopology);
         }

         if (!updateAvailabilityModeFirst) {
            cacheStatus.getPartitionHandlingManager().setAvailabilityMode(availabilityMode);
         }
         return true;
      }
   }

   private void registerPersistentUUID(CacheTopology cacheTopology) {
      int count = cacheTopology.getActualMembers().size();
      for(int i = 0; i < count; i++) {
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

   private void resetLocalTopologyBeforeRebalance(String cacheName, CacheTopology newCacheTopology,
         CacheTopology oldCacheTopology, CacheTopologyHandler handler) {
      // Cannot rely on the pending CH, because it is also used for conflict resolution
      boolean newRebalance = newCacheTopology.getPhase() != CacheTopology.Phase.NO_REBALANCE &&
                             newCacheTopology.getPhase() != CacheTopology.Phase.CONFLICT_RESOLUTION;
      if (newRebalance) {
         // The initial topology doesn't need a reset because we are guaranteed not to be a member
         if (oldCacheTopology == null)
            return;

         // We only need a reset if we missed a topology update
         if (newCacheTopology.getTopologyId() <= oldCacheTopology.getTopologyId() + 1)
            return;

         // We have missed a topology update, and that topology might have removed some of our segments.
         // If this rebalance adds those same segments, we need to remove the old data/inbound transfers first.
         // This can happen when the coordinator changes, either because the old one left or because there was a merge,
         // and the rebalance after merge arrives before the merged topology update.
         if (newCacheTopology.getRebalanceId() != oldCacheTopology.getRebalanceId()) {
            // The currentCH changed, we need to install a "reset" topology with the new currentCH first
            registerPersistentUUID(newCacheTopology);
            CacheTopology resetTopology = new CacheTopology(newCacheTopology.getTopologyId() - 1,
                  newCacheTopology.getRebalanceId() - 1, newCacheTopology.getCurrentCH(), null,
                  CacheTopology.Phase.NO_REBALANCE, newCacheTopology.getActualMembers(), persistentUUIDManager.mapAddresses(newCacheTopology.getActualMembers()));
            log.debugf("Installing fake cache topology %s for cache %s", resetTopology, cacheName);
            handler.updateConsistentHash(resetTopology);
         }
      }
   }

   @Override
   public void handleStableTopologyUpdate(final String cacheName, final CacheTopology newStableTopology,
         final Address sender, final int viewId) {
      final LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
      if (cacheStatus != null) {
         cacheStatus.getTopologyUpdatesExecutor().execute(
               () -> doHandleStableTopologyUpdate(cacheName, newStableTopology, viewId, sender, cacheStatus));
      }
   }

   private void doHandleStableTopologyUpdate(String cacheName, CacheTopology newStableTopology, int viewId,
                                             Address sender, LocalCacheStatus cacheStatus) {
      synchronized (runningCaches) {
         if (!validateCommandViewId(newStableTopology, viewId, sender, cacheName))
            return;

         CacheTopology stableTopology = cacheStatus.getStableTopology();
         if (stableTopology == null || stableTopology.getTopologyId() < newStableTopology.getTopologyId()) {
            log.tracef("Updating stable topology for cache %s: %s", cacheName, newStableTopology);
            cacheStatus.setStableTopology(newStableTopology);
         }
      }
   }

   @Override
   public void handleRebalance(final String cacheName, final CacheTopology cacheTopology, final int viewId,
         final Address sender) throws InterruptedException {
      if (!running) {
         log.debugf("Ignoring rebalance request %s for cache %s, the local cache manager is not running",
               cacheTopology.getTopologyId(), cacheName);
         return;
      }

      final LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
      if (cacheStatus == null) {
         log.tracef("Ignoring rebalance %s for cache %s that doesn't exist locally",
               cacheTopology.getTopologyId(), cacheName);
         return;
      }

      cacheStatus.getTopologyUpdatesExecutor().execute(() -> {
         try {
            doHandleRebalance(viewId, cacheStatus, cacheTopology, cacheName, sender);
         } catch (IllegalLifecycleStateException e) {
            // Ignore errors when the cache is shutting down
         } catch (Throwable t) {
            log.rebalanceStartError(cacheName, t);
         }
      });
   }

   private void doHandleRebalance(int viewId, LocalCacheStatus cacheStatus, CacheTopology cacheTopology,
                                  String cacheName, Address sender) {
      try {
         waitForView(viewId, cacheStatus.getJoinInfo().getTimeout(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
         // Shutting down, ignore the exception and the rebalance
         return;
      }

      synchronized (cacheStatus) {
         CacheTopology existingTopology = cacheStatus.getCurrentTopology();
         if (existingTopology != null && cacheTopology.getTopologyId() <= existingTopology.getTopologyId()) {
            // Start rebalance commands are sent asynchronously to the entire cluster
            // So it's possible to receive an old one on a joiner after the joiner has already become a member.
            log.debugf("Ignoring old rebalance for cache %s, current topology is %s: %s", cacheName,
                  existingTopology.getTopologyId(), cacheTopology);
            return;
         }

         if (!updateCacheTopology(cacheName, cacheTopology, viewId, sender, cacheStatus))
            return;

         log.debugf("Starting local rebalance for cache %s, topology = %s", cacheName, cacheTopology);
         cacheTopology.logRoutingTableInformation();

         CacheTopologyHandler handler = cacheStatus.getHandler();
         resetLocalTopologyBeforeRebalance(cacheName, cacheTopology, existingTopology, handler);

         ConsistentHash unionCH = cacheStatus.getJoinInfo().getConsistentHashFactory().union(
               cacheTopology.getCurrentCH(), cacheTopology.getPendingCH());
         CacheTopology newTopology = new CacheTopology(cacheTopology.getTopologyId(), cacheTopology.getRebalanceId(),
               cacheTopology.getCurrentCH(), cacheTopology.getPendingCH(), unionCH, cacheTopology.getPhase(),
               cacheTopology.getActualMembers(), cacheTopology.getMembersPersistentUUIDs());
         handler.rebalance(newTopology);
      }
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

   @Override
   public boolean isTotalOrderCache(String cacheName) {
      if (!running) {
         log.tracef("isTotalOrderCache(%s) returning false because the local cache manager is not running", cacheName);
         return false;
      }
      LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
      if (cacheStatus == null) {
         log.tracef("isTotalOrderCache(%s) returning false because the cache doesn't exist locally", cacheName);
         return false;
      }
      boolean totalOrder = cacheStatus.getJoinInfo().isTotalOrder();
      log.tracef("isTotalOrderCache(%s) returning %s", cacheName, totalOrder);
      return totalOrder;
   }

   private void waitForView(int viewId, long timeout, TimeUnit timeUnit) throws InterruptedException {
      try {
         transport.withView(viewId).get(timeout, timeUnit);
      } catch (ExecutionException e) {
         // The view future should never complete with an exception
         throw new CacheException(e.getCause());
      } catch (TimeoutException e) {
         throw log.timeoutWaitingForView(viewId, transport.getViewId());
      }
   }

   @ManagedAttribute(description = "Rebalancing enabled", displayName = "Rebalancing enabled",
         dataType = DataType.TRAIT, writable = true)
   @Override
   public boolean isRebalancingEnabled() throws Exception {
      return isCacheRebalancingEnabled(null);
   }

   @Override
   public void setRebalancingEnabled(boolean enabled) throws Exception {
      setCacheRebalancingEnabled(null, enabled);
   }

   @Override
   public boolean isCacheRebalancingEnabled(String cacheName) throws Exception {
      int viewId = transport.getViewId();
      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
                                                                  CacheTopologyControlCommand.Type.POLICY_GET_STATUS,
                                                                  transport.getAddress(), viewId);
      return executeOnCoordinatorRetry(command, viewId);
   }

   public <T> T executeOnCoordinatorRetry(ReplicableCommand command, int viewId) throws Exception {
      boolean retried = false;
      long endNanos = timeService.expectedEndTime(getGlobalTimeout(), TimeUnit.MILLISECONDS);
      while (true) {
         try {
            long remainingMillis = timeService.remainingTime(endNanos, TimeUnit.MILLISECONDS);
            return (T) executeOnCoordinator(command, remainingMillis);
         } catch (SuspectException e) {
            if (trace)
               log.tracef("Coordinator left the cluster while querying rebalancing status, retrying");
            // The view information in JGroupsTransport is not updated atomically, so we could have sent the request
            // to the old coordinator. We work around this by sending the request a second time, with the same view id.
            if (retried) {
               viewId = Math.max(viewId + 1, transport.getViewId());
               long remainingNanos = timeService.remainingTime(endNanos, TimeUnit.NANOSECONDS);
               waitForView(viewId, remainingNanos, TimeUnit.NANOSECONDS);
               retried = false;
            } else {
               retried = true;
            }
         }
      }
   }

   @Override
   public void setCacheRebalancingEnabled(String cacheName, boolean enabled) throws Exception {
      CacheTopologyControlCommand.Type type = enabled ? CacheTopologyControlCommand.Type.POLICY_ENABLE
            : CacheTopologyControlCommand.Type.POLICY_DISABLE;
      ReplicableCommand command = new CacheTopologyControlCommand(cacheName, type, transport.getAddress(),
            transport.getViewId());
      executeOnClusterSync(command, getGlobalTimeout(), false, false);
   }

   @Override
   public RebalancingStatus getRebalancingStatus(String cacheName) throws Exception {
      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
                                                                  CacheTopologyControlCommand.Type.REBALANCING_GET_STATUS,
                                                                  transport.getAddress(), transport.getViewId());
      int viewId = transport.getViewId();
      return executeOnCoordinatorRetry(command, viewId);
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
   public void setCacheAvailability(String cacheName, AvailabilityMode availabilityMode) throws Exception {
      CacheTopologyControlCommand.Type type = CacheTopologyControlCommand.Type.AVAILABILITY_MODE_CHANGE;
      ReplicableCommand command = new CacheTopologyControlCommand(cacheName, type, transport.getAddress(),
            availabilityMode, transport.getViewId());
      executeOnCoordinator(command, getGlobalTimeout());
   }

   @Override
   public void cacheShutdown(String name) throws Exception {
      ReplicableCommand command = new CacheTopologyControlCommand(name, CacheTopologyControlCommand.Type.SHUTDOWN_REQUEST, transport.getAddress(), transport.getViewId());
      executeOnCoordinator(command, getGlobalTimeout());
   }

   @Override
   public void handleCacheShutdown(String cacheName) {
      ComponentRegistry cr = gcr.getNamedComponentRegistry(cacheName);
      // Perform any orderly shutdown operations here
      PassivationManager passivationManager = cr.getComponent(PassivationManager.class);
      if (passivationManager != null) {
         passivationManager.passivateAll();
      }

      // The cache has shutdown, write the CH state
      ScopedPersistentState cacheState = new ScopedPersistentStateImpl(cacheName);
      cacheState.setProperty(GlobalStateManagerImpl.VERSION, Version.getVersion());
      cacheState.setProperty(GlobalStateManagerImpl.TIMESTAMP, timeService.instant().toString());
      cacheState.setProperty(GlobalStateManagerImpl.VERSION_MAJOR, Version.getMajor());
      LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
      cacheStatus.getCurrentTopology().getCurrentCH().remapAddresses(persistentUUIDManager.addressToPersistentUUID()).toScopedState(cacheState);
      globalStateManager.writeScopedState(cacheState);
   }

   private Object executeOnCoordinator(ReplicableCommand command, long timeout) throws Exception {
      Response response;
      if (transport.isCoordinator()) {
         try {
            if (trace) log.tracef("Attempting to execute command on self: %s", command);
            gcr.wireDependencies(command);
            response = (Response) command.invoke();
         } catch (Throwable t) {
            throw new CacheException("Error handling join request", t);
         }
      } else {
         // this node is not the coordinator
         Address coordinator = transport.getCoordinator();
         Map<Address, Response> responseMap = transport.invokeRemotely(Collections.singleton(coordinator),
               command, ResponseMode.SYNCHRONOUS, timeout, null, DeliverOrder.NONE, false);
         response = responseMap.get(coordinator);
      }
      if (response == null || !response.isSuccessful()) {
         Throwable exception = response instanceof ExceptionResponse
               ? ((ExceptionResponse)response).getException() : null;
         throw new CacheException("Bad response received from coordinator: " + response, exception);
      }
      return ((SuccessfulResponse) response).getResponseValue();
   }

   private void executeOnCoordinatorAsync(final ReplicableCommand command) throws Exception {
      // if we are the coordinator, the execution is actually synchronous
      if (transport.isCoordinator()) {
         asyncTransportExecutor.execute(() -> {
            if (trace) log.tracef("Attempting to execute command on self: %s", command);
            try {
               gcr.wireDependencies(command);
               command.invoke();
            } catch (Throwable t) {
               log.errorf(t, "Failed to execute ReplicableCommand %s on coordinator async: %s", command, t.getMessage());
            }
         });
      } else {
         Address coordinator = transport.getCoordinator();
         // ignore the responses
         transport.invokeRemotely(Collections.singleton(coordinator), command,
                                  ResponseMode.ASYNCHRONOUS, 0, null, DeliverOrder.NONE, false);
      }
   }

   private Map<Address, Object> executeOnClusterSync(final ReplicableCommand command, final int timeout,
                                                     boolean totalOrder, boolean distributed)
         throws Exception {
      // first invoke remotely

      if (totalOrder) {
         Map<Address, Response> responseMap = transport.invokeRemotely(null, command,
               ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS,
               timeout, null, DeliverOrder.TOTAL, distributed);
         return parseResponses(responseMap);
      }

      Future<Map<Address, Response>> remoteFuture = transport.invokeRemotelyAsync(null, command,
                  ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, timeout, null, DeliverOrder.NONE, false);

      // invoke the command on the local node
      gcr.wireDependencies(command);
      Response localResponse;
      try {
         if (trace) log.tracef("Attempting to execute command on self: %s", command);
         localResponse = (Response) command.invoke();
      } catch (Throwable throwable) {
         throw new Exception(throwable);
      }
      if (!localResponse.isSuccessful()) {
         throw new CacheException("Unsuccessful local response");
      }

      // wait for the remote commands to finish
      Map<Address, Response> responseMap = remoteFuture.get(timeout, TimeUnit.MILLISECONDS);

      // parse the responses
      Map<Address, Object> responseValues = parseResponses(responseMap);

      responseValues.put(transport.getAddress(), ((SuccessfulResponse) localResponse).getResponseValue());

      return responseValues;
   }

   private Map<Address, Object> parseResponses(Map<Address, Response> responseMap) {
      Map<Address, Object> responseValues = new HashMap<>(transport.getMembers().size());
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
         throw log.invalidPersistentState(ScopedPersistentState.GLOBAL_SCOPE);
      }
      persistentUUID = PersistentUUID.fromString(state.getProperty("uuid"));
   }

   @Override
   public PersistentUUID getPersistentUUID() {
      return persistentUUID;
   }
}

class LocalCacheStatus {
   private final CacheJoinInfo joinInfo;
   private final CacheTopologyHandler handler;
   private final PartitionHandlingManager partitionHandlingManager;
   private volatile CacheTopology currentTopology;
   private volatile CacheTopology stableTopology;
   private final LimitedExecutor topologyUpdatesExecutor;

   LocalCacheStatus(String cacheName, CacheJoinInfo joinInfo, CacheTopologyHandler handler,
                    PartitionHandlingManager phm, ExecutorService executor) {
      this.joinInfo = joinInfo;
      this.handler = handler;
      this.partitionHandlingManager = phm;

      this.topologyUpdatesExecutor = new LimitedExecutor("Topology-" + cacheName, executor, 1);
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

   LimitedExecutor getTopologyUpdatesExecutor() {
      return topologyUpdatesExecutor;
   }
}
