package org.infinispan.topology;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.executors.SemaphoreCompletionService;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;

/**
 * The {@code LocalTopologyManager} implementation.
 *
 * @author Dan Berindei
 * @since 5.2
 */
@MBean(objectName = "LocalTopologyManager", description = "Controls the cache membership and state transfer")
public class LocalTopologyManagerImpl implements LocalTopologyManager {
   private static Log log = LogFactory.getLog(LocalTopologyManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   private Transport transport;
   private ExecutorService asyncTransportExecutor;
   private GlobalComponentRegistry gcr;
   private TimeService timeService;

   private final WithinThreadExecutor withinThreadExecutor = new WithinThreadExecutor();

   // We synchronize on the entire map while handling a status request, to make sure there are no concurrent topology
   // updates from the old coordinator.
   private final Map<String, LocalCacheStatus> runningCaches =
         Collections.synchronizedMap(new HashMap<String, LocalCacheStatus>());
   private volatile boolean running;


   @Inject
   public void inject(Transport transport,
                      @ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService asyncTransportExecutor,
                      GlobalComponentRegistry gcr, TimeService timeService) {
      this.transport = transport;
      this.asyncTransportExecutor = asyncTransportExecutor;
      this.gcr = gcr;
      this.timeService = timeService;
   }

   // Arbitrary value, only need to start after JGroupsTransport
   @Start(priority = 100)
   public void start() {
      if (trace) {
         log.tracef("Starting LocalTopologyManager on %s", transport.getAddress());
      }
      running = true;
   }

   // Need to stop before the JGroupsTransport
   @Stop(priority = 9)
   public void stop() {
      if (trace) {
         log.tracef("Stopping LocalTopologyManager on %s", transport.getAddress());
      }
      running = false;
      withinThreadExecutor.shutdown();
   }

   @Override
   public CacheTopology join(String cacheName, CacheJoinInfo joinInfo, CacheTopologyHandler stm,
         PartitionHandlingManager phm) throws Exception {
      log.debugf("Node %s joining cache %s", transport.getAddress(), cacheName);

      // For Total Order caches, we must not move the topology updates to another thread
      ExecutorService topologyUpdatesExecutor = joinInfo.isTotalOrder() ? withinThreadExecutor : asyncTransportExecutor;
      LocalCacheStatus cacheStatus = new LocalCacheStatus(joinInfo, stm, phm, topologyUpdatesExecutor);
      runningCaches.put(cacheName, cacheStatus);

      long timeout = joinInfo.getTimeout();
      long endTime = timeService.expectedEndTime(timeout, TimeUnit.MILLISECONDS);
      // Pretend the join is using up a thread from the topology updates completion service.
      // This ensures that the initial topology and the GET_CACHE_LISTENERS request will happen on this thread,
      // and other topology updates are only handled after we call backgroundTaskFinished(null)
      cacheStatus.getTopologyUpdatesCompletionService().continueTaskInBackground();
      try {
         while (true) {
            int viewId = transport.getViewId();
            try {
               ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
                       CacheTopologyControlCommand.Type.JOIN, transport.getAddress(), joinInfo, viewId);
               CacheStatusResponse initialStatus = (CacheStatusResponse) executeOnCoordinator(command, timeout);
               // Ignore null responses, that's what the current coordinator returns if is shutting down
               if (initialStatus != null) {
                  doHandleTopologyUpdate(cacheName, initialStatus.getCacheTopology(), initialStatus.getAvailabilityMode(),
                        viewId, transport.getCoordinator(), cacheStatus);
                  doHandleStableTopologyUpdate(cacheName, initialStatus.getStableTopology(), viewId, cacheStatus);
                  return initialStatus.getCacheTopology();
               }
            } catch (Exception e) {
               log.debugf(e, "Error sending join request for cache %s to coordinator", cacheName);
               if (timeService.isTimeExpired(endTime)) {
                  throw e;
               }
               // TODO Add some configuration for this, or use a fraction of state transfer timeout
               Thread.sleep(100);
            }
         }
      } finally {
         cacheStatus.getTopologyUpdatesCompletionService().backgroundTaskFinished(null);
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
   public void confirmRebalance(String cacheName, int topologyId, int rebalanceId, Throwable throwable) {
      // Note that if the coordinator changes again after we sent the command, we will get another
      // query for the status of our running caches. So we don't need to retry if the command failed.
      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
            CacheTopologyControlCommand.Type.REBALANCE_CONFIRM, transport.getAddress(),
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
      Map<String, CacheStatusResponse> caches = new HashMap<String, CacheStatusResponse>();
      synchronized (runningCaches) {
         for (Map.Entry<String, LocalCacheStatus> e : runningCaches.entrySet()) {
            String cacheName = e.getKey();
            LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
            AvailabilityMode availabilityMode = cacheStatus.getPartitionHandlingManager() != null ?
                    cacheStatus.getPartitionHandlingManager().getAvailabilityMode() : null;
            caches.put(e.getKey(), new CacheStatusResponse(cacheStatus.getJoinInfo(),
                    cacheStatus.getCurrentTopology(), cacheStatus.getStableTopology(),
                    availabilityMode));
         }
      }

      boolean rebalancingEnabled = true;
      // Avoid adding a direct dependency to the ClusterTopologyManager
      ReplicableCommand command = new CacheTopologyControlCommand(null,
            CacheTopologyControlCommand.Type.POLICY_GET_STATUS, transport.getAddress(),
            transport.getViewId());
      try {
         gcr.wireDependencies(command);
         rebalancingEnabled = (Boolean) ((SuccessfulResponse) command.perform(null)).getResponseValue();
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

      final LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
      if (cacheStatus == null) {
         log.tracef("Ignoring consistent hash update %s for cache %s that doesn't exist locally",
               cacheTopology.getTopologyId(), cacheName);
         return;
      }

      cacheStatus.getTopologyUpdatesCompletionService().submit(new Runnable() {
         @Override
         public void run() {
            doHandleTopologyUpdate(cacheName, cacheTopology, availabilityMode, viewId, sender, cacheStatus);
         }
      }, null);
      // Clear any finished tasks from the completion service's queue to avoid leaks
      List<? extends Future<Void>> futures = cacheStatus.getTopologyUpdatesCompletionService().drainCompletionQueue();
      boolean interrupted = false;
      for (Future<Void> future : futures) {
         // These will not block since they are already completed
         try {
            future.get();
         } catch (InterruptedException e) {
            // This shouldn't happen as each task is complete
            interrupted = true;
         } catch (ExecutionException e) {
            log.topologyUpdateError(cacheTopology.getTopologyId(), e.getCause());
         }
      }
      if (interrupted) {
         Thread.currentThread().interrupt();
      }
   }

   protected void doHandleTopologyUpdate(String cacheName, CacheTopology cacheTopology,
         AvailabilityMode availabilityMode, int viewId, Address sender, LocalCacheStatus cacheStatus) {
      try {
         waitForView(viewId);
      } catch (InterruptedException e) {
         // Shutting down, ignore the exception and the rebalance
         return;
      }

      synchronized (cacheStatus) {
         CacheTopology existingTopology = cacheStatus.getCurrentTopology();
         if (existingTopology != null && cacheTopology.getTopologyId() <= existingTopology.getTopologyId()) {
            log.debugf("Ignoring late consistent hash update for cache %s, current topology is %s: %s",
                  cacheName, existingTopology.getTopologyId(), cacheTopology);
            return;
         }

         CacheTopologyHandler handler = cacheStatus.getHandler();
         resetLocalTopologyBeforeRebalance(cacheName, cacheTopology, existingTopology, handler);

         if (!updateCacheTopology(cacheName, cacheTopology, sender, cacheStatus))
            return;

         ConsistentHash unionCH = null;
         if (cacheTopology.getPendingCH() != null) {
            unionCH = cacheStatus.getJoinInfo().getConsistentHashFactory().union(cacheTopology.getCurrentCH(),
                  cacheTopology.getPendingCH());
         }

         CacheTopology unionTopology = new CacheTopology(cacheTopology.getTopologyId(), cacheTopology.getRebalanceId(),
               cacheTopology.getCurrentCH(), cacheTopology.getPendingCH(), unionCH, cacheTopology.getActualMembers());
         unionTopology.logRoutingTableInformation();

         boolean updateAvailabilityModeFirst = availabilityMode != AvailabilityMode.AVAILABLE;
         if (updateAvailabilityModeFirst) {
            if (cacheStatus.getPartitionHandlingManager() != null && availabilityMode != null) {
               cacheStatus.getPartitionHandlingManager().setAvailabilityMode(availabilityMode);
            }
         }
         if ((existingTopology == null || existingTopology.getRebalanceId() != cacheTopology.getRebalanceId()) && unionCH != null) {
            // This CH_UPDATE command was sent after a REBALANCE_START command, but arrived first.
            // We will start the rebalance now and ignore the REBALANCE_START command when it arrives.
            log.tracef("This topology update has a pending CH, starting the rebalance now");
            handler.rebalance(unionTopology);
         } else {
            handler.updateConsistentHash(unionTopology);
         }

         if (!updateAvailabilityModeFirst) {
            if (cacheStatus.getPartitionHandlingManager() != null && availabilityMode != null) {
               cacheStatus.getPartitionHandlingManager().setAvailabilityMode(availabilityMode);
            }
         }
      }
   }

   private boolean updateCacheTopology(String cacheName, CacheTopology cacheTopology, Address sender,
         LocalCacheStatus cacheStatus) {
      // Block if there is a status request in progress
      // The caller should have already acquired the cacheStatus monitor
      synchronized (runningCaches) {
         if (!sender.equals(transport.getCoordinator())) {
            log.debugf("Ignoring topology %d from old coordinator %s", cacheTopology.getTopologyId(), sender);
            return false;
         }
      }

      log.debugf("Updating local topology for cache %s: %s", cacheName, cacheTopology);
      cacheStatus.setCurrentTopology(cacheTopology);
      return true;
   }

   private void resetLocalTopologyBeforeRebalance(String cacheName, CacheTopology newCacheTopology,
         CacheTopology oldCacheTopology, CacheTopologyHandler handler) {
      boolean newRebalance = newCacheTopology.getPendingCH() != null;
      if (newRebalance) {
         // The initial topology doesn't need a reset because we are guaranteed not to be a member
         if (oldCacheTopology == null)
            return;

         // We only need a reset if we missed a topology update
         if (newCacheTopology.getTopologyId() == oldCacheTopology.getTopologyId() + 1)
            return;

         // We have missed a topology update, and that topology might have removed some of our segments.
         // If this rebalance adds those same segments, we need to remove the old data/inbound transfers first.
         // This can happen when the coordinator changes, either because the old one left or because there was a merge,
         // and the rebalance after merge arrives before the merged topology update.
         if (newCacheTopology.getRebalanceId() != oldCacheTopology.getRebalanceId()) {
            // The currentCH changed, we need to install a "reset" topology with the new currentCH first
            CacheTopology resetTopology = new CacheTopology(newCacheTopology.getTopologyId() - 1,
                  newCacheTopology.getRebalanceId() - 1, newCacheTopology.getCurrentCH(), null, newCacheTopology.getActualMembers());
            log.debugf("Installing fake cache topology %s for cache %s", resetTopology, cacheName);
            handler.updateConsistentHash(resetTopology);
         }
      }
   }

   @Override
   public void handleStableTopologyUpdate(final String cacheName, final CacheTopology newStableTopology,
         final int viewId) {
      final LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
      if (cacheStatus != null) {
         cacheStatus.getTopologyUpdatesCompletionService().submit(new Runnable() {
            @Override
            public void run() {
               doHandleStableTopologyUpdate(cacheName, newStableTopology, viewId, cacheStatus);
            }
         }, null);
      }
   }

   protected void doHandleStableTopologyUpdate(String cacheName, CacheTopology newStableTopology, int viewId,
         LocalCacheStatus cacheStatus) {
      synchronized (cacheStatus) {
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

      cacheStatus.getTopologyUpdatesCompletionService().submit(new Runnable() {
         @Override
         public void run() {
            doHandleRebalance(viewId, cacheStatus, cacheTopology, cacheName, sender);
         }
      }, null);
   }

   protected void doHandleRebalance(int viewId, LocalCacheStatus cacheStatus, CacheTopology cacheTopology, String cacheName, Address sender) {
      try {
         waitForView(viewId);
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

         if (!updateCacheTopology(cacheName, cacheTopology, sender, cacheStatus))
            return;

         log.debugf("Starting local rebalance for cache %s, topology = %s", cacheName, cacheTopology);
         cacheTopology.logRoutingTableInformation();
         cacheStatus.setCurrentTopology(cacheTopology);

         CacheTopologyHandler handler = cacheStatus.getHandler();
         resetLocalTopologyBeforeRebalance(cacheName, cacheTopology, existingTopology, handler);

         ConsistentHash unionCH = cacheStatus.getJoinInfo().getConsistentHashFactory().union(
               cacheTopology.getCurrentCH(), cacheTopology.getPendingCH());
         CacheTopology newTopology = new CacheTopology(cacheTopology.getTopologyId(), cacheTopology
               .getRebalanceId(),
               cacheTopology.getCurrentCH(), cacheTopology.getPendingCH(), unionCH, cacheTopology.getActualMembers());
         handler.rebalance(newTopology);
      }
   }

   @Override
   public CacheTopology getCacheTopology(String cacheName) {
      LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
      return cacheStatus.getCurrentTopology();
   }

   @Override
   public CacheTopology getStableCacheTopology(String cacheName) {
      LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
      return cacheStatus.getCurrentTopology();
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

   private void waitForView(int viewId) throws InterruptedException {
      transport.waitForView(viewId);
   }

   @ManagedAttribute(description = "Rebalancing enabled", displayName = "Rebalancing enabled",
         dataType = DataType.TRAIT, writable = true)
   @Override
   public boolean isRebalancingEnabled() throws Exception {
      ReplicableCommand command = new CacheTopologyControlCommand(null,
            CacheTopologyControlCommand.Type.POLICY_GET_STATUS, transport.getAddress(), transport.getViewId());
      while (true) {
         Address coordinator = transport.getCoordinator();
         int nextViewId = transport.getViewId() + 1;
         try {
            return (Boolean) executeOnCoordinator(command, getGlobalTimeout());
         } catch (SuspectException e) {
            if (trace) log.tracef("Coordinator left the cluster while querying rebalancing status, retrying");
            transport.waitForView(nextViewId);
         }
      }
   }

   @Override
   public void setRebalancingEnabled(boolean enabled) throws Exception {
      CacheTopologyControlCommand.Type type = enabled ? CacheTopologyControlCommand.Type.POLICY_ENABLE
            : CacheTopologyControlCommand.Type.POLICY_DISABLE;
      ReplicableCommand command = new CacheTopologyControlCommand(null, type, transport.getAddress(),
            transport.getViewId());
      executeOnClusterSync(command, getGlobalTimeout(), false, false);
   }

   @ManagedAttribute(description = "Cluster availability", displayName = "Cluster availability",
         dataType = DataType.TRAIT, writable = false)
   public String getClusterAvailability() {
      AvailabilityMode clusterAvailability = AvailabilityMode.AVAILABLE;
      synchronized (runningCaches) {
         for (LocalCacheStatus cacheStatus : runningCaches.values()) {
            AvailabilityMode availabilityMode = cacheStatus.getPartitionHandlingManager() != null ?
                  cacheStatus.getPartitionHandlingManager().getAvailabilityMode() : null;
            clusterAvailability = availabilityMode != null ? clusterAvailability.min(availabilityMode) : clusterAvailability;

         }
      }
      return clusterAvailability.toString();
   }

   @Override
   public AvailabilityMode getCacheAvailability(String cacheName) {
      LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
      AvailabilityMode availabilityMode = cacheStatus.getPartitionHandlingManager() != null ?
            cacheStatus.getPartitionHandlingManager().getAvailabilityMode() : AvailabilityMode.AVAILABLE;
      return availabilityMode;
   }

   @Override
   public void setCacheAvailability(String cacheName, AvailabilityMode availabilityMode) throws Exception {
      CacheTopologyControlCommand.Type type = CacheTopologyControlCommand.Type.AVAILABILITY_MODE_CHANGE;
      ReplicableCommand command = new CacheTopologyControlCommand(cacheName, type, transport.getAddress(),
            availabilityMode, transport.getViewId());
      executeOnCoordinator(command, getGlobalTimeout());
   }

   private Object executeOnCoordinator(ReplicableCommand command, long timeout) throws Exception {
      Response response;
      if (transport.isCoordinator()) {
         try {
            if (log.isTraceEnabled()) log.tracef("Attempting to execute command on self: %s", command);
            gcr.wireDependencies(command);
            response = (Response) command.perform(null);
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
         asyncTransportExecutor.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
               if (log.isTraceEnabled()) log.tracef("Attempting to execute command on self: %s", command);
               gcr.wireDependencies(command);
               try {
                  return command.perform(null);
               } catch (Throwable t) {
                  log.errorf(t, "Failed to execute ReplicableCommand %s on coordinator async: %s", command, t.getMessage());
                  throw new Exception(t);
               }
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
                  ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, timeout, null, DeliverOrder.NONE, false);
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
         throw new CacheException("Unsuccessful local response");
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
      return (int) gcr.getGlobalConfiguration().transport().distributedSyncTimeout();
   }
}

class LocalCacheStatus {
   private final CacheJoinInfo joinInfo;
   private final CacheTopologyHandler handler;
   private final PartitionHandlingManager partitionHandlingManager;
   private volatile CacheTopology currentTopology;
   private volatile CacheTopology stableTopology;
   private final SemaphoreCompletionService<Void> topologyUpdatesCompletionService;

   public LocalCacheStatus(CacheJoinInfo joinInfo, CacheTopologyHandler handler, PartitionHandlingManager phm,
         ExecutorService executor) {
      this.joinInfo = joinInfo;
      this.handler = handler;
      this.partitionHandlingManager = phm;

      this.topologyUpdatesCompletionService = new SemaphoreCompletionService<>(executor, 1);
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

   public CacheTopology getCurrentTopology() {
      return currentTopology;
   }

   public void setCurrentTopology(CacheTopology currentTopology) {
      this.currentTopology = currentTopology;
   }

   public CacheTopology getStableTopology() {
      return stableTopology;
   }

   public void setStableTopology(CacheTopology stableTopology) {
      this.stableTopology = stableTopology;
   }

   public SemaphoreCompletionService<Void> getTopologyUpdatesCompletionService() {
      return topologyUpdatesCompletionService;
   }
}
