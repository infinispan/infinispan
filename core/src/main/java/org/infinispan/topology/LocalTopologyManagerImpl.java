package org.infinispan.topology;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.distribution.ch.ConsistentHash;
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
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
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
   }

   @Override
   public CacheTopology join(String cacheName, CacheJoinInfo joinInfo, CacheTopologyHandler stm,
         PartitionHandlingManager phm) throws Exception {
      log.debugf("Node %s joining cache %s", transport.getAddress(), cacheName);
      LocalCacheStatus cacheStatus = new LocalCacheStatus(joinInfo, stm, phm);
      runningCaches.put(cacheName, cacheStatus);

      long timeout = joinInfo.getTimeout();
      long endTime = timeService.expectedEndTime(timeout, TimeUnit.MILLISECONDS);
      // Synchronize here to delay any rebalance until after we have received the initial cache topology.
      // This ensures that the cache will have a topology at the end of startup (with awaitInitialTransfer disabled).
      synchronized (cacheStatus) {
         while (true) {
            int viewId = transport.getViewId();
            try {
               ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
                       CacheTopologyControlCommand.Type.JOIN, transport.getAddress(), joinInfo, viewId);
               CacheStatusResponse initialStatus = executeOnCoordinator(command, timeout);
               // Ignore null responses, that's what the current coordinator returns if is shutting down
               if (initialStatus != null) {
                  handleConsistentHashUpdate(cacheName, initialStatus.getCacheTopology(),
                                             null, initialStatus.getAvailabilityMode(), viewId, TopologyUpdate.CH_UPDATE, TopologyState.NONE, transport.getCoordinator());
                  handleStableTopologyUpdate(cacheName, initialStatus.getStableTopology(), viewId);
                  cacheStatus.markJoined();
                  return initialStatus.getCacheTopology();
               }
            } catch (Exception e) {
               log.debugf(e, "Error sending join request for cache %s to coordinator", cacheName);
               if (timeService.isTimeExpired(endTime)) {
                  throw e;
               }
               // TODO Add some configuration for this, or use a fraction of state transfer timeout
               cacheStatus.wait(1000);
            }
         }
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
   public Map<String, CacheStatusResponse> handleStatusRequest(int viewId) {
      Map<String, CacheStatusResponse> caches = new HashMap<>();
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
      return caches;
   }

   @Override
   public void handleTopologyUpdate(String cacheName, CacheTopology cacheTopology, ConsistentHash newCH, AvailabilityMode availabilityMode,
                                    TopologyState state, int viewId, Address sender) throws InterruptedException {
      handleConsistentHashUpdate(cacheName, cacheTopology, newCH, availabilityMode, viewId, TopologyUpdate.CH_UPDATE, state, sender);
   }

   @Override
   public void handleReadCHUpdate(String cacheName, CacheTopology cacheTopology, AvailabilityMode availabilityMode,
                                  int viewId, Address sender) throws InterruptedException {
      Throwable throwable = null;
      try {
         handleConsistentHashUpdate(cacheName, cacheTopology, null, availabilityMode, viewId, TopologyUpdate.READ_CH_UPDATE, null, sender);
      } catch (Throwable t) {
         throwable = t;
      } finally {
         ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
                                                                     CacheTopologyControlCommand.Type.READ_CH_CONFIRM,
                                                                     transport.getAddress(),
                                                                     cacheTopology.getTopologyId(), cacheTopology.getRebalanceId(),
                                                                     throwable, transport.getViewId());
         try {
            executeOnCoordinatorAsync(command);
         } catch (Exception e) {
            log.debugf(e, "Error sending the rebalance completed notification for cache %s to the coordinator",
                       cacheName);
         }
      }
   }

   @Override
   public void handleWriteCHUpdate(String cacheName, CacheTopology cacheTopology, AvailabilityMode availabilityMode,
                                   int viewId, Address sender) throws InterruptedException {
      handleConsistentHashUpdate(cacheName, cacheTopology, null, availabilityMode, viewId, TopologyUpdate.WRITE_CH_UPDATE, null, sender);
   }

   private void handleConsistentHashUpdate(String cacheName, CacheTopology cacheTopology, ConsistentHash newCH, AvailabilityMode availabilityMode,
                                           int viewId, TopologyUpdate topologyUpdate, TopologyState state, final Address sender) throws InterruptedException {
      if (!running) {
         log.tracef("Ignoring %s consistent hash update %s for cache %s, the local cache manager is not running",
                    topologyUpdate.print(), cacheTopology.getTopologyId(), cacheName);
         return;
      }
      waitForView(viewId);

      final LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
      if (cacheStatus == null) {
         log.tracef("Ignoring %s consistent hash update %s for cache %s that doesn't exist locally",
                    topologyUpdate.print(), cacheTopology.getTopologyId(), cacheName);
         return;
      }

      if (!cacheStatus.isJoined() && topologyUpdate != TopologyUpdate.CH_UPDATE &&
            !containsLocalMember(cacheTopology.getReadConsistentHash()) &&
            !containsLocalMember(cacheTopology.getWriteConsistentHash())) {
         log.tracef("Ignoring %s consistent hash update %s for cache %s. Member hasn't joined yet",
                   topologyUpdate.print(), cacheTopology.getTopologyId(), cacheName);
         return;
      }

      synchronized (cacheStatus) {
         CacheTopology existingTopology = cacheStatus.getCurrentTopology();
         if (existingTopology != null && cacheTopology.getTopologyId() <= existingTopology.getTopologyId()) {
            log.debugf("Ignoring late %s Consistent Hash update for cache %s, current topology is %s: %s",
                       topologyUpdate.print(), cacheName, existingTopology.getTopologyId(), cacheTopology);
            return;
         }

         log.debugf("Updating local %s consistent hash for cache %s: new topology = %s", topologyUpdate.print(),
                    cacheName, cacheTopology);

         if (!updateCacheTopology(cacheName, cacheTopology, sender, cacheStatus))
            return;

         cacheStatus.setCurrentTopology(cacheTopology);
         cacheTopology.logRoutingTableInformation();

         final boolean updateAvailabilityModeFirst = availabilityMode != AvailabilityMode.AVAILABLE;
         final PartitionHandlingManager partitionHandlingManager = cacheStatus.getPartitionHandlingManager();
         final boolean canUpdateAvailability = availabilityMode != null && partitionHandlingManager != null;

         if (canUpdateAvailability && updateAvailabilityModeFirst) {
            partitionHandlingManager.setAvailabilityMode(availabilityMode);
         }

         switch (topologyUpdate) {
            case READ_CH_UPDATE:
               cacheStatus.getHandler().updateReadConsistentHash(cacheTopology);
               break;
            case WRITE_CH_UPDATE:
               cacheStatus.getHandler().updateWriteConsistentHash(cacheTopology);
               break;
            case CH_UPDATE:
               cacheStatus.getHandler().updateConsistentHash(cacheTopology, newCH, state);
               break;
         }

         if (canUpdateAvailability && !updateAvailabilityModeFirst) {
            partitionHandlingManager.setAvailabilityMode(availabilityMode);
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
         } else {
            log.debugf("Updating local topology for cache %s: %s", cacheName, cacheTopology);
            cacheStatus.setCurrentTopology(cacheTopology);
            return true;
         }
      }
   }

   @Override
   public void handleStableTopologyUpdate(String cacheName, CacheTopology newStableTopology, int viewId) {
      LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
      if (cacheStatus != null) {
         synchronized (cacheStatus) {
            CacheTopology stableTopology = cacheStatus.getStableTopology();
            if (stableTopology == null || stableTopology.getTopologyId() < newStableTopology.getTopologyId()) {
               log.tracef("Updating stable topology for cache %s: %s", cacheName, newStableTopology);
               cacheStatus.setStableTopology(newStableTopology);
            }
         }
      }
   }

   @Override
   public void handleRebalance(String cacheName, CacheTopology cacheTopology, ConsistentHash newCH, int viewId, Address sender) throws InterruptedException {
      if (!running) {
         log.debugf("Ignoring rebalance request %s for cache %s, the local cache manager is not running",
                    cacheTopology.getTopologyId(), cacheName);
         return;
      }
      waitForView(viewId);

      LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
      if (cacheStatus == null) {
         log.tracef("Ignoring rebalance %s for cache %s that doesn't exist locally",
               cacheTopology.getTopologyId(), cacheName);
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
         CacheTopologyHandler handler = cacheStatus.getHandler();

         log.debugf("Starting local rebalance for cache %s, topology = %s", cacheName, cacheTopology);
         cacheTopology.logRoutingTableInformation();
         cacheStatus.setCurrentTopology(cacheTopology);

         handler.rebalance(cacheTopology, newCH);
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
      return executeOnCoordinator(command, getGlobalTimeout());
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
      return cacheStatus.getPartitionHandlingManager() != null ?
            cacheStatus.getPartitionHandlingManager().getAvailabilityMode() :
            AvailabilityMode.AVAILABLE;
   }

   @Override
   public void setCacheAvailability(String cacheName, AvailabilityMode availabilityMode) throws Exception {
      CacheTopologyControlCommand.Type type = CacheTopologyControlCommand.Type.AVAILABILITY_MODE_CHANGE;
      ReplicableCommand command = new CacheTopologyControlCommand(cacheName, type, transport.getAddress(),
            availabilityMode, transport.getViewId());
      executeOnCoordinator(command, getGlobalTimeout());
   }

   private <T> T executeOnCoordinator(ReplicableCommand command, long timeout) throws Exception {
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
      //noinspection unchecked
      return (T) ((SuccessfulResponse) response).getResponseValue();
   }

   private void executeOnCoordinatorAsync(final ReplicableCommand command) throws Exception {
      // if we are the coordinator, the execution is actually synchronous
      if (transport.isCoordinator()) {
         asyncTransportExecutor.execute(new Runnable() {
            @Override
            public void run() {
               if (log.isTraceEnabled()) log.tracef("Attempting to execute command on self: %s", command);
               gcr.wireDependencies(command);
               try {
                  command.perform(null);
               } catch (Throwable t) {
                  log.errorf(t, "Failed to execute ReplicableCommand %s on coordinator async: %s", command, t.getMessage());
               }
            }
         });
      } else {
         Address coordinator = transport.getCoordinator();
         // ignore the responses
         transport.invokeRemotely(Collections.singleton(coordinator), command,
                                  ResponseMode.ASYNCHRONOUS_WITH_SYNC_MARSHALLING, 0, null, DeliverOrder.NONE, false);
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

      responseValues.put(transport.getAddress(), ((SuccessfulResponse) localResponse).getResponseValue());

      return responseValues;
   }

   private int getGlobalTimeout() {
      // TODO Rename setting to something like globalRpcTimeout
      return (int) gcr.getGlobalConfiguration().transport().distributedSyncTimeout();
   }

   private boolean containsLocalMember(ConsistentHash consistentHash) {
      return consistentHash.getMembers().contains(transport.getAddress());
   }

   private static enum TopologyUpdate {
      READ_CH_UPDATE {
         @Override
         String print() {
            return "read";
         }
      },
      WRITE_CH_UPDATE {
         @Override
         String print() {
            return "write";
         }
      },
      CH_UPDATE {
         @Override
         String print() {
            return "";
         }
      };

      abstract String print();


   }
}

class LocalCacheStatus {
   private final CacheJoinInfo joinInfo;
   private final CacheTopologyHandler handler;
   private final PartitionHandlingManager partitionHandlingManager;
   private volatile CacheTopology currentTopology;
   private volatile CacheTopology stableTopology;
   private volatile boolean joined;

   public LocalCacheStatus(CacheJoinInfo joinInfo, CacheTopologyHandler handler, PartitionHandlingManager phm) {
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

   public void markJoined() {
      this.joined = true;
   }

   public boolean isJoined() {
      return joined;
   }
}
