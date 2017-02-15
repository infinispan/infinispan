package org.infinispan.topology;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.transport.Address;

/**
 * Runs on every node and handles the communication with the {@link ClusterTopologyManager}.
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Scope(Scopes.GLOBAL)
public interface LocalTopologyManager {

   /**
    * Forwards the join request to the coordinator.
    * @return The current consistent hash.
    */
   CacheTopology join(String cacheName, CacheJoinInfo joinInfo, CacheTopologyHandler stm, PartitionHandlingManager phm) throws Exception;

   /**
    * Forwards the leave request to the coordinator.
    */
   void leave(String cacheName);

   /**
    * Confirm that the local cache {@code cacheName} has finished receiving the new data for topology
    * {@code topologyId}.
    *
    * <p>The coordinator can change during the state transfer, so we make the rebalance RPC async
    * and we send the response as a different command.
    *  @param cacheName the name of the cache
    * @param topologyId the current topology id of the node at the time the rebalance is completed. This must be >= than the one when rebalance starts.
    * @param rebalanceId
    * @param throwable {@code null} unless local rebalance ended because of an error.
    */
   void confirmRebalancePhase(String cacheName, int topologyId, int rebalanceId, Throwable throwable);

   /**
    * Recovers the current topology information for all running caches and returns it to the coordinator.
    * @param viewId
    */
   // TODO Add a new class to hold the CacheJoinInfo and the CacheTopology
   ManagerStatusResponse handleStatusRequest(int viewId);

   /**
    * Updates the current and/or pending consistent hash, without transferring any state.
    */
   void handleTopologyUpdate(String cacheName, CacheTopology cacheTopology, AvailabilityMode availabilityMode,
                             int viewId, Address sender) throws InterruptedException;

   /**
    * Update the stable cache topology.
    *
    * Mostly needed for backup, so that a new coordinator can recover the stable topology of the cluster.
    */
   void handleStableTopologyUpdate(String cacheName, CacheTopology cacheTopology, final Address sender,
         int viewId);

   /**
    * Performs the state transfer.
    */
   void handleRebalance(String cacheName, CacheTopology cacheTopology, int viewId, Address sender) throws InterruptedException;

   /**
    * @return the current topology for a cache.
    */
   CacheTopology getCacheTopology(String cacheName);

   /**
    * @return the last stable topology for a cache.
    */
   CacheTopology getStableCacheTopology(String cacheName);

   /**
    * Checks if the cache defined by {@code cacheName} is using total order.
    * <p/>
    * If this component is not running or the {@code cacheName} is not defined, it returns {@code false}.
    *
    * @return {@code true} if the cache is using the total order protocol, {@code false} otherwise.
    */
   boolean isTotalOrderCache(String cacheName);

   /**
    * Checks whether rebalancing is enabled for the entire cluster.
    */
   boolean isRebalancingEnabled() throws Exception;

   /**
    * Checks whether rebalancing is enabled for the specified cache.
    */
   boolean isCacheRebalancingEnabled(String cacheName) throws Exception;

   /**
    * Enable or disable rebalancing in the entire cluster.
    */
   void setRebalancingEnabled(boolean enabled) throws Exception;

   /**
    * Enable or disable rebalancing for the specified cache.
    */
   void setCacheRebalancingEnabled(String cacheName, boolean enabled) throws Exception;

   /**
    * Retrieve the rebalancing status for the specified cache
    */
   RebalancingStatus getRebalancingStatus(String cacheName) throws Exception;

   /**
    * Retrieves the availability state of a cache.
    */
   AvailabilityMode getCacheAvailability(String cacheName);

   /**
    * Updates the availability state of a cache (for the entire cluster).
    */
   void setCacheAvailability(String cacheName, AvailabilityMode availabilityMode) throws Exception;

   /**
    * Returns the local UUID of this node. If global state persistence is enabled, this UUID will be saved and reused
    * across restarts
    */
   PersistentUUID getPersistentUUID();

   /**
    * Initiates a cluster-wide cache shutdown for the specified cache
    * @throws Exception
    */
   void cacheShutdown(String name) throws Exception;

   /**
    * Handles the local operations related to gracefully shutting down a cache
    */
   void handleCacheShutdown(String cacheName);

}
