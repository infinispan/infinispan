package org.infinispan.topology;

import java.util.concurrent.CompletionStage;

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
   CompletionStage<CacheTopology> join(String cacheName, CacheJoinInfo joinInfo, CacheTopologyHandler stm, PartitionHandlingManager phm) throws Exception;

   /**
    * Forwards the leave request to the coordinator.
    */
   void leave(String cacheName, long timeout);

   /**
    * Confirm that the local cache {@code cacheName} has finished receiving the new data for topology
    * {@code topologyId}.
    *
    * <p>The coordinator can change during the state transfer, so we make the rebalance RPC async
    * and we send the response as a different command.
    *  @param cacheName the name of the cache
    * @param topologyId the current topology id of the node at the time the rebalance is completed.
    * @param rebalanceId the id of the current rebalance
    * @param throwable {@code null} unless local rebalance ended because of an error.
    */
   void confirmRebalancePhase(String cacheName, int topologyId, int rebalanceId, Throwable throwable);

   /**
    * Recovers the current topology information for all running caches and returns it to the coordinator.
    *
    * @param viewId The coordinator's view id
    */
   // TODO Add a new class to hold the CacheJoinInfo and the CacheTopology
   CompletionStage<ManagerStatusResponse> handleStatusRequest(int viewId);

   /**
    * Updates the current and/or pending consistent hash, without transferring any state.
    */
   CompletionStage<Void> handleTopologyUpdate(String cacheName, CacheTopology cacheTopology,
                                              AvailabilityMode availabilityMode, int viewId, Address sender);

   /**
    * Update the stable cache topology.
    * <p>
    * Mostly needed for backup, so that a new coordinator can recover the stable topology of the cluster.
    */
   CompletionStage<Void> handleStableTopologyUpdate(String cacheName, CacheTopology cacheTopology, final Address sender,
                                                    int viewId);

   /**
    * Performs the state transfer.
    */
   CompletionStage<Void> handleRebalance(String cacheName, CacheTopology cacheTopology, int viewId, Address sender);

   /**
    * @return the current topology for a cache.
    */
   CacheTopology getCacheTopology(String cacheName);

   /**
    * @return the last stable topology for a cache.
    */
   CacheTopology getStableCacheTopology(String cacheName);

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
    */
   void cacheShutdown(String name);

   /**
    * Handles the local operations related to gracefully shutting down a cache
    */
   CompletionStage<Void> handleCacheShutdown(String cacheName);

   /**
    * Returns a {@link CompletionStage} that completes when the cache with the name {@code cacheName} has a
    * stable topology. Returns null if the cache does not exist.
    */
   CompletionStage<Void> stableTopologyCompletion(String cacheName);

   /**
    * Asserts the cache with the given name has a stable topology installed.
    *
    * @param cacheName: The cache name to search.
    * @throws MissingMembersException: Thrown when the cache does not have a stable topology.
    */
   default void assertTopologyStable(String cacheName) { }

}
