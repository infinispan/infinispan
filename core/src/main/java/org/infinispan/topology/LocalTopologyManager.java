package org.infinispan.topology;

import java.util.Map;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.partionhandling.AvailabilityMode;
import org.infinispan.partionhandling.impl.PartitionHandlingManager;

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
   void confirmRebalance(String cacheName, int topologyId, int rebalanceId, Throwable throwable);

   /**
    * Recovers the current topology information for all running caches and returns it to the coordinator.
    * @param viewId
    */
   // TODO Add a new class to hold the CacheJoinInfo and the CacheTopology
   Map<String, CacheStatusResponse> handleStatusRequest(int viewId);

   /**
    * Updates the current and/or pending consistent hash, without transferring any state.
    */
   void handleTopologyUpdate(String cacheName, CacheTopology cacheTopology, AvailabilityMode availabilityMode, int viewId) throws InterruptedException;

   /**
    * Update the stable cache topology.
    *
    * Mostly needed for backup, so that a new coordinator can recover the stable topology of the cluster.
    */
   void handleStableTopologyUpdate(String cacheName, CacheTopology cacheTopology, int viewId);

   /**
    * Performs the state transfer.
    */
   void handleRebalance(String cacheName, CacheTopology cacheTopology, int viewId) throws InterruptedException;

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
    * Enable or disable rebalancing in the entire cluster.
    */
   void setRebalancingEnabled(boolean enabled) throws Exception;

   /**
    * Retrieves the availability state of a cache.
    */
   AvailabilityMode getCacheAvailability(String cacheName);

   /**
    * Updates the availability state of a cache (for the entire cluster).
    */
   void setCacheAvailability(String cacheName, AvailabilityMode availabilityMode) throws Exception;
}
