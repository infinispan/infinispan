package org.infinispan.topology;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.transport.Address;

/**
 * Maintains the topology for all the caches in the cluster.
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Scope(Scopes.GLOBAL)
public interface ClusterTopologyManager {

   enum ClusterManagerStatus {
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

   /**
    * Returns the list of nodes that joined the cache with the given {@code cacheName} if the current
    * node is the coordinator. If the node is not the coordinator, the method returns null.
    */
   List<Address> currentJoiners(String cacheName);

   /**
    * Signals that a new member is joining the cache.
    *
    * The returned {@code CacheStatusResponse.cacheTopology} is the current cache topology before the node joined.
    * If the node is the first to join the cache, the returned topology does include the joiner,
    * and it is never {@code null}.
    */
   CompletionStage<CacheStatusResponse> handleJoin(String cacheName, Address joiner, CacheJoinInfo joinInfo, int viewId) throws Exception;


   /**
    * Signals that a member is leaving the cache.
    * @param viewId is always ignored
    * @deprecated since 16.0, use {@link #handleLeave(String, Address)} instead
    */
   @Deprecated(since = "16.0", forRemoval = true)
   default CompletionStage<Void> handleLeave(String cacheName, Address leaver, int viewId) throws Exception {
      return handleLeave(cacheName, leaver);
   }

   /**
    * Signals that a member is leaving the cache.
    */
   CompletionStage<Void> handleLeave(String cacheName, Address leaver) throws Exception;

   /**
    * Marks the rebalance as complete on the sender.
    * @param viewId is always ignored
    * @deprecated since 16.0, use {@link #handleRebalancePhaseConfirm(String, Address, int, Throwable)} instead
    */
   @Deprecated(since = "16.0", forRemoval = true)
   default CompletionStage<Void> handleRebalancePhaseConfirm(String cacheName, Address node, int topologyId, Throwable throwable, int viewId) throws Exception {
      return handleRebalancePhaseConfirm(cacheName, node, topologyId, throwable);
   }

   CompletionStage<Void> handleRebalancePhaseConfirm(String cacheName, Address node, int topologyId, Throwable throwable) throws Exception;

   boolean isRebalancingEnabled();

   /**
    * Returns whether rebalancing is enabled or disabled for this container.
    */
   boolean isRebalancingEnabled(String cacheName);

   /**
    * Globally enables or disables whether automatic rebalancing should occur.
    */
   CompletionStage<Void> setRebalancingEnabled(boolean enabled);

   /**
    * Enables or disables rebalancing for the specified cache
    */
   CompletionStage<Void> setRebalancingEnabled(String cacheName, boolean enabled);

   /**
    * Retrieves the rebalancing status of a cache
    */
   RebalancingStatus getRebalancingStatus(String cacheName);

   CompletionStage<Void> forceRebalance(String cacheName);

   CompletionStage<Void> forceAvailabilityMode(String cacheName, AvailabilityMode availabilityMode);

   CompletionStage<Void> handleShutdownRequest(String cacheName) throws Exception;

   boolean useCurrentTopologyAsStable(String cacheName, boolean force);

   /**
    * Sets the id of the initial topology in given cache. This is necessary when using entry versions
    * that contain topology id; had we started with topology id 1, newer versions would not be recognized properly.
    */
   void setInitialCacheTopologyId(String cacheName, int topologyId);

   ClusterManagerStatus getStatus();
}
