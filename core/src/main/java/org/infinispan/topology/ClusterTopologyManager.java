package org.infinispan.topology;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.partionhandling.impl.AvailabilityMode;
import org.infinispan.remoting.transport.Address;

/**
 * Maintains the topology for all the caches in the cluster.
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Scope(Scopes.GLOBAL)
public interface ClusterTopologyManager {
   /**
    * Signals that a new member is joining the cache.
    *
    * The returned {@code CacheStatusResponse.cacheTopology} is the current cache topology before the node joined.
    * If the node is the first to join the cache, the returned topology does include the joiner,
    * and it is never {@code null}.
    */
   CacheStatusResponse handleJoin(String cacheName, Address joiner, CacheJoinInfo joinInfo, int viewId) throws Exception;

   /**
    * Signals that a member is leaving the cache.
    */
   void handleLeave(String cacheName, Address leaver, int viewId) throws Exception;

   /**
    * Marks the rebalance as complete on the sender.
    */
   void handleRebalanceCompleted(String cacheName, Address node, int topologyId, Throwable throwable, int viewId) throws Exception;

   /**
    * Install a new cluster view.
    */
   void handleClusterView(boolean isMerge, int viewId);

   void broadcastRebalanceStart(String cacheName, CacheTopology cacheTopology, boolean totalOrder, boolean distributed);

   void broadcastTopologyUpdate(String cacheName, CacheTopology cacheTopology, AvailabilityMode availabilityMode, boolean totalOrder, boolean distributed);

   void broadcastStableTopologyUpdate(String cacheName, CacheTopology cacheTopology, boolean totalOrder, boolean distributed);

   boolean isRebalancingEnabled();

   void setRebalancingEnabled(boolean enabled);

   void forceRebalance(String cacheName);
}
