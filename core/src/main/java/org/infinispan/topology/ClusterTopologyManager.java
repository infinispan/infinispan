package org.infinispan.topology;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.transport.Address;

/**
 * Maintains the list of members and performs rebalance operations.
 * The {@link RebalancePolicy} actually decides when to perform the rebalance or how to update the
 * consistent hash.
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Scope(Scopes.GLOBAL)
public interface ClusterTopologyManager {
   /**
    * Used by {@link RebalancePolicy} to start a state transfer.
    */
   void triggerRebalance(String cacheName) throws Exception;


   /**
    * Updates the members list and notifies the {@link RebalancePolicy}.
    * @return The current consistent hash.
    */
   CacheTopology handleJoin(String cacheName, Address joiner, CacheJoinInfo joinInfo, int viewId) throws Exception;

   /**
    * Updates the members list and notifies the {@link RebalancePolicy}
    */
   void handleLeave(String cacheName, Address leaver, int viewId) throws Exception;

   /**
    * Marks the rebalance as complete on the sender.
    */
   void handleRebalanceCompleted(String cacheName, Address node, int topologyId, Throwable throwable, int viewId) throws Exception;

   public void handleNewView(ViewChangedEvent e);
}
