package org.infinispan.topology;

import org.infinispan.statetransfer.StateTransferManager;

/**
 * The link between {@link LocalTopologyManager} and {@link StateTransferManager}.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public interface CacheTopologyHandler {

   /**
    * Invoked when the CH has to be immediately updated because of a leave or when the state transfer has completed
    * and we have to install a permanent CH (pendingCH == null). A state transfer is not always required.
    */
   void updateConsistentHash(CacheTopology cacheTopology);

   /**
    * Invoked when state transfer has to be started.
    *
    * The caller will not consider the local rebalance done when this method returns. Instead, the handler
    * will have to call {@link LocalTopologyManager#confirmRebalancePhase(String, int, int, Throwable)}
    */
   void rebalance(CacheTopology cacheTopology);
}
