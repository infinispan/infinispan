package org.infinispan.partitionhandling.impl;

import java.util.List;
import java.util.Map;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheStatusResponse;
import org.infinispan.topology.CacheTopology;

/**
 * Implementations decide what to do when the cache membership changes, either because new nodes joined, nodes left,
 * or there was a merge. The decision is then applied by calling one of the {@link AvailabilityStrategyContext} methods.
 *
 * The strategy can also queue actions until the current rebalance ends, and execute them on
 * {@link #onRebalanceEnd(AvailabilityStrategyContext)}.
 *
 * Method invocations are synchronized, so it's not possible to have concurrent invocations.
 *
 * @author Mircea Markus
 * @author Dan Berindei
 * @since 7.0
 */
public interface AvailabilityStrategy {
   /**
    * Compute the read consistent hash for a topology with a {@code null} union consistent hash.
    * Originally a copy of {@link CacheTopology#getReadConsistentHash()} but differs in case of scattered cache.
    */
   static ConsistentHash ownersConsistentHash(CacheTopology topology, ConsistentHashFactory chFactory) {
      switch (topology.getPhase()) {
         case NO_REBALANCE:
            return topology.getCurrentCH();
         case TRANSITORY:
            // This is used to determine nodes that own the entries. In scattered cache (which uses transitory topology)
            // the pendingCH is used for reading but the nodes in there are not guaranteed to have the data yet.
            // CurrentCH should be safe - the nodes either have the data or the owner is unknown.
            return topology.getCurrentCH();
         case CONFLICT_RESOLUTION:
         case READ_OLD_WRITE_ALL:
            return topology.getCurrentCH();
         case READ_ALL_WRITE_ALL:
            return chFactory.union(topology.getCurrentCH(), topology.getPendingCH());
         case READ_NEW_WRITE_ALL:
            return topology.getPendingCH();
         default:
            throw new IllegalStateException();
      }
   }

   /**
    * Called when a node joins.
    */
   void onJoin(AvailabilityStrategyContext context, Address joiner);

   /**
    * Called when a node leaves gracefully.
    */
   void onGracefulLeave(AvailabilityStrategyContext context, Address leaver);

   /**
    * Called when the cluster view changed (e.g. because one or more nodes left abruptly).
    */
   void onClusterViewChange(AvailabilityStrategyContext context, List<Address> clusterMembers);

   /**
    * Called when two or more partitions merge, to compute the stable and current cache topologies for the merged
    * cluster.
    */
   void onPartitionMerge(AvailabilityStrategyContext context, Map<Address, CacheStatusResponse> statusResponseMap);

   /**
    * Called when a rebalance ends. Can be used to re-assess the state of the cache and apply pending changes.
    */
   void onRebalanceEnd(AvailabilityStrategyContext context);

   /**
    * Called when the administrator manually changes the availability status.
    */
   void onManualAvailabilityChange(AvailabilityStrategyContext context, AvailabilityMode newAvailabilityMode);
}
