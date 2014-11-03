package org.infinispan.partionhandling.impl;

import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.partionhandling.AvailabilityMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheStatusResponse;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PreferConsistencyStrategy implements AvailabilityStrategy {
   private static final Log log = LogFactory.getLog(PreferConsistencyStrategy.class);

   @Override
   public void onJoin(AvailabilityStrategyContext context, Address joiner) {
      if (context.getAvailabilityMode() != AvailabilityMode.AVAILABLE) {
         log.debugf("Cache %s not available (%s), postponing rebalance for joiner %s", context.getCacheName(),
               context.getAvailabilityMode(), joiner);
         return;
      }

      context.queueRebalance(context.getExpectedMembers());
   }

   @Override
   public void onGracefulLeave(AvailabilityStrategyContext context, Address leaver) {
      CacheTopology currentTopology = context.getCurrentTopology();
      List<Address> newMembers = new ArrayList<>(currentTopology.getMembers());
      newMembers.remove(leaver);
      if (newMembers.isEmpty()) {
         log.debugf("The last node of cache %s left", context.getCacheName());
         context.updateCurrentTopology(newMembers);
         return;
      }

      if (context.getAvailabilityMode() != AvailabilityMode.AVAILABLE) {
         log.debugf("Cache %s is not available, ignoring graceful leaver %s", context.getCacheName(), leaver);
         return;
      }

      if (isDataLost(context.getStableTopology().getCurrentCH(), newMembers)) {
         log.enteringUnavailableModeGracefulLeaver(context.getCacheName(), leaver);
         context.updateAvailabilityMode(AvailabilityMode.UNAVAILABLE, true);
         return;
      }

      updateMembersAndRebalance(context, newMembers);
   }

   @Override
   public void onClusterViewChange(AvailabilityStrategyContext context, List<Address> clusterMembers) {
      if (context.getAvailabilityMode() != AvailabilityMode.AVAILABLE) {
         log.debugf("Cache %s is not available, ignoring cluster view change", context.getCacheName());
         return;
      }

      CacheTopology currentTopology = context.getCurrentTopology();
      List<Address> newMembers = new ArrayList<>(currentTopology.getMembers());
      if (!newMembers.retainAll(clusterMembers)) {
         log.debugf("Cache %s did not lose any members, ignoring view change", context.getCacheName());
         return;
      }

      // We could keep track of the members that left gracefully and avoid entering degraded mode in some cases.
      // But the information about graceful leavers would be lost when the coordinator changes anyway, making
      // the results of the strategy harder to reason about.
      CacheTopology stableTopology = context.getStableTopology();
      List<Address> stableMembers = stableTopology.getMembers();
      List<Address> lostMembers = new ArrayList<>(stableMembers);
      lostMembers.removeAll(newMembers);
      if (isDataLost(stableTopology.getCurrentCH(), newMembers)) {
         log.enteringDegradedModeLostData(context.getCacheName(), lostMembers);
         context.updateAvailabilityMode(AvailabilityMode.DEGRADED_MODE, true);
         return;
      }
      if (isMinorityPartition(stableMembers, lostMembers)) {
         log.enteringDegradedModeMinorityPartition(context.getCacheName(), newMembers, lostMembers, stableMembers);
         context.updateAvailabilityMode(AvailabilityMode.DEGRADED_MODE, true);
         return;
      }

      // We got back to available mode (e.g. because there was a merge before, but the merge coordinator didn't
      // finish installing the merged topology).
      updateMembersAndRebalance(context, newMembers);
   }

   protected boolean isMinorityPartition(List<Address> stableMembers, List<Address> lostMembers) {
      return lostMembers.size() >= Math.ceil(stableMembers.size() / 2d);
   }

   @Override
   public void onPartitionMerge(AvailabilityStrategyContext context,
         Collection<CacheStatusResponse> statusResponses) {
      // Because of the majority check in onAbruptLeave, we assume that at most one partition was able to evolve
      // and install new cache topologies. The other(s) would have entered degraded mode, and they would keep
      // the original topology.
      // One scenario not covered is if two partitions started separately and they have completely different topologies.
      // In that case, there is no way to prevent the two partitions from having inconsistent data.
      int maxTopologyId = 0;
      CacheTopology maxStableTopology = null;
      CacheTopology maxActiveTopology = null;
      Set<CacheTopology> unavailableTopologies = new HashSet<>();
      Set<CacheTopology> degradedTopologies = new HashSet<>();
      for (CacheStatusResponse response : statusResponses) {
         CacheTopology partitionStableTopology = response.getStableTopology();
         if (partitionStableTopology == null) {
            // The node hasn't properly joined yet.
            continue;
         }
         if (maxStableTopology == null || maxStableTopology.getTopologyId() < partitionStableTopology.getTopologyId()) {
            maxStableTopology = partitionStableTopology;
         }

         CacheTopology partitionTopology = response.getCacheTopology();
         if (partitionTopology == null) {
            // The node hasn't properly joined yet.
            continue;
         }
         if (partitionTopology.getTopologyId() > maxTopologyId) {
            maxTopologyId = partitionTopology.getTopologyId();
         }
         if (response.getAvailabilityMode() == AvailabilityMode.AVAILABLE) {
            if (maxActiveTopology == null || maxActiveTopology.getTopologyId() < partitionTopology.getTopologyId()) {
               maxActiveTopology = partitionTopology;
            }
         } else if (response.getAvailabilityMode() == AvailabilityMode.DEGRADED_MODE) {
            degradedTopologies.add(partitionTopology);
         } else if (response.getAvailabilityMode() == AvailabilityMode.UNAVAILABLE) {
            unavailableTopologies.add(partitionTopology);
         } else {
            log.unexpectedAvailabilityMode(context.getAvailabilityMode(), context.getCacheName(),
                  response.getCacheTopology());
         }
      }

      if (maxStableTopology != null) {
         log.tracef("Max stable partition topology: %s", maxStableTopology);
      }
      if (maxActiveTopology != null) {
         log.tracef("Max active partition topology: %s", maxActiveTopology);
      }
      if (!degradedTopologies.isEmpty()) {
         log.tracef("Found degraded partition topologies: %s", degradedTopologies);
      }
      if (!unavailableTopologies.isEmpty()) {
         log.tracef("Found unavailable partition topologies: %s", unavailableTopologies);
      }

      CacheTopology mergedTopology;
      AvailabilityMode mergedAvailabilityMode;
      if (!unavailableTopologies.isEmpty()) {
         log.debugf("At least one of the partitions is unavailable, staying in unavailable mode");
         mergedAvailabilityMode = AvailabilityMode.UNAVAILABLE;
         mergedTopology = unavailableTopologies.iterator().next();
      } else if (maxActiveTopology != null) {
         log.debugf("One of the partitions is available, using that partition's topology");
         List<Address> newMembers = new ArrayList<>(maxActiveTopology.getMembers());
         newMembers.retainAll(context.getExpectedMembers());
         mergedAvailabilityMode = computeAvailabilityAfterMerge(context, maxStableTopology, newMembers);
         mergedTopology = maxActiveTopology;
      } else if (!degradedTopologies.isEmpty()) {
         log.debugf("No active or unavailable partitions, so all the partitions must be in degraded mode.");
         // We can't be in degraded mode without a stable topology
         List<Address> newMembers = new ArrayList<>(maxStableTopology.getMembers());
         newMembers.retainAll(context.getExpectedMembers());
         mergedAvailabilityMode = computeAvailabilityAfterMerge(context, maxStableTopology, newMembers);
         mergedTopology = maxStableTopology;
      } else {
         log.debugf("No current topology, recovered only joiners for cache %s", context.getCacheName());
         mergedAvailabilityMode = AvailabilityMode.AVAILABLE;
         mergedTopology = null;
      }

      // Increment the topology id so that it's bigger than any topology that might have been sent by the old
      // coordinator. +1 is enough because there nodes wait for the new JGroups view before answering the status
      // request, and after they have the new view they can't process topology updates with the old view id.
      // Also cancel any pending rebalance by removing the pending CH, because we don't recover the rebalance
      // confirmation status (yet).
      if (mergedTopology != null) {
         mergedTopology = new CacheTopology(maxTopologyId + 1, mergedTopology.getRebalanceId(),
               mergedTopology.getCurrentCH(), null);
      }
      context.updateTopologiesAfterMerge(mergedTopology, maxStableTopology, mergedAvailabilityMode);

      // It shouldn't be possible to recover from unavailable mode without user action
      if (mergedAvailabilityMode == AvailabilityMode.UNAVAILABLE) {
         log.debugf("After merge, cache %s is staying in unavailable mode", context.getCacheName());
         context.updateAvailabilityMode(AvailabilityMode.UNAVAILABLE, true);
         return;
      } else if (mergedAvailabilityMode == AvailabilityMode.AVAILABLE) {
         log.debugf("After merge, cache %s has recovered and is entering available mode");
         updateMembersAndRebalance(context, context.getExpectedMembers());
      }
      // Do nothing for DEGRADED_MODE
   }

   protected AvailabilityMode computeAvailabilityAfterMerge(AvailabilityStrategyContext context,
         CacheTopology maxStableTopology, List<Address> newMembers) {
      if (maxStableTopology != null) {
         List<Address> stableMembers = maxStableTopology.getMembers();
         List<Address> lostMembers = new ArrayList<>(stableMembers);
         lostMembers.removeAll(context.getExpectedMembers());
         if (isDataLost(maxStableTopology.getCurrentCH(), newMembers)) {
            log.keepingDegradedModeAfterMergeDataLost(context.getCacheName(), newMembers, lostMembers, stableMembers);
            return AvailabilityMode.DEGRADED_MODE;
         }
         if (lostMembers.size() >= Math.ceil(stableMembers.size() / 2d)) {
            log.keepingDegradedModeAfterMergeMinorityPartition(context.getCacheName(), newMembers, lostMembers,
                  stableMembers);
            return AvailabilityMode.DEGRADED_MODE;
         }
      }
      return AvailabilityMode.AVAILABLE;
   }

   @Override
   public void onRebalanceEnd(AvailabilityStrategyContext context) {
      // We may have a situation where 2 nodes leave in sequence, and the rebalance for the first leave only finishes
      // after the second node left (and we entered degraded/unavailable mode).
      // For now, we ignore the rebalance and we keep the cache in degraded/unavailable mode.
      // Don't need to queue another rebalance, if we need another rebalance it's already in the queue
   }

   @Override
   public void onManualAvailabilityChange(AvailabilityStrategyContext context, AvailabilityMode newAvailabilityMode) {
      if (newAvailabilityMode == AvailabilityMode.AVAILABLE) {
         List<Address> newMembers = context.getExpectedMembers();
         // Update the topology to remove leavers - the current topology may not have been updated for a while
         context.updateCurrentTopology(newMembers);
         context.updateAvailabilityMode(newAvailabilityMode, false);
         // Then queue a rebalance to include the joiners as well
         context.queueRebalance(newMembers);
      } else {
         context.updateAvailabilityMode(newAvailabilityMode, true);
      }
   }

   private void updateMembersAndRebalance(AvailabilityStrategyContext context, List<Address> newMembers) {
      // Change the availability mode if needed
      context.updateAvailabilityMode(AvailabilityMode.AVAILABLE, false);

      // Update the topology to remove leavers - in case there is a rebalance in progress, or rebalancing is disabled
      context.updateCurrentTopology(newMembers);
      // Then queue a rebalance to include the joiners as well
      context.queueRebalance(context.getExpectedMembers());
   }

   private boolean isDataLost(ConsistentHash currentCH, List<Address> newMembers) {
      for (int i = 0; i < currentCH.getNumSegments(); i++) {
         if (!InfinispanCollections.containsAny(newMembers, currentCH.locateOwnersForSegment(i)))
            return true;
      }
      return false;
   }
}
