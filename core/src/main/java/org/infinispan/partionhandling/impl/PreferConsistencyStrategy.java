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
import java.util.List;

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
      if (context.getAvailabilityMode() != AvailabilityMode.AVAILABLE) {
         log.debugf("Cache %s is not available, ignoring graceful leaver %s", context.getCacheName(), leaver);
         return;
      }

      CacheTopology currentTopology = context.getCurrentTopology();
      List<Address> newMembers = new ArrayList<>(currentTopology.getMembers());
      newMembers.retainAll(context.getExpectedMembers());

      if (isDataLost(context.getStableTopology().getCurrentCH(), newMembers)) {
         log.enteringUnavailableModeGracefulLeaver(context.getCacheName(), leaver);
         context.updateAvailabilityMode(AvailabilityMode.UNAVAILABLE);
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
         context.updateAvailabilityMode(AvailabilityMode.DEGRADED_MODE);
         return;
      }
      if (isMinorityPartition(stableMembers, lostMembers)) {
         log.enteringDegradedModeMinorityPartition(context.getCacheName(), newMembers, lostMembers, stableMembers);
         context.updateAvailabilityMode(AvailabilityMode.DEGRADED_MODE);
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
      CacheTopology maxActiveTopology = null;
      CacheTopology maxDegradedTopology = null;
      CacheTopology maxUnavailableTopology = null;
      CacheTopology maxStableTopology = null;
      for (CacheStatusResponse response : statusResponses) {
         CacheTopology partitionStableTopology = response.getStableTopology();
         if (maxStableTopology == null || !maxStableTopology.equals(partitionStableTopology)) {
            log.tracef("Found stable partition topology: %s", maxStableTopology);
         }
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
         if (response.getAvailabilityMode() == AvailabilityMode.AVAILABLE) {
            if (maxActiveTopology == null || !maxActiveTopology.equals(partitionTopology)) {
               log.tracef("Found active partition topology: %s", maxActiveTopology);
            }
            if (maxActiveTopology == null || maxActiveTopology.getTopologyId() < partitionTopology.getTopologyId()) {
               maxActiveTopology = partitionTopology;
            }
         } else if (response.getAvailabilityMode() == AvailabilityMode.DEGRADED_MODE) {
            if (maxDegradedTopology == null || !maxDegradedTopology.equals(partitionTopology)) {
               log.tracef("Found degraded partition topology: %s", maxDegradedTopology);
            }
            if (maxDegradedTopology == null || maxDegradedTopology.getTopologyId() < partitionTopology.getTopologyId()) {
               maxDegradedTopology = partitionTopology;
            }
         } else if (response.getAvailabilityMode() == AvailabilityMode.UNAVAILABLE) {
            if (maxUnavailableTopology == null || !maxUnavailableTopology.equals(partitionTopology)) {
               log.tracef("Found unavailable partition topology: %s", maxUnavailableTopology);
            }
            if (maxUnavailableTopology == null || maxUnavailableTopology.getTopologyId() < partitionTopology.getTopologyId()) {
               maxUnavailableTopology = partitionTopology;
            }
         } else {
            log.unexpectedAvailabilityMode(context.getAvailabilityMode(), context.getCacheName(),
                  response.getCacheTopology());
         }
      }

      CacheTopology mergedTopology;
      AvailabilityMode mergedAvailabilityMode;
      if (maxUnavailableTopology != null) {
         log.debugf("One of the partitions is unavailable, using that partition's topology and staying in unavailable mode");
         mergedAvailabilityMode = AvailabilityMode.UNAVAILABLE;
         mergedTopology = maxUnavailableTopology;
      } else if (maxActiveTopology != null) {
         log.debugf("One of the partitions is available, using that partition's topology and staying in available mode");
         mergedAvailabilityMode = AvailabilityMode.AVAILABLE;
         mergedTopology = maxActiveTopology;
      } else if (maxDegradedTopology != null) {
         log.debugf("No active or unavailable partitions, so all the partitions must be in degraded mode.");
         mergedAvailabilityMode = AvailabilityMode.DEGRADED_MODE;
         mergedTopology = maxDegradedTopology;
      } else {
         log.debugf("No current topology, recovered only joiners for cache %s", context.getCacheName());
         mergedAvailabilityMode = AvailabilityMode.AVAILABLE;
         mergedTopology = null;
      }

      // Cancel any pending rebalance by removing the pending CH.
      // Needed because we don't recover the rebalance confirmation status (yet).
      // By definition, the stable topology doesn't have a rebalance in progress.
      if (mergedTopology != null && mergedTopology.getPendingCH() != null) {
         mergedTopology = new CacheTopology(mergedTopology.getTopologyId() + 1, mergedTopology.getRebalanceId(),
               mergedTopology.getCurrentCH(), null);
      }

      log.debugf("Updating topologies after merge for cache %s, current topology = %s, stable topology = %s, availability mode = %s",
            context.getCacheName(), mergedTopology, maxStableTopology, mergedAvailabilityMode);
      context.updateTopologiesAfterMerge(mergedTopology, maxStableTopology, mergedAvailabilityMode);

      // It shouldn't be possible to recover from unavailable mode without user action
      if (mergedAvailabilityMode == AvailabilityMode.UNAVAILABLE) {
         log.debugf("After merge, cache %s is staying in unavailable mode", context.getCacheName());
         context.updateAvailabilityMode(AvailabilityMode.UNAVAILABLE);
         return;
      }

      List<Address> newMembers = new ArrayList<>(mergedTopology.getMembers());
      newMembers.retainAll(context.getExpectedMembers());
      if (maxStableTopology != null) {
         List<Address> stableMembers = maxStableTopology.getMembers();
         List<Address> lostMembers = new ArrayList<>(stableMembers);
         lostMembers.removeAll(context.getExpectedMembers());
         if (isDataLost(maxStableTopology.getCurrentCH(), newMembers)) {
            log.keepingDegradedModeAfterMergeDataLost(context.getCacheName(), newMembers, lostMembers, stableMembers);
            context.updateAvailabilityMode(AvailabilityMode.DEGRADED_MODE);
            return;
         }
         if (lostMembers.size() >= Math.ceil(stableMembers.size() / 2d)) {
            log.keepingDegradedModeAfterMergeMinorityPartition(context.getCacheName(), newMembers, lostMembers,
                  stableMembers);
            context.updateAvailabilityMode(AvailabilityMode.DEGRADED_MODE);
            return;
         }
      }

      // Get back to available mode
      log.debugf("After merge, cache %s has recovered and is entering available mode");
      updateMembersAndRebalance(context, context.getExpectedMembers());
   }

   @Override
   public void onRebalanceEnd(AvailabilityStrategyContext context) {
      // We may have a situation where 2 nodes leave in sequence, and the rebalance for the first leave only finishes
      // after the second node left (and we entered degraded/unavailable mode).
      // For now, we ignore the rebalance and we keep the cache in degraded/unavailable mode.
      // Don't need to queue another rebalance, if we need another rebalance it's already in the queue
   }

   @Override
   public void onManualAvailabilityChange(AvailabilityStrategyContext context) {
      updateMembersAndRebalance(context, context.getExpectedMembers());
   }

   private void updateMembersAndRebalance(AvailabilityStrategyContext context, List<Address> newMembers) {
      // Change the availability mode if needed
      context.updateAvailabilityMode(AvailabilityMode.AVAILABLE);

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
