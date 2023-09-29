package org.infinispan.partitionhandling.impl;

import static org.infinispan.partitionhandling.impl.AvailabilityStrategy.ownersConsistentHash;
import static org.infinispan.util.logging.events.Messages.MESSAGES;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheJoinInfo;
import org.infinispan.topology.CacheStatusResponse;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.PersistentUUIDManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogManager;

public class PreferConsistencyStrategy implements AvailabilityStrategy {
   private static final Log log = LogFactory.getLog(PreferConsistencyStrategy.class);
   private final EventLogManager eventLogManager;
   private final PersistentUUIDManager persistentUUIDManager;
   private final LostDataCheck lostDataCheck;

   public PreferConsistencyStrategy(EventLogManager eventLogManager, PersistentUUIDManager persistentUUIDManager, LostDataCheck lostDataCheck) {
      this.eventLogManager = eventLogManager;
      this.persistentUUIDManager = persistentUUIDManager;
      this.lostDataCheck = lostDataCheck;
   }

   @Override
   public void onJoin(AvailabilityStrategyContext context, Address joiner) {
      if (context.getAvailabilityMode() != AvailabilityMode.AVAILABLE) {
         // If the cluster was manually put into degraded mode, we don't do anything.
         // The node is able to join, but without rebalance and state transfer, everything remains degraded.
         if (context.isManuallyDegraded()) {
            log.debugf("Cache %s not available (%s), postponing rebalance for joiner %s", context.getCacheName(),
                  context.getAvailabilityMode(), joiner);
            return;
         }

         CacheTopology stableTopology = context.getStableTopology();
         List<Address> currentMembers = new ArrayList<>(context.getExpectedMembers());

         // Ignore members without any segments (e.g. zero-capacity nodes)
         currentMembers.removeIf(a -> {
            Float cf = context.getCapacityFactors().get(a);
            return cf == null || cf == 0f;
         });

         // Nodes might be joining back with different addresses, we utilize the persistent UUID to verify.
         ConsistentHash ch = stableTopology.getCurrentCH().remapAddresses(persistentUUIDManager.addressToPersistentUUID());
         List<Address> membersUuid = currentMembers.stream()
               .map(persistentUUIDManager::getPersistentUuid)
               .collect(Collectors.toList());

         // Not losing any data with the members. We utilize the persistent UUID to verify.
         if (!lostDataCheck.test(ch, membersUuid)) {
            List<Address> lost = new ArrayList<>(stableTopology.getMembers()).stream()
                  .map(persistentUUIDManager::getPersistentUuid)
                  .filter(m -> !membersUuid.contains(m))
                  .collect(Collectors.toList());

            // We know we are not losing data, but doing the inverse check from the partition.
            // We check if there is still a partition taking place and only recover if it is safe.
            if (!isMinorityPartition(currentMembers, lost)) {
               if (log.isDebugEnabled())
                  log.debugf("Cache %s was unavailable (%s), members joined back %s", context.getCacheName(),
                        context.getAvailabilityMode(), currentMembers);

               updateMembersAndRebalance(context, context.getCurrentTopology().getMembers(), context.getExpectedMembers());
               return;
            }
         }

         if (log.isDebugEnabled())
            log.debugf("Cache %s not available (%s), still pending member %s current %s", context.getCacheName(),
                  context.getAvailabilityMode(), stableTopology.getMembers(), currentMembers);

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

      if (lostDataCheck.test(context.getStableTopology().getCurrentCH(), newMembers)) {
         eventLogManager.getEventLogger().context(context.getCacheName()).warn(EventLogCategory.CLUSTER, MESSAGES.enteringDegradedModeGracefulLeaver(leaver));
         context.updateAvailabilityMode(newMembers, AvailabilityMode.DEGRADED_MODE, true);
         return;
      }

      updateMembersAndRebalance(context, newMembers, newMembers);
   }

   @Override
   public void onClusterViewChange(AvailabilityStrategyContext context, List<Address> clusterMembers) {
      CacheTopology currentTopology = context.getCurrentTopology();
      List<Address> newMembers = new ArrayList<>(currentTopology.getMembers());
      if (!newMembers.retainAll(clusterMembers)) {
         log.debugf("Cache %s did not lose any members, ignoring view change", context.getCacheName());
         return;
      }

      if (context.getAvailabilityMode() != AvailabilityMode.AVAILABLE) {
         log.debugf("Cache %s is not available, updating the actual members only", context.getCacheName());
         context.updateAvailabilityMode(newMembers, context.getAvailabilityMode(), false);
         return;
      }

      // We could keep track of the members that left gracefully and avoid entering degraded mode in some cases.
      // But the information about graceful leavers would be lost when the coordinator changes anyway, making
      // the results of the strategy harder to reason about.
      CacheTopology stableTopology = context.getStableTopology();
      List<Address> stableMembers = new ArrayList<>(stableTopology.getMembers());
      // Ignore members without any segments (e.g. zero-capacity nodes)
      stableMembers.removeIf(a -> stableTopology.getCurrentCH().getSegmentsForOwner(a).isEmpty());
      List<Address> lostMembers = new ArrayList<>(stableMembers);
      lostMembers.removeAll(newMembers);
      if (lostDataCheck.test(stableTopology.getCurrentCH(), newMembers)) {
         eventLogManager.getEventLogger().context(context.getCacheName()).error(EventLogCategory.CLUSTER, MESSAGES.enteringDegradedModeLostData(lostMembers));
         context.updateAvailabilityMode(newMembers, AvailabilityMode.DEGRADED_MODE, true);
         return;
      }
      if (isMinorityPartition(stableMembers, lostMembers)) {
         eventLogManager.getEventLogger().context(context.getCacheName()).error(EventLogCategory.CLUSTER, MESSAGES.enteringDegradedModeMinorityPartition(newMembers, lostMembers, stableMembers));
         context.updateAvailabilityMode(newMembers, AvailabilityMode.DEGRADED_MODE, true);
         return;
      }

      // We got back to available mode (e.g. because there was a merge before, but the merge coordinator didn't
      // finish installing the merged topology).
      updateMembersAndRebalance(context, newMembers, newMembers);
   }

   protected boolean isMinorityPartition(List<Address> stableMembers, List<Address> lostMembers) {
      return lostMembers.size() >= Math.ceil(stableMembers.size() / 2d);
   }

   @Override
   public void onPartitionMerge(AvailabilityStrategyContext context, Map<Address, CacheStatusResponse> statusResponseMap) {
      // Because of the majority check in onAbruptLeave, we assume that at most one partition was able to evolve
      // and install new cache topologies. The other(s) would have entered degraded mode, and they would keep
      // the original topology.
      int maxTopologyId = 0;
      int maxRebalanceId = 0;
      CacheTopology maxStableTopology = null;
      CacheTopology maxActiveTopology = null;
      Set<CacheTopology> degradedTopologies = new HashSet<>();
      CacheTopology maxDegradedTopology = null;
      CacheJoinInfo joinInfo = null;
      for (CacheStatusResponse response : statusResponseMap.values()) {
         CacheTopology partitionStableTopology = response.getStableTopology();
         if (partitionStableTopology == null) {
            // The node hasn't properly joined yet.
            continue;
         }
         if (maxStableTopology == null || maxStableTopology.getTopologyId() < partitionStableTopology.getTopologyId()) {
            maxStableTopology = partitionStableTopology;
            joinInfo = response.getCacheJoinInfo();
         }

         CacheTopology partitionTopology = response.getCacheTopology();
         if (partitionTopology == null) {
            // The node hasn't properly joined yet.
            continue;
         }
         if (partitionTopology.getTopologyId() > maxTopologyId) {
            maxTopologyId = partitionTopology.getTopologyId();
         }
         if (partitionTopology.getRebalanceId() > maxRebalanceId) {
            maxRebalanceId = partitionTopology.getRebalanceId();
         }
         if (response.getAvailabilityMode() == AvailabilityMode.AVAILABLE) {
            if (maxActiveTopology == null || maxActiveTopology.getTopologyId() < partitionTopology.getTopologyId()) {
               maxActiveTopology = partitionTopology;
            }
         } else if (response.getAvailabilityMode() == AvailabilityMode.DEGRADED_MODE) {
            degradedTopologies.add(partitionTopology);
            if (maxDegradedTopology == null || maxDegradedTopology.getTopologyId() < partitionTopology.getTopologyId()) {
               maxDegradedTopology = partitionTopology;
            }
         } else {
            eventLogManager.getEventLogger().context(context.getCacheName()).error(EventLogCategory.CLUSTER, MESSAGES.unexpectedAvailabilityMode(context.getAvailabilityMode(), response.getCacheTopology()));
         }
      }

      if (maxStableTopology != null) {
         log.tracef("Max stable partition topology: %s", maxStableTopology);
      }
      if (maxActiveTopology != null) {
         log.tracef("Max active partition topology: %s", maxActiveTopology);
      }
      if (maxDegradedTopology != null) {
         log.tracef("Max degraded partition topology: %s, all degraded: %s", maxDegradedTopology, degradedTopologies);
      }

      List<Address> expectedMembers = context.getExpectedMembers();
      List<Address> actualMembers = new ArrayList<>(expectedMembers);
      CacheTopology mergedTopology;
      AvailabilityMode mergedAvailabilityMode;
      if (maxActiveTopology != null) {
         log.debugf("One of the partitions is available, using that partition's topology");
         mergedTopology = maxActiveTopology;
         mergedAvailabilityMode = AvailabilityMode.AVAILABLE;
      } else if (!degradedTopologies.isEmpty()) {
         log.debugf("No active partitions, so all the partitions must be in degraded mode.");
         // Once a partition enters degraded mode its CH won't change, but it could be that a partition managed to
         // rebalance before losing another member and entering degraded mode.
         mergedTopology = maxDegradedTopology;
         if (maxStableTopology != null) {
            actualMembers.retainAll(maxStableTopology.getMembers());
         } else {
            actualMembers.retainAll(mergedTopology.getMembers());
         }
         mergedAvailabilityMode = AvailabilityMode.DEGRADED_MODE;
      } else {
         log.debugf("No current topology, recovered only joiners for cache %s. Skipping availability update.", context.getCacheName());
         return;
      }

      // Increment the topology id so that it's bigger than any topology that might have been sent by the old
      // coordinator.
      // +1 is enough because nodes wait for the new JGroups view before answering the
      // status request, and then they can't process topology updates from the old view.
      // Also cancel any pending rebalance by removing the pending CH, because we don't recover the rebalance
      // confirmation status (yet).
      AvailabilityMode newAvailabilityMode = computeAvailabilityAfterMerge(context, maxStableTopology, actualMembers);
      if (mergedTopology != null) {
         boolean resolveConflicts = context.resolveConflictsOnMerge() && actualMembers.size() > 1 && newAvailabilityMode == AvailabilityMode.AVAILABLE;
         if (resolveConflicts) {
            // Record all distinct read owners and hashes
            Set<ConsistentHash> distinctHashes = new HashSet<>();
            for (CacheStatusResponse response : statusResponseMap.values()) {
               CacheTopology cacheTopology = response.getCacheTopology();
               if (cacheTopology != null) {
                  ConsistentHash readCH = ownersConsistentHash(cacheTopology, joinInfo.getConsistentHashFactory());
                  if (readCH != null && !readCH.getMembers().isEmpty()) {
                     distinctHashes.add(readCH);
                  }
               }
            }
            ConsistentHash preferredHash = ownersConsistentHash(mergedTopology, joinInfo.getConsistentHashFactory());
            ConsistentHash conflictHash = context.calculateConflictHash(preferredHash, distinctHashes, expectedMembers);

            mergedTopology = new CacheTopology(++maxTopologyId, maxRebalanceId + 1, conflictHash, null,
                  CacheTopology.Phase.CONFLICT_RESOLUTION, actualMembers, persistentUUIDManager.mapAddresses(actualMembers));

            // Update the currentTopology and try to resolve conflicts
            context.updateTopologiesAfterMerge(mergedTopology, maxStableTopology, mergedAvailabilityMode);
            context.queueConflictResolution(mergedTopology, new HashSet<>(preferredHash.getMembers()));
            return;
         }

         // There's no pendingCH, therefore the topology is in stable phase
         actualMembers.retainAll(mergedTopology.getMembers());
         mergedTopology = new CacheTopology(maxTopologyId + 1, mergedTopology.getRebalanceId(),
                                            mergedTopology.getCurrentCH(), null,
                                            CacheTopology.Phase.NO_REBALANCE, actualMembers,
                                            persistentUUIDManager.mapAddresses(actualMembers));
      }

      context.updateTopologiesAfterMerge(mergedTopology, maxStableTopology, mergedAvailabilityMode);

      // It shouldn't be possible to recover from unavailable mode without user action
      if (newAvailabilityMode == AvailabilityMode.DEGRADED_MODE) {
         log.debugf("After merge, cache %s is staying in degraded mode", context.getCacheName());
         context.updateAvailabilityMode(actualMembers, newAvailabilityMode, true);
      } else { // AVAILABLE
         log.debugf("After merge, cache %s has recovered and is entering available mode", context.getCacheName());
         updateMembersAndRebalance(context, actualMembers, context.getExpectedMembers());
      }
   }

   private AvailabilityMode computeAvailabilityAfterMerge(AvailabilityStrategyContext context,
                                                          CacheTopology maxStableTopology, List<Address> newMembers) {
      if (maxStableTopology != null) {
         List<Address> stableMembers = maxStableTopology.getMembers();
         List<Address> lostMembers = new ArrayList<>(stableMembers);
         lostMembers.removeAll(context.getExpectedMembers());
         if (lostDataCheck.test(maxStableTopology.getCurrentCH(), newMembers)) {
            eventLogManager.getEventLogger().context(context.getCacheName()).error(EventLogCategory.CLUSTER, MESSAGES.keepingDegradedModeAfterMergeDataLost(newMembers, lostMembers, stableMembers));
            return AvailabilityMode.DEGRADED_MODE;
         }
         if (lostMembers.size() >= Math.ceil(stableMembers.size() / 2d)) {
            eventLogManager.getEventLogger().context(context.getCacheName()).warn(EventLogCategory.CLUSTER, MESSAGES.keepingDegradedModeAfterMergeMinorityPartition(newMembers, lostMembers,
                  stableMembers));
            return AvailabilityMode.DEGRADED_MODE;
         }
      }
      return AvailabilityMode.AVAILABLE;
   }

   @Override
   public void onRebalanceEnd(AvailabilityStrategyContext context) {
      // We may have a situation where 2 nodes leave in sequence, and the rebalance for the first leave only finishes
      // after the second node left (and we entered degraded mode).
      // For now, we ignore the rebalance and we keep the cache in degraded mode.
      // Don't need to queue another rebalance, if we need another rebalance it's already in the queue
   }

   @Override
   public void onManualAvailabilityChange(AvailabilityStrategyContext context, AvailabilityMode newAvailabilityMode) {
      List<Address> actualMembers = context.getCurrentTopology().getActualMembers();
      List<Address> newMembers = context.getExpectedMembers();
      if (newAvailabilityMode == AvailabilityMode.AVAILABLE) {
         // Update the topology to remove leavers - the current topology may not have been updated for a while
         context.updateCurrentTopology(actualMembers);
         context.updateAvailabilityMode(actualMembers, newAvailabilityMode, false);
         // Then queue a rebalance to include the joiners as well
         context.queueRebalance(newMembers);
      } else {
         context.manuallyUpdateAvailabilityMode(actualMembers, newAvailabilityMode, true);
      }
   }

   private void updateMembersAndRebalance(AvailabilityStrategyContext context, List<Address> actualMembers,
         List<Address> newMembers) {
      // Change the availability mode if needed
      context.updateAvailabilityMode(actualMembers, AvailabilityMode.AVAILABLE, false);

      // Update the topology to remove leavers - in case there is a rebalance in progress, or rebalancing is disabled
      context.updateCurrentTopology(newMembers);
      // Then queue a rebalance to include the joiners as well
      context.queueRebalance(context.getExpectedMembers());
   }
}
