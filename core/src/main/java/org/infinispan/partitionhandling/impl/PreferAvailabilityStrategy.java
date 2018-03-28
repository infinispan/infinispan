package org.infinispan.partitionhandling.impl;

import static org.infinispan.partitionhandling.impl.AvailabilityStrategy.readConsistentHash;
import static org.infinispan.util.logging.events.Messages.MESSAGES;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheStatusResponse;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.PersistentUUIDManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.util.logging.events.EventLogCategory;
import org.infinispan.util.logging.events.EventLogManager;

public class PreferAvailabilityStrategy implements AvailabilityStrategy {
   private static final Log log = LogFactory.getLog(PreferAvailabilityStrategy.class);
   private static final boolean trace = log.isTraceEnabled();

   private final EventLogManager eventLogManager;
   private final PersistentUUIDManager persistentUUIDManager;
   private final LostDataCheck lostDataCheck;

   public PreferAvailabilityStrategy(EventLogManager eventLogManager, PersistentUUIDManager persistentUUIDManager, LostDataCheck lostDataCheck) {
      this.eventLogManager = eventLogManager;
      this.persistentUUIDManager = persistentUUIDManager;
      this.lostDataCheck = lostDataCheck;
   }

   @Override
   public void onJoin(AvailabilityStrategyContext context, Address joiner) {
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
      if (context.getStableTopology() != null && lostDataCheck.test(context.getStableTopology().getCurrentCH(), newMembers)) {
         eventLogManager.getEventLogger().context(context.getCacheName()).warn(EventLogCategory.CLUSTER, MESSAGES.lostDataBecauseOfGracefulLeaver(leaver));
      }

      // We have to do this in case rebalancing is disabled, or there is another rebalance in progress
      context.updateCurrentTopology(newMembers);
      context.queueRebalance(newMembers);
   }

   @Override
   public void onClusterViewChange(AvailabilityStrategyContext context, List<Address> clusterMembers) {
      CacheTopology currentTopology = context.getCurrentTopology();
      List<Address> newMembers = new ArrayList<>(currentTopology.getMembers());
      if (!newMembers.retainAll(clusterMembers)) {
         if (trace) log.tracef("Cache %s did not lose any members, skipping rebalance", context.getCacheName());
         return;
      }

      checkForLostData(context.getCacheName(), context.getStableTopology(), newMembers);

      // We have to do the update in case rebalancing is disabled, or there is another rebalance in progress
      context.updateCurrentTopology(newMembers);
      context.queueRebalance(newMembers);
   }

   protected void checkForLostData(String cacheName, CacheTopology stableTopology, List<Address> newMembers) {
      List<Address> stableMembers = stableTopology.getMembers();
      List<Address> lostMembers = new ArrayList<>(stableMembers);
      lostMembers.removeAll(newMembers);
      if (lostDataCheck.test(stableTopology.getCurrentCH(), newMembers)) {
         eventLogManager.getEventLogger().context(cacheName).fatal(EventLogCategory.CLUSTER, MESSAGES.lostDataBecauseOfAbruptLeavers(lostMembers));
      } else if (lostMembers.size() >= Math.ceil(stableMembers.size() / 2d)) {
         eventLogManager.getEventLogger().context(cacheName).warn(EventLogCategory.CLUSTER, MESSAGES.minorityPartition(newMembers, lostMembers, stableMembers));
      }
   }

   @Override
   public void onPartitionMerge(AvailabilityStrategyContext context,
                                Map<Address, CacheStatusResponse> statusResponseMap) {
      String cacheName = context.getCacheName();
      List<Address> newMembers = context.getExpectedMembers();

      List<Partition> partitions = computePartitions(statusResponseMap, cacheName);

      if (partitions.size() == 0) {
         log.debugf("No current topology, recovered only joiners for cache %s", cacheName);
         context.updateCurrentTopology(newMembers);

         // Then start a rebalance with the expected members
         context.queueRebalance(newMembers);

         return;
      }

      if (partitions.size() == 1) {
         // Single partition, only the coordinator changed
         // Usually this means the old coordinator left the cluster, but the other nodes are all alive
         Partition p = partitions.get(0);
         log.debugf("Recovered a single partition for cache %s: %s", cacheName, p.topology);

         // Cancel any pending rebalance and only use the read CH,
         // because we don't recover the rebalance confirmation status (yet).
         List<Address> survivingMembers = new ArrayList<>(newMembers);
         if (survivingMembers.retainAll(p.readCH.getMembers())) {
            checkForLostData(cacheName, p.stableTopology, survivingMembers);
         }
         CacheTopology mergedTopology = new CacheTopology(p.topology.getTopologyId() + 1,
                                                          p.topology.getRebalanceId() + 1,
                                                          p.readCH, null, null,
                                                          CacheTopology.Phase.NO_REBALANCE,
                                                          survivingMembers,
                                                          persistentUUIDManager.mapAddresses(survivingMembers));

         context.updateTopologiesAfterMerge(mergedTopology, p.stableTopology, null, false);

         if (survivingMembers.isEmpty()) {
            // No surviving members, use the expected members instead
            survivingMembers = newMembers;
         }
         // Remove leavers first
         if (!survivingMembers.equals(p.topology.getMembers())) {
            context.updateCurrentTopology(survivingMembers);
         }

         // Then start a rebalance with the expected members
         context.queueRebalance(newMembers);

         return;
      }

      // Merge with multiple topologies that are not clear descendants of each other
      Partition preferredPartition = selectPreferredPartition(partitions);

      // Increment the topology id so that it's bigger than any topology that might have been sent by the old
      // coordinator. +1 is enough because there nodes wait for the new JGroups view before answering the status
      // request, and after they have the new view they can't process topology updates with the old view id.
      int mergeTopologyId = 0;
      int mergeRebalanceId = 0;
      for (Partition p : partitions) {
         CacheTopology topology = p.topology;
         if (topology != null) {
            if (mergeTopologyId <= topology.getTopologyId()) {
               mergeTopologyId = topology.getTopologyId() + 1;
            }
            if (mergeRebalanceId <= topology.getRebalanceId()) {
               mergeRebalanceId = topology.getRebalanceId() + 1;
            }
         }
      }

      // Record all distinct read owners and hashes for conflict resolution
      Set<Address> possibleOwners = new HashSet<>();
      Set<ConsistentHash> distinctHashes = new HashSet<>();
      for (Partition p : partitions) {
         possibleOwners.addAll(p.readCH.getMembers());
         distinctHashes.add(p.readCH);
      }

      // Remove nodes that have not yet fully rejoined the cluster
      possibleOwners.retainAll(newMembers);
      boolean resolveConflicts = context.resolveConflictsOnMerge() && possibleOwners.size() > 1;
      if (trace) {
         log.tracef("Cache %s, resolveConflicts=%s, newMembers=%s, possibleOwners=%s, preferredTopology=%s, " +
                    "mergeTopologyId=%s",
                    cacheName, resolveConflicts, newMembers, possibleOwners, preferredPartition.topology,
                    mergeTopologyId);
      }
      List<Address> actualMembers = new ArrayList<>(newMembers);
      CacheTopology mergedTopology;
      if (resolveConflicts) {
         actualMembers.retainAll(possibleOwners);

         // Start conflict resolution by using the preferred topology's read CH as a base
         // And the union of all distinct consistent hashes as the source
         ConsistentHash conflictHash = context.calculateConflictHash(preferredPartition.readCH, distinctHashes);
         mergedTopology = new CacheTopology(mergeTopologyId, mergeRebalanceId, preferredPartition.readCH,
                                            conflictHash, conflictHash, CacheTopology.Phase.CONFLICT_RESOLUTION,
                                            actualMembers, persistentUUIDManager.mapAddresses(actualMembers));
      } else {
         actualMembers.retainAll(preferredPartition.readCH.getMembers());

         for (Partition p : partitions) {
            if (p != preferredPartition) {
               log.ignoringCacheTopology(p.senders, p.topology);
            }
         }

         // Cancel any pending rebalance and only use the read CH,
         // because we don't recover the rebalance confirmation status (yet).
         mergedTopology = new CacheTopology(mergeTopologyId, mergeRebalanceId, preferredPartition.readCH, null,
                                            CacheTopology.Phase.NO_REBALANCE, actualMembers,
                                            persistentUUIDManager.mapAddresses(actualMembers));
      }

      context.updateTopologiesAfterMerge(mergedTopology, preferredPartition.stableTopology, null, resolveConflicts);

      // First update the CHs to remove any nodes that left from the current topology
      if (!actualMembers.containsAll(preferredPartition.readCH.getMembers())) {
         checkForLostData(cacheName, preferredPartition.stableTopology, actualMembers);
      }

      // No need to update topology if conflict resolution has already occurred because pendingCh != null
      if (!resolveConflicts) {
         assert !actualMembers.isEmpty();

         // Initialize the cache with the joiners
         context.updateCurrentTopology(actualMembers);
      }

      // Then start a rebalance with the merged members
      context.queueRebalance(newMembers);
   }

   private Partition selectPreferredPartition(List<Partition> partitions) {
      Partition preferredPartition = null;
      for (Partition p : partitions) {
         // TODO Investigate comparing the number of segments owned by the senders +
         // the number of the number of segments for partition(senders includes owner) agrees
         if (preferredPartition == null) {
            preferredPartition = p;
         } else if (preferredPartition.senders.size() < p.senders.size()) {
            preferredPartition = p;
         } else if (preferredPartition.senders.size() == p.senders.size() &&
                    preferredPartition.actualMembers.size() < p.actualMembers.size()) {
            preferredPartition = p;
         } else if (preferredPartition.topology.getTopologyId() < p.topology.getTopologyId()) {
            // Partitions are already sorted in reverse topology order,
            // but we make an explicit check for clarity
            preferredPartition = p;
         }
      }
      assert preferredPartition != null;
      return preferredPartition;
   }

   /**
    * Ignore the AvailabilityStrategyContext and only compute the preferred topology for testing.
    *
    * @return The preferred topology, or {@code null} if there is no preferred topology.
    */
   public CacheTopology computePreferredTopology(Map<Address, CacheStatusResponse> statusResponseMap) {
      List<Partition> partitions = computePartitions(statusResponseMap, "");

      return partitions.size() != 0 ? selectPreferredPartition(partitions).topology : null;
   }

   private List<Partition> computePartitions(Map<Address, CacheStatusResponse> statusResponseMap,
                                             String cacheName) {
      // The topology is not updated atomically across the cluster, so even members of the same partition
      // can report different topologies
      List<Partition> partitions = new ArrayList<>();
      for (Map.Entry<Address, CacheStatusResponse> e : statusResponseMap.entrySet()) {
         Address sender = e.getKey();
         CacheStatusResponse response = e.getValue();
         CacheTopology topology = response.getCacheTopology();
         if (topology == null || !topology.getMembers().contains(sender)) {
            // The node hasn't properly joined yet, so it can't be part of a partition
            continue;
         }
         Partition p = new Partition();
         p.topology = topology;
         p.stableTopology = response.getStableTopology();
         // The topologies used here do not have a union CH, so CacheTopology.getReadConsistentHash() doesn't work
         p.readCH = readConsistentHash(topology, response.getCacheJoinInfo().getConsistentHashFactory());
         p.actualMembers = topology.getActualMembers();
         p.senders.add(sender);
         partitions.add(p);
      }

      // Sort partitions in reverse topology id order to simplify the algorithm
      partitions.sort((p1, p2) -> {
         if (p1.topology.getTopologyId() != p2.topology.getTopologyId()) {
            return p2.topology.getTopologyId() - p1.topology.getTopologyId();
         }
         return 0;
      });

      for (int i = 0; i < partitions.size();) {
         Partition p = partitions.get(i);

         boolean fold = false;
         for (int j = 0; j < i; j++) {
            Partition referencePartition = partitions.get(j);

            // If read owners don't overlap then we clearly have 2 independent partitions
            if (!InfinispanCollections.containsAny(p.readCH.getMembers(), referencePartition.readCH.getMembers())) {
               continue;
            }

            // Normally the difference between the topology ids seen by different members is 0 or 1.
            // But only rebalance start and phase changes are kept in step,
            // topology updates are fire-and-forget, so the difference can be higher
            // even when communication between nodes is not interrupted.
            int referenceTopologyId = referencePartition.topology.getTopologyId();
            int topologyId = p.topology.getTopologyId();
            if (topologyId == referenceTopologyId) {
               if (p.topology.equals(referencePartition.topology)) {
                  // The topology is exactly the same, ignore it
                  log.tracef("Cache %s ignoring topology from %s, same as topology from %s: %s",
                             cacheName, p.senders, referencePartition.senders, p.topology);
                  fold = true;
               } else {
                  if (trace)
                     log.tracef(
                        "Cache %s partition of %s overlaps with partition of %s, with the same topology id",
                        cacheName, p.senders, referencePartition.senders);
               }
            } else {
               // topologyId < referenceTopologyId
               // Small topology differences are expected even between properly communicating nodes,
               // but we should consider it a different partition if p's sender might have some entries
               // that the other members lost.
               // Having a majority doesn't matter, because the lost data check ensures p's sender
               // couldn't make any updates without talking to the reference partition's members
               if (!lostDataCheck.test(p.readCH, referencePartition.actualMembers)) {
                  log.tracef("Cache %s ignoring compatible old topology from %s: %s", cacheName, p.senders, p.topology);
                  fold = true;
               } else {
                  if (trace)
                     log.tracef("Cache %s partition of %s overlaps with partition of %s but possibly holds extra entries",
                                cacheName, p.senders, referencePartition.senders);
               }
            }
            if (fold) {
               referencePartition.senders.addAll(p.senders);
               partitions.remove(i);
               break;
            }
         }
         if (!fold) {
            if (trace) log.tracef("Cache %s keeping partition from %s: %s", cacheName, p.senders, p.topology);
            i++;
         }
      }
      return partitions;
   }

   @Override
   public void onRebalanceEnd(AvailabilityStrategyContext context) {
      // Do nothing, if we need another rebalance it's already in the queue
   }

   @Override
   public void onManualAvailabilityChange(AvailabilityStrategyContext context, AvailabilityMode newAvailabilityMode) {
      // The cache should always be AVAILABLE
   }

   private static class Partition {
      CacheTopology topology;
      CacheTopology stableTopology;
      ConsistentHash readCH;
      List<Address> actualMembers;
      List<Address> senders = new ArrayList<>();
   }
}
