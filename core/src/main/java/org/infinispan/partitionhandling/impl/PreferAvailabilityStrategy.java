package org.infinispan.partitionhandling.impl;

import static org.infinispan.util.logging.events.Messages.MESSAGES;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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

   public static final Comparator<CacheStatusResponse> RESPONSE_COMPARATOR = (s1, s2) -> {
      if (s2 == null)
         return -1;

      CacheTopology t1 = s1.getCacheTopology();
      CacheTopology t2 = s2.getCacheTopology();
      int topologyId = Integer.compare(t1.getTopologyId(), t2.getTopologyId());
      if (topologyId != 0)
         return topologyId;

      int rebalanceId = Integer.compare(t1.getRebalanceId(), t2.getRebalanceId());
      if (rebalanceId != 0)
         return topologyId;

      List<Address> m1 = t1.getMembers();
      List<Address> m2 = t2.getMembers();
      if (m1.size() == m2.size()) {
         int compareAddress = m1.get(0).compareTo(m2.get(0));
         if (compareAddress == 0)
            return Integer.compare(t1.hashCode(), t2.hashCode());
      }
      return m1.size() > m2.size() ? -1 : 1;
   };

   private static final Log log = LogFactory.getLog(PreferAvailabilityStrategy.class);
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
         log.tracef("Cache %s did not lose any members, skipping rebalance", context.getCacheName());
         return;
      }

      checkForLostData(context, newMembers);

      // We have to do the update in case rebalancing is disabled, or there is another rebalance in progress
      context.updateCurrentTopology(newMembers);
      context.queueRebalance(newMembers);
   }

   protected void checkForLostData(AvailabilityStrategyContext context, List<Address> newMembers) {
      CacheTopology stableTopology = context.getStableTopology();
      List<Address> stableMembers = stableTopology.getMembers();
      List<Address> lostMembers = new ArrayList<>(stableMembers);
      lostMembers.removeAll(newMembers);
      if (lostDataCheck.test(stableTopology.getCurrentCH(), newMembers)) {
         eventLogManager.getEventLogger().context(context.getCacheName()).fatal(EventLogCategory.CLUSTER, MESSAGES.lostDataBecauseOfAbruptLeavers(lostMembers));
      } else if (lostMembers.size() >= Math.ceil(stableMembers.size() / 2d)) {
         eventLogManager.getEventLogger().context(context.getCacheName()).warn(EventLogCategory.CLUSTER, MESSAGES.minorityPartition(newMembers, lostMembers, stableMembers));
      }
   }

   @Override
   public void onPartitionMerge(AvailabilityStrategyContext context, Map<Address, CacheStatusResponse> statusResponseMap) {
      // We must first sort the response list here, to ensure that the maxTopology is chosen deterministically in the
      // event that multiple topologies exist with the same number of members.
      List<CacheStatusResponse> statusResponses = statusResponseMap.values().stream()
            .sorted(RESPONSE_COMPARATOR)
            .collect(Collectors.toList());

      List<Address> newMembers = context.getExpectedMembers();

      // Pick the biggest stable topology (i.e. the one with most members)
      CacheTopology maxStableTopology = null;
      List<Address> maxStableActualMembers = null;
      for (CacheStatusResponse response : statusResponses) {
         CacheTopology stableTopology = response.getStableTopology();
         if (stableTopology == null) {
            // The node hasn't properly joined yet.
            continue;
         }
         List<Address> actualMembers = new ArrayList<>(stableTopology.getMembers());
         actualMembers.retainAll(newMembers);
         if (maxStableTopology == null || maxStableActualMembers.size() < actualMembers.size()) {
            maxStableTopology = stableTopology;
            maxStableActualMembers = actualMembers;
         }
      }

      // Now pick the biggest current topology derived from the biggest stable topology
      CacheTopology maxTopology = null;
      List<Address> maxActualMembers = null;
      for (CacheStatusResponse response : statusResponses) {
         CacheTopology stableTopology = response.getStableTopology();
         if (!Objects.equals(stableTopology, maxStableTopology))
            continue;

         CacheTopology topology = response.getCacheTopology();
         if (topology == null) {
            // The node hasn't properly joined yet.
            continue;
         }
         List<Address> actualMembers = new ArrayList<>(topology.getMembers());
         if (maxTopology == null || maxActualMembers.size() < actualMembers.size()) {
            maxTopology = topology;
            maxActualMembers = actualMembers;
         }
      }

      if (maxTopology == null) {
         log.debugf("No current topology, recovered only joiners for cache %s", context.getCacheName());
      }

      // Since we picked the biggest topology by size, its topology id may not be the biggest
      int maxTopologyId = 0;
      int maxRebalanceId = 0;
      for (CacheStatusResponse response : statusResponses) {
         CacheTopology topology = response.getCacheTopology();
         if (topology != null) {
            if (maxTopologyId < topology.getTopologyId()) {
               maxTopologyId = topology.getTopologyId();
            }
            if (maxRebalanceId < topology.getRebalanceId()) {
               maxRebalanceId = topology.getRebalanceId();
            }
         }
      }

      // Increment the topology id so that it's bigger than any topology that might have been sent by the old
      // coordinator. +1 is enough because there nodes wait for the new JGroups view before answering the status
      // request, and after they have the new view they can't process topology updates with the old view id.
      // Also cancel any pending rebalance by removing the pending CH, because we don't recover the rebalance
      // confirmation status (yet).
      CacheTopology mergedTopology = null;
      boolean resolveConflicts = false;
      if (maxTopology != null) {

         // Record all distinct read owners and hashes
         Set<Address> possibleOwners = new HashSet<>();
         Set<ConsistentHash> distinctHashes = new HashSet<>();
         for (CacheStatusResponse response : statusResponses) {
            CacheTopology cacheTopology = response.getCacheTopology();
            if (cacheTopology != null) {
               ConsistentHash hash = cacheTopology.getCurrentCH();
               if (hash != null && !hash.getMembers().isEmpty()) {
                  possibleOwners.addAll(hash.getMembers());
                  distinctHashes.add(hash);
               }
            }
         }

         resolveConflicts = context.resolveConflictsOnMerge() && !possibleOwners.isEmpty() && possibleOwners.size() > 1 && !maxTopology.getMembers().containsAll(possibleOwners);
         if (resolveConflicts) {
            List<Address> members = new ArrayList<>(possibleOwners);
            ConsistentHash conflictHash = context.calculateConflictHash(distinctHashes);
            mergedTopology = new CacheTopology(maxTopologyId + 1, maxRebalanceId + 1, maxTopology.getCurrentCH(),
                  conflictHash, conflictHash, CacheTopology.Phase.CONFLICT_RESOLUTION, members, persistentUUIDManager.mapAddresses(members));
         } else {
            // TODO If maxTopology.getPhase() == READ_NEW_WRITE_ALL, the pending CH would be more appropriate
            // The best approach may be to collect the read owners from all the topologies and use their union as the current CH
            mergedTopology = new CacheTopology(maxTopologyId + 1, maxRebalanceId + 1,
                  maxTopology.getCurrentCH(), null, CacheTopology.Phase.NO_REBALANCE, maxTopology.getActualMembers(),
                  persistentUUIDManager.mapAddresses(maxTopology.getActualMembers()));
         }
      }

      context.updateTopologiesAfterMerge(mergedTopology, maxStableTopology, null, resolveConflicts);

      // First update the CHs to remove any nodes that left from the current topology
      List<Address> survivingMembers = new ArrayList<>(newMembers);
      if (mergedTopology != null && survivingMembers.retainAll(mergedTopology.getMembers())) {
         checkForLostData(context, survivingMembers);
      }
      if (survivingMembers.isEmpty()) {
         // No surviving members, use the expected members instead
         survivingMembers = newMembers;
      }
      context.updateCurrentTopology(survivingMembers);

      // Then start a rebalance with the merged members
      context.queueRebalance(newMembers);
   }

   @Override
   public void onRebalanceEnd(AvailabilityStrategyContext context) {
      // Do nothing, if we need another rebalance it's already in the queue
   }

   @Override
   public void onManualAvailabilityChange(AvailabilityStrategyContext context, AvailabilityMode newAvailabilityMode) {
      // The cache should always be AVAILABLE
   }
}
