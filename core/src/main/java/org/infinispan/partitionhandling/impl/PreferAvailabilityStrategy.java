package org.infinispan.partitionhandling.impl;

import static org.infinispan.util.logging.events.Messages.MESSAGES;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
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

public class PreferAvailabilityStrategy implements AvailabilityStrategy {

   public static final Comparator<CacheStatusResponse> RESPONSE_COMPARATOR = (s1, s2) -> {
      if (s2 == null)
         return -1;

      CacheTopology t1 = s1.getCacheTopology();
      CacheTopology t2 = s2.getCacheTopology();
      int topologyId = ((Integer)t1.getTopologyId()).compareTo(t2.getTopologyId());
      if (topologyId != 0)
         return topologyId;

      int rebalanceId = ((Integer)t1.getRebalanceId()).compareTo(t2.getRebalanceId());
      if (rebalanceId != 0)
         return topologyId;

      List<Address> m1 = t1.getMembers();
      List<Address> m2 = t2.getMembers();
      if (m1.size() == m2.size()) {
         int compareAddress = m1.get(0).compareTo(m2.get(0));
         if (compareAddress == 0)
            return ((Integer)t1.hashCode()).compareTo(t2.hashCode());
      }
      return m1.size() > m2.size() ? -1 : 1;
   };

   private static final Log log = LogFactory.getLog(PreferAvailabilityStrategy.class);
   private final EventLogManager eventLogManager;
   private final PersistentUUIDManager persistentUUIDManager;
   private final LostDataCheck lostDataCheck;
   private final boolean resolveConflictsOnMerge;

   public PreferAvailabilityStrategy(EventLogManager eventLogManager, PersistentUUIDManager persistentUUIDManager, LostDataCheck lostDataCheck, boolean resolveConflictsOnMerge) {
      this.eventLogManager = eventLogManager;
      this.persistentUUIDManager = persistentUUIDManager;
      this.lostDataCheck = lostDataCheck;
      this.resolveConflictsOnMerge = resolveConflictsOnMerge;
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

      // Pick the biggest stable topology (i.e. the one with most members)
      CacheTopology maxStableTopology = null;
      for (CacheStatusResponse response : statusResponses) {
         CacheTopology stableTopology = response.getStableTopology();
         if (stableTopology == null) {
            // The node hasn't properly joined yet.
            continue;
         }
         if (maxStableTopology == null || maxStableTopology.getMembers().size() < stableTopology.getMembers().size()) {
            maxStableTopology = stableTopology;
         }
      }

      // Now pick the biggest current topology derived from the biggest stable topology
      CacheTopology maxTopology = null;
      for (CacheStatusResponse response : statusResponses) {
         CacheTopology stableTopology = response.getStableTopology();
         if (!Objects.equals(stableTopology, maxStableTopology))
            continue;

         CacheTopology topology = response.getCacheTopology();
         if (topology == null) {
            // The node hasn't properly joined yet.
            continue;
         }
         if (maxTopology == null || maxTopology.getMembers().size() < topology.getMembers().size()) {
            maxTopology = topology;
         }
      }

      if (maxTopology == null) {
         log.debugf("No current topology, recovered only joiners for cache %s", context.getCacheName());
      }

      // Since we picked the biggest topology, its topology id may not be the biggest
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
      List<Address> newMembers = context.getExpectedMembers();
      boolean resolveConflicts = resolveConflictsOnMerge && isSplitBrainHealing(context, maxTopology, maxStableTopology);
      if (maxTopology != null) {
         // If we are required to resolveConflicts, then we utilise the CH of the expected members. This is necessary
         // so that during conflict resolution, writes go to all owners
         if (resolveConflicts) {
            CacheJoinInfo joinInfo = context.getJoinInfo();
            ConsistentHashFactory chf = joinInfo.getConsistentHashFactory();
            ConsistentHash mergehash = chf.create(joinInfo.getHashFunction(), joinInfo.getNumOwners(), joinInfo.getNumSegments(), newMembers, context.getCapacityFactors());
            mergedTopology = new CacheTopology(maxTopologyId + 1, maxRebalanceId + 1, maxTopology.getCurrentCH(),
                  mergehash, mergehash, CacheTopology.Phase.CONFLICT_RESOLUTION, newMembers, persistentUUIDManager.mapAddresses(newMembers));
         } else {
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
      context.updateCurrentTopology(survivingMembers);

      // Then start a rebalance with the merged members
      context.queueRebalance(newMembers);
   }

   // If we have more expected members than stable members, then we know that a split brain heal is occurring
   // However if the maxTopologySize == 1, then we know that we have a new coordinator
   private boolean isSplitBrainHealing(AvailabilityStrategyContext context, CacheTopology maxTopology, CacheTopology maxStableTopology) {
      boolean isNewCoordinator = maxTopology != null && maxTopology.getMembers().size() == 1;
      boolean membershipIncreased = !isNewCoordinator && maxStableTopology != null && context.getExpectedMembers().size() > maxStableTopology.getMembers().size();
      return !isNewCoordinator && membershipIncreased;
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
