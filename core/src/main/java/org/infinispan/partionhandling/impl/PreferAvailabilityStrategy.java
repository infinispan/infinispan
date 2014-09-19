package org.infinispan.partionhandling.impl;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheStatusResponse;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class PreferAvailabilityStrategy implements AvailabilityStrategy {
   private static final Log log = LogFactory.getLog(PreferAvailabilityStrategy.class);

   @Override
   public void onJoin(AvailabilityStrategyContext context, Address joiner) {
      context.queueRebalance(context.getExpectedMembers());
   }

   @Override
   public void onGracefulLeave(AvailabilityStrategyContext context, Address leaver) {
      CacheTopology currentTopology = context.getCurrentTopology();
      List<Address> newMembers = new ArrayList<>(currentTopology.getMembers());
      if (!newMembers.remove(leaver)) {
         log.debugf("Cache %s does not have member %s, ignoring leave request", context.getCacheName(), leaver);
         return;
      }
      if (context.getStableTopology() != null && isDataLost(context.getStableTopology().getCurrentCH(), newMembers)) {
         log.lostDataBecauseOfGracefulLeaver(context.getCacheName(), leaver);
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
      if (isDataLost(stableTopology.getCurrentCH(), newMembers)) {
         log.lostDataBecauseOfAbruptLeavers(context.getCacheName(), lostMembers);
      } else if (lostMembers.size() >= Math.ceil(stableMembers.size() / 2d)) {
         log.minorityPartition(context.getCacheName(), lostMembers.size(), stableMembers.size());
      }
   }

   @Override
   public void onPartitionMerge(AvailabilityStrategyContext context, Collection<CacheStatusResponse> statusResponses) {
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

      // Since we picked the biggest topology, its topology id may not be the biggest
      int maxTopologyId = 0;
      for (CacheStatusResponse response : statusResponses) {
         CacheTopology topology = response.getCacheTopology();
         if (topology != null && maxTopologyId < topology.getTopologyId()) {
            maxTopologyId = topology.getTopologyId();
         }
      }

      // Cancel any pending rebalance in the current topology.
      // By definition, the stable topology does not have a pending CH.
      CacheTopology mergedTopology = maxTopology;
      if (maxTopology.getPendingCH() != null) {
         mergedTopology = new CacheTopology(maxTopologyId + 1, maxTopology.getRebalanceId(),
               maxTopology.getCurrentCH(), null);
      }

      context.updateTopologiesAfterMerge(mergedTopology, maxStableTopology, null);

      // First update the CHs to remove any nodes that left from the current topology
      List<Address> newMembers = context.getExpectedMembers();
      List<Address> survivingMembers = new ArrayList<>(newMembers);
      if (survivingMembers.retainAll(newMembers)) {
         checkForLostData(context, survivingMembers);
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
   public void onManualAvailabilityChange(AvailabilityStrategyContext context) {
      context.updateCurrentTopology(context.getExpectedMembers());
      context.queueRebalance(context.getExpectedMembers());
   }

   private boolean isDataLost(ConsistentHash currentCH, List<Address> newMembers) {
      for (int i = 0; i < currentCH.getNumSegments(); i++) {
         if (!containsAny(newMembers, currentCH.locateOwnersForSegment(i)))
            return true;
      }
      return false;
   }

   private boolean containsAny(List<Address> newMembers, List<Address> owners) {
      for (Address owner : owners) {
         if (newMembers.contains(owner))
            return true;
      }
      return false;
   }
}
