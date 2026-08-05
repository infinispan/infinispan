package org.infinispan.distribution.ch.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.distribution.topologyaware.TopologyInfo;
import org.infinispan.distribution.topologyaware.TopologyLevel;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;

/**
 * A topology-aware variant of {@link PureRendezvousConsistentHashFactory} that enforces placement
 * of backup owners across distinct failure domains (site → rack → machine → node).
 *
 * <p>Topology constraints are applied inline during assignment: for each segment, candidates are
 * only accepted at a given owner position if they satisfy the topology diversity constraint for
 * that position (distinct site, rack, machine, or node depending on how many distinct locations
 * exist and how many owners are requested).</p>
 *
 * <p>No load cap, balance correction, or primary-swap pass is applied. The assignment is a pure
 * function of the node UUIDs, capacity factors, and topology metadata. Use
 * {@link TopologyAwareRendezvousConsistentHashFactory} for tighter floor/ceil balance guarantees.</p>
 *
 * <p>When topology information is absent, this factory degrades gracefully to the behaviour of
 * {@link PureRendezvousConsistentHashFactory}.</p>
 *
 * @author Infinispan
 * @since 16.3
 */
@ProtoTypeId(ProtoStreamTypeIds.PURE_TOPOLOGY_AWARE_RENDEZVOUS_CONSISTENT_HASH_FACTORY)
public class PureTopologyAwareRendezvousConsistentHashFactory
      extends PureRendezvousConsistentHashFactory {

   private static final PureTopologyAwareRendezvousConsistentHashFactory INSTANCE =
         new PureTopologyAwareRendezvousConsistentHashFactory();

   protected PureTopologyAwareRendezvousConsistentHashFactory() { }

   @ProtoFactory
   public static PureTopologyAwareRendezvousConsistentHashFactory getInstance() {
      return INSTANCE;
   }

   /**
    * Overrides build() to apply topology diversity constraints inline during assignment.
    * The rendezvous rankings are computed by the base class; topology constraints are applied via
    * the {@link TopologyFilter} hook at each owner position.
    */
   @Override
   DefaultConsistentHash build(int numOwners, int numSegments, List<Address> members,
                                Map<Address, Float> capacityFactors) {
      int actualNumOwners = computeActualNumOwners(numOwners, members, capacityFactors);

      List<Address>[] rankings = computeRankings(numSegments, members, capacityFactors);

      // Build topology info from eligible members
      List<Address> eligible = new ArrayList<>(members.size());
      for (Address m : members) {
         float cf = capacityFactors != null ? capacityFactors.getOrDefault(m, 1f) : 1f;
         if (cf > 0) eligible.add(m);
      }
      TopologyInfo topologyInfo = new TopologyInfo(numSegments, actualNumOwners, eligible, capacityFactors);

      final int numSites = topologyInfo.getDistinctLocationsCount(TopologyLevel.SITE);
      final int numRacks = topologyInfo.getDistinctLocationsCount(TopologyLevel.RACK);
      final int numMachines = topologyInfo.getDistinctLocationsCount(TopologyLevel.MACHINE);

      // Topology filter: at each owner position, the candidate must be in a distinct
      // site/rack/machine from already-assigned owners, degrading gracefully when locations
      // are fewer than numOwners.
      TopologyFilter filter = (pos, candidate, currentOwners) -> {
         if (pos == 0) return true;
         if (pos < numSites) {
            return !topologyInfo.duplicateLocation(TopologyLevel.SITE, currentOwners, candidate, false);
         } else if (pos < numRacks) {
            return !topologyInfo.duplicateLocation(TopologyLevel.RACK, currentOwners, candidate, false);
         } else if (pos < numMachines) {
            return !topologyInfo.duplicateLocation(TopologyLevel.MACHINE, currentOwners, candidate, false);
         }
         return !currentOwners.contains(candidate);
      };

      // Pure rendezvous assignment with topology filter — no cap, no balance correction
      @SuppressWarnings("unchecked")
      List<Address>[] segmentOwners = new List[numSegments];

      for (int s = 0; s < numSegments; s++) {
         List<Address> owners = new ArrayList<>(actualNumOwners);
         segmentOwners[s] = owners;
         List<Address> ranking = rankings[s];

         // First pass: respect topology filter
         for (Address candidate : ranking) {
            if (owners.size() >= actualNumOwners) break;
            int pos = owners.size();
            if (filter.canOwn(pos, candidate, owners)) {
               owners.add(candidate);
            }
         }
         // Fallback: if topology constraint left gaps, fill ignoring topology
         if (owners.size() < actualNumOwners) {
            for (Address candidate : ranking) {
               if (owners.size() >= actualNumOwners) break;
               if (!owners.contains(candidate)) {
                  owners.add(candidate);
               }
            }
         }
      }

      return DefaultConsistentHash.create(numOwners, numSegments, members, capacityFactors, segmentOwners);
   }

   @Override
   public boolean equals(Object other) {
      return other != null && other.getClass() == getClass();
   }

   @Override
   public int hashCode() {
      return 8243;
   }
}
