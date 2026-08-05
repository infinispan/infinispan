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
 * A topology-aware variant of {@link HistoryHintedRendezvousConsistentHashFactory} that enforces
 * placement of backup owners across distinct failure domains (site → rack → machine → node),
 * combined with bounded-load greedy correction and primary-ownership redistribution to guarantee
 * tight {@code floor/ceil} balance.
 *
 * <p>Extends {@link HistoryHintedRendezvousConsistentHashFactory}, overriding only {@code build()}
 * to apply topology diversity constraints inline during the greedy owner assignment. The
 * history-hinted {@code rebalance()} implementation is inherited unchanged, so topology-aware
 * caches also benefit from minimal segment movement across topology changes.</p>
 *
 * <p>The {@code build()} override runs in three phases:</p>
 * <ol>
 *   <li><b>Phase 1 — topology-aware greedy assignment</b>: assign owners per segment using
 *       rendezvous rankings, bounded-load caps, and a {@link TopologyFilter} that enforces
 *       distinct site/rack/machine placement at each owner position.</li>
 *   <li><b>Phase 2a — balance correction</b>: give at least one segment to any eligible node
 *       whose ideal is positive but whose current count is zero ({@link #balanceCorrection}).
 *       Topology constraints are preserved during correction.</li>
 *   <li><b>Phase 2b — primary redistribution</b>: delegate to
 *       {@link SegmentOwnershipBalancer#redistributePrimary} to bring every node within
 *       {@code floor/ceil} of its primary ideal. Because only the order within each segment's
 *       existing owner set changes, this phase preserves topology diversity and incurs no data
 *       movement.</li>
 * </ol>
 *
 * <p>When topology information is absent, this factory degrades gracefully to the behaviour of
 * {@link HistoryHintedRendezvousConsistentHashFactory}.</p>
 *
 * @author Infinispan
 * @since 16.3
 * @see HistoryHintedRendezvousConsistentHashFactory
 * @see SegmentOwnershipBalancer
 */
@ProtoTypeId(ProtoStreamTypeIds.TOPOLOGY_AWARE_RENDEZVOUS_CONSISTENT_HASH_FACTORY)
public class TopologyAwareRendezvousConsistentHashFactory
      extends HistoryHintedRendezvousConsistentHashFactory {

   private static final TopologyAwareRendezvousConsistentHashFactory INSTANCE =
         new TopologyAwareRendezvousConsistentHashFactory();

   protected TopologyAwareRendezvousConsistentHashFactory() { }

   @ProtoFactory
   public static TopologyAwareRendezvousConsistentHashFactory getInstance() {
      return INSTANCE;
   }

   /**
    * Overrides {@code build()} to apply topology diversity constraints inline during greedy
    * assignment, then delegates the primary-ownership redistribution to
    * {@link SegmentOwnershipBalancer#redistributePrimary}.
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

      // Phase 1: greedy assignment with topology filter + bounded-load caps
      float totalCapacity = SegmentOwnershipBalancer.computeTotalCapacity(members, capacityFactors);
      int[] cap = computeCaps(members, capacityFactors, totalCapacity, numSegments, actualNumOwners);
      int[] owned = new int[members.size()];

      @SuppressWarnings("unchecked")
      List<Address>[] segmentOwners = new List[numSegments];

      for (int s = 0; s < numSegments; s++) {
         List<Address> owners = new ArrayList<>(actualNumOwners);
         segmentOwners[s] = owners;
         assignOwners(s, rankings[s], owners, members, cap, owned, actualNumOwners, filter);
      }

      // Phase 2a: balance correction — give at least one segment to any under-assigned eligible node
      float[] ideal = SegmentOwnershipBalancer.computeIdeals(members, capacityFactors, totalCapacity, numSegments, actualNumOwners);
      balanceCorrection(segmentOwners, members, owned, ideal, actualNumOwners, filter);

      // Phase 2b: primary redistribution — free position swaps via SegmentOwnershipBalancer
      if (actualNumOwners > 1) {
         float[] primaryIdeal = SegmentOwnershipBalancer.computeIdeals(members, capacityFactors, totalCapacity, numSegments, 1);
         int[] primaryOwned = SegmentOwnershipBalancer.countPrimaryOwned(segmentOwners, members);
         SegmentOwnershipBalancer.redistributePrimary(segmentOwners, rankings, members, primaryOwned, primaryIdeal);
      }

      return DefaultConsistentHash.create(numOwners, numSegments, members, capacityFactors, segmentOwners);
   }

   @Override
   public boolean equals(Object other) {
      return other != null && other.getClass() == getClass();
   }

   @Override
   public int hashCode() {
      return 7193;
   }

   // ---- Topology-aware assignment helpers ----

   /**
    * Assigns owners for a single segment from the given ranking, respecting ownership caps and the
    * topology filter constraint at each owner position.
    *
    * @param topologyFilter optional per-position predicate; {@code null} means no topology constraint.
    */
   private static void assignOwners(int segment, List<Address> ranking, List<Address> owners,
                                     List<Address> members, int[] cap, int[] owned, int actualNumOwners,
                                     TopologyFilter topologyFilter) {
      // First pass: respect cap (and topology constraint if provided)
      for (Address candidate : ranking) {
         if (owners.size() >= actualNumOwners) break;
         int idx = members.indexOf(candidate);
         if (owned[idx] < cap[idx]) {
            int pos = owners.size();
            if (topologyFilter == null || topologyFilter.canOwn(pos, candidate, owners)) {
               owners.add(candidate);
               owned[idx]++;
            }
         }
      }
      // Fallback pass: if under-owned, add remaining ignoring cap (but still respecting topology)
      if (owners.size() < actualNumOwners) {
         for (Address candidate : ranking) {
            if (owners.size() >= actualNumOwners) break;
            if (!owners.contains(candidate)) {
               int pos = owners.size();
               if (topologyFilter == null || topologyFilter.canOwn(pos, candidate, owners)) {
                  int idx = members.indexOf(candidate);
                  owners.add(candidate);
                  owned[idx]++;
               }
            }
         }
      }
      // Final fallback: if topology constraint made it impossible to fill, ignore topology
      if (owners.size() < actualNumOwners) {
         for (Address candidate : ranking) {
            if (owners.size() >= actualNumOwners) break;
            if (!owners.contains(candidate)) {
               int idx = members.indexOf(candidate);
               owners.add(candidate);
               owned[idx]++;
            }
         }
      }
   }

   /**
    * Post-assignment balance correction: for any eligible node with zero ownership when its ideal
    * is positive, steals one slot from an over-assigned node.
    *
    * <p>Only replaces backup positions (vi ≥ 1) to avoid spurious primary-switch counts, except
    * when numOwners=1 where position 0 must be used. Topology constraints are preserved.</p>
    */
   private static void balanceCorrection(List<Address>[] segmentOwners, List<Address> members,
                                          int[] owned, float[] ideal, int actualNumOwners,
                                          TopologyFilter topologyFilter) {
      int minVi = actualNumOwners > 1 ? 1 : 0;
      for (int hungry = 0; hungry < members.size(); hungry++) {
         // Only correct extreme case: node has 0 ownership when ideal > 0
         if (owned[hungry] != 0 || ideal[hungry] <= 0) continue;
         Address hungryNode = members.get(hungry);
         outer:
         for (int s = 0; s < segmentOwners.length; s++) {
            List<Address> owners = segmentOwners[s];
            if (owners.contains(hungryNode)) continue;
            for (int vi = owners.size() - 1; vi >= minVi; vi--) {
               Address victim = owners.get(vi);
               int vi_idx = members.indexOf(victim);
               if (owned[vi_idx] <= (int) Math.ceil(ideal[vi_idx])) continue;
               if (topologyFilter != null) {
                  List<Address> othersAtSwapPos = new ArrayList<>(owners);
                  othersAtSwapPos.remove(vi);
                  if (!topologyFilter.canOwn(vi, hungryNode, othersAtSwapPos)) continue;
               }
               owners.set(vi, hungryNode);
               owned[hungry]++;
               owned[vi_idx]--;
               break outer; // give hungry node just 1 segment, then re-check
            }
         }
         // If still 0 (no ceil-exceeding victim found), try floor-exceeding victim
         if (owned[hungry] != 0) continue;
         outer2:
         for (int s = 0; s < segmentOwners.length; s++) {
            List<Address> owners = segmentOwners[s];
            if (owners.contains(hungryNode)) continue;
            for (int vi = owners.size() - 1; vi >= minVi; vi--) {
               Address victim = owners.get(vi);
               int vi_idx = members.indexOf(victim);
               if (owned[vi_idx] <= (int) Math.floor(ideal[vi_idx])) continue;
               if (topologyFilter != null) {
                  List<Address> othersAtSwapPos = new ArrayList<>(owners);
                  othersAtSwapPos.remove(vi);
                  if (!topologyFilter.canOwn(vi, hungryNode, othersAtSwapPos)) continue;
               }
               owners.set(vi, hungryNode);
               owned[hungry]++;
               owned[vi_idx]--;
               break outer2;
            }
         }
      }
   }

   static int[] computeCaps(List<Address> members, Map<Address, Float> capacityFactors,
                            float totalCapacity, int numSegments, int actualNumOwners) {
      int[] cap = new int[members.size()];
      for (int i = 0; i < members.size(); i++) {
         float cf = capacityFactors != null ? capacityFactors.getOrDefault(members.get(i), 1f) : 1f;
         cap[i] = (int) Math.ceil((double) numSegments * actualNumOwners * cf / totalCapacity);
      }
      return cap;
   }
}
