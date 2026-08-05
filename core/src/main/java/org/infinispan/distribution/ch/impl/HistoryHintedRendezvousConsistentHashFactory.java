package org.infinispan.distribution.ch.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;

/**
 * A {@link org.infinispan.distribution.ch.impl.ConsistentHashFactory} that extends
 * {@link RendezvousConsistentHashFactory} with a history-hinted load-balancing pass to minimise
 * segment movement across topology changes.
 *
 * <h2>Rebalance algorithm</h2>
 * <p>The {@code rebalance()} method runs in three phases:</p>
 * <ol>
 *   <li><b>Phase 1 — pure rendezvous assignment</b> (inherited from
 *       {@link PureRendezvousConsistentHashFactory}): assign the top-N ranked nodes per segment
 *       by rendezvous score.</li>
 *   <li><b>Phase 2 — history-hinted load balancing</b>: for any node that is above
 *       {@code ceil(ideal)}, scan its segments. Before falling through to the normal
 *       {@link SegmentOwnershipBalancer} candidate selection, check whether any node that owned
 *       that segment in the <em>prior</em> topology is currently under-loaded. If so, prefer
 *       that prior owner as the replacement — its data is already warm, so moving the segment
 *       back incurs no net movement cost. Only when no prior-topology owner is eligible does
 *       the algorithm fall through to the standard shed-weakest scoring.</li>
 *   <li><b>Phase 3 — standard load balancing</b>: delegate remaining over-ceiling excess and
 *       primary-ownership redistribution to {@link SegmentOwnershipBalancer#apply}.</li>
 * </ol>
 *
 * <p>The result is that segments are effectively "pinned" to prior owners whenever that preserves
 * load balance, reducing the total number of segments that move on each topology change compared
 * to the pure-rendezvous rebalance.</p>
 *
 * <p>Like {@link RendezvousConsistentHashFactory}, this factory guarantees that no node owns more
 * than {@code ceil(ideal)} segments and achieves tight {@code floor/ceil} primary balance.</p>
 *
 * <p>This factory requires all cluster members to be at or above
 * {@link org.infinispan.remoting.transport.NodeVersion#SIXTEEN_THREE}.</p>
 *
 * @author wburns
 * @since 16.3
 * @see RendezvousConsistentHashFactory
 * @see SegmentOwnershipBalancer
 */
@ProtoTypeId(ProtoStreamTypeIds.HISTORY_HINTED_RENDEZVOUS_CONSISTENT_HASH_FACTORY)
public class HistoryHintedRendezvousConsistentHashFactory extends RendezvousConsistentHashFactory {

   private static final HistoryHintedRendezvousConsistentHashFactory INSTANCE =
         new HistoryHintedRendezvousConsistentHashFactory();

   protected HistoryHintedRendezvousConsistentHashFactory() { }

   @ProtoFactory
   public static HistoryHintedRendezvousConsistentHashFactory getInstance() {
      return INSTANCE;
   }

   /**
    * Rebalances {@code baseCH}, using its current segment assignments as history hints to
    * minimise movement during the load-balancing pass.
    *
    * <h3>Stable-state shortcut</h3>
    * <p>When both of the following hold, Phases 1 and 2 are skipped and only the primary
    * redistribution pass (part of Phase 3) is run:</p>
    * <ul>
    *   <li>Every segment already has exactly {@code actualNumOwners} owners (no under-replication
    *       from a leaver).</li>
    *   <li>Every non-zero-capacity member already owns at least one segment (no joiner is
    *       waiting for its first assignment).</li>
    * </ul>
    * <p>Total ownership is already correct, so the only work that may remain is correcting any
    * primary imbalance.  Running the primary redistribution pass over the existing owner sets
    * and returning its result (or {@code baseCH} if nothing changed) gives idempotency and
    * prevents {@code waitForNoRebalance} from looping indefinitely.</p>
    *
    * <p>When the shortcut does not fire, all three phases run.  Phase 2 (history hints) uses
    * {@code baseCH} as the prior-topology reference for both leaves (under-replicated segments)
    * and joins (new node has no segments yet).  Phase 3 handles any remaining excess and primary
    * redistribution.</p>
    */
   @Override
   public DefaultConsistentHash rebalance(DefaultConsistentHash baseCH) {
      int numOwners = baseCH.getNumOwners();
      int numSegments = baseCH.getNumSegments();
      List<Address> members = baseCH.getMembers();
      Map<Address, Float> capacityFactors = baseCH.getCapacityFactors();

      int actualNumOwners = computeActualNumOwners(numOwners, members, capacityFactors);

      // Stable-state shortcut: total ownership is already correct — every segment has the right
      // number of owners and every member has at least one segment.  Skip Phases 1 and 2 (which
      // would rebuild from scratch and re-apply history hints unnecessarily) and run only the
      // primary redistribution pass over the existing owner sets.  Return baseCH unchanged if
      // primaries were already balanced, satisfying both idempotency and the contract requirement
      // that rebalance() returns baseCH when no changes are needed.
      if (allSegmentsFullyReplicated(baseCH, actualNumOwners) && allMembersHaveSegments(baseCH, members, capacityFactors)) {
         if (actualNumOwners <= 1) {
            return baseCH;
         }
         @SuppressWarnings("unchecked")
         List<Address>[] segmentOwners = new List[numSegments];
         for (int s = 0; s < numSegments; s++) {
            segmentOwners[s] = new ArrayList<>(baseCH.locateOwnersForSegment(s));
         }
         List<Address>[] rankings = computeRankings(numSegments, members, capacityFactors);
         float totalCapacity = SegmentOwnershipBalancer.computeTotalCapacity(members, capacityFactors);
         float[] primaryIdeal = SegmentOwnershipBalancer.computeIdeals(members, capacityFactors,
               totalCapacity, numSegments, 1);
         int[] primaryOwned = SegmentOwnershipBalancer.countPrimaryOwned(segmentOwners, members);
         SegmentOwnershipBalancer.redistributePrimary(segmentOwners, rankings, members,
               primaryOwned, primaryIdeal);
         DefaultConsistentHash rebalanced = DefaultConsistentHash.create(numOwners, numSegments,
               members, capacityFactors, segmentOwners);
         return rebalanced.equals(baseCH) ? baseCH : rebalanced;
      }

      // Phase 1: pure rendezvous rankings
      List<Address>[] rankings = computeRankings(numSegments, members, capacityFactors);

      // Build initial owners from pure-rendezvous top-N (same as RendezvousConsistentHashFactory)
      @SuppressWarnings("unchecked")
      List<Address>[] segmentOwners = new List[numSegments];
      for (int s = 0; s < numSegments; s++) {
         List<Address> ranking = rankings[s];
         List<Address> owners = new ArrayList<>(actualNumOwners);
         for (int i = 0; i < actualNumOwners; i++) {
            owners.add(ranking.get(i));
         }
         segmentOwners[s] = owners;
      }

      // Phase 2: history-hinted redistribution — prefer prior owners when draining over-loaded nodes.
      applyHistoryHints(segmentOwners, rankings, members, capacityFactors, numSegments,
            actualNumOwners, baseCH);

      // Phase 3: standard load balancing — handles any remaining total-ownership excess AND
      // primary-ownership redistribution.  Phase 3 must always run: even when Phases 1+2 leave
      // total ownership unchanged, primary ownership may still be imbalanced (e.g. after a join
      // where no backup slots needed moving but the new node should absorb some primaries).
      SegmentOwnershipBalancer.apply(segmentOwners, rankings, members, capacityFactors,
            numSegments, actualNumOwners);

      DefaultConsistentHash rebalanced = DefaultConsistentHash.create(numOwners, numSegments,
            members, capacityFactors, segmentOwners);
      return rebalanced.equals(baseCH) ? baseCH : rebalanced;
   }

   /**
    * Returns {@code true} if every segment in {@code ch} has exactly {@code actualNumOwners}
    * owners — i.e. no segment is under-replicated due to a recent leave, and no segment is
    * over-replicated due to a union/conflict-resolution CH that merged owner sets from both
    * partitions (which would have more than {@code actualNumOwners} owners per segment).
    */
   private static boolean allSegmentsFullyReplicated(DefaultConsistentHash ch, int actualNumOwners) {
      int numSegments = ch.getNumSegments();
      for (int s = 0; s < numSegments; s++) {
         if (ch.locateOwnersForSegment(s).size() != actualNumOwners) {
            return false;
         }
      }
      return true;
   }

   /**
    * Returns {@code true} if every non-zero-capacity member in {@code members} owns at least
    * one segment in {@code ch} — confirming no joiner is waiting for its first assignment.
    */
   private static boolean allMembersHaveSegments(DefaultConsistentHash ch, List<Address> members,
                                                  Map<Address, Float> capacityFactors) {
      for (Address member : members) {
         float cf = capacityFactors != null ? capacityFactors.getOrDefault(member, 1f) : 1f;
         if (cf == 0f) continue; // zero-capacity nodes intentionally hold no segments
         if (ch.getSegmentsForOwner(member).isEmpty()) {
            return false;
         }
      }
      return true;
   }

   /**
    * History-hinted redistribution pass.
    *
    * <p>Scans every overloaded node (not just the worst). For each overloaded node, iterates its
    * <em>backup</em> segments and checks whether any prior owner of that segment is currently at
    * or above its ideal — meaning it already holds the data and absorbing the segment back is not
    * a net cost. If such a prior owner exists and is not already in the owner set, the overloaded
    * node is replaced by the prior owner.</p>
    *
    * <p>The loop repeats until no overloaded node has any history-hinted swap available, at which
    * point the standard balancer ({@link SegmentOwnershipBalancer#apply}) handles any remaining excess
    * via the standard shed-weakest scoring.</p>
    *
    * <p>Primary slots ({@code position 0}) are not touched here — that is left to the primary
    * redistribution pass in Phase 3, which is purely a position-swap (no data movement).</p>
    */
   private static void applyHistoryHints(List<Address>[] segmentOwners, List<Address>[] rankings,
                                         List<Address> members, Map<Address, Float> capacityFactors,
                                         int numSegments, int actualNumOwners,
                                         DefaultConsistentHash priorCH) {
      float totalCapacity = SegmentOwnershipBalancer.computeTotalCapacity(members, capacityFactors);
      float[] ideal = SegmentOwnershipBalancer.computeIdeals(members, capacityFactors,
            totalCapacity, numSegments, actualNumOwners);
      int[] owned = SegmentOwnershipBalancer.countOwned(segmentOwners, members);

      int n = members.size();

      // Check every node — not just the worst — so a node that has no swap opportunity
      // does not block other overloaded nodes from being processed.
      for (int i = 0; i < n; i++) {
         float excess = owned[i] - ((float) Math.ceil(ideal[i]) + 1);
         if (excess <= 0) continue; // this node is not overloaded

         Address overloadedNode = members.get(i);

         // For each segment where this node is a backup, look for a prior owner that is
         // currently at or above its own ideal (loaded or overloaded) and not already in
         // the owner set. Prefer the prior owner with the best rendezvous rank.
         for (int s = 0; s < numSegments; s++) {
            List<Address> owners = segmentOwners[s];
            int pos = owners.indexOf(overloadedNode);
            if (pos <= 0) continue; // not a backup in this segment

            List<Address> priorOwners = priorCH.locateOwnersForSegment(s);
            List<Address> ranking = rankings[s];

            int bestPriorOwnerIdx = -1;
            int bestPriorOwnerRank = Integer.MAX_VALUE;

            for (Address priorOwner : priorOwners) {
               int pi = members.indexOf(priorOwner);
               if (pi < 0) continue; // left the cluster
               if (owners.contains(priorOwner)) continue; // already in owner set
               // Accept only if the prior owner is not fully loaded (owns < ideal),
               // meaning it has capacity to absorb the segment back without worsening balance.
               if (owned[pi] >= ideal[pi]) continue;

               int priorRank = ranking.indexOf(priorOwner);
               if (priorRank < 0) continue; // zero-capacity node

               if (priorRank < bestPriorOwnerRank) {
                  bestPriorOwnerRank = priorRank;
                  bestPriorOwnerIdx = pi;
               }
            }

            if (bestPriorOwnerIdx == -1) continue; // no eligible prior owner for this segment

            // Swap: replace the overloaded node's backup slot with the prior owner
            owners.set(pos, members.get(bestPriorOwnerIdx));
            owned[i]--;
            owned[bestPriorOwnerIdx]++;

            // Re-check whether this node is still overloaded before scanning more segments
            excess = owned[i] - ((float) Math.ceil(ideal[i]) + 1);
            if (excess <= 0) break;
         }
      }
   }

   @Override
   public boolean equals(Object other) {
      return other != null && other.getClass() == getClass();
   }

   @Override
   public int hashCode() {
      return 8221;
   }
}
