package org.infinispan.distribution.ch.impl;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.infinispan.remoting.transport.Address;

/**
 * Load-balancing post-processor for initial segment assignments that also can generate rankings
 * per segment for every node in the topology.
 *
 * <p>An initial segment assignment produced by a mapping strategy will typically have some nodes
 * above {@code ceil(ideal)} because hash-based placement does not account for load. This class
 * performs two convergent redistribution passes to correct that:</p>
 *
 * <ul>
 *   <li><b>Total-ownership redistribution</b> ({@link #redistributeTotal}): drains every node
 *       that owns more than {@code ceil(ideal)} segments by moving its excess backup slots to
 *       under-loaded candidates. Candidate selection maximises the net fitness delta of the swap
 *       (see below), keeping divergence from the natural assignment as small as possible. Nodes
 *       that end up below {@code floor(ideal)} are intentionally left as-is:
 *       under-loaded nodes do not cause memory pressure, and filling them would generate
 *       unnecessary movement on the next topology change.</li>
 *   <li><b>Primary-ownership redistribution</b> ({@link #redistributePrimary}): swaps primary ↔
 *       backup within each segment's existing owner set to bring every node's primary count within
 *       {@code floor/ceil} of its primary ideal. Because only the order within each segment changes
 *       (not the set), this pass incurs zero data movement.</li>
 * </ul>
 *
 * <p>Both passes operate on the same {@code segmentOwners} array in place, sharing the same
 * rankings so that all choices stay consistent with the underlying score ordering.</p>
 *
 * <h2>Composition model</h2>
 * <p>This class is deliberately decoupled from any specific initial mapping strategy.
 * A caller computes an initial {@code segmentOwners} array, then calls {@link #apply} to correct
 * the load distribution. The rankings array passed to {@link #apply} must be consistent with
 * whatever scoring was used to produce the initial assignment so that candidate preferences
 * remain coherent.</p>
 *
 * <h2>Algorithm details — total redistribution</h2>
 * <p>On each iteration:</p>
 * <ol>
 *   <li>Find the node {@code W} with the highest excess {@code owned(W) - ceil(ideal(W))}.
 *       If no node exceeds {@code ceil(ideal)}, stop.</li>
 *   <li><b>Pass 1 — backup-slot swap</b>: scan every segment where {@code W} is a
 *       <em>backup</em> owner (position ≥ 1). Among all eligible {@code (segment, candidate)}
 *       pairs (candidates not already in the segment and strictly below their own
 *       {@code ceil(ideal)}), choose the one that maximises
 *       {@code delta = W's rank - candidate's rank} (highest net fitness improvement),
 *       breaking ties by largest candidate deficit.</li>
 *   <li>If Pass 1 found nothing (all excess is at position 0),
 *       <b>Pass 2 — primary-vacating swap</b>: scan segments where {@code W} is primary.
 *       Promote the highest-ranked existing backup to primary (free reorder), then give the
 *       vacated backup slot to the best under-loaded candidate using the same scoring.</li>
 *   <li>If neither pass found a move, convergence is complete.</li>
 * </ol>
 *
 * <h2>Delta scoring</h2>
 * <p>{@code delta = donorRankInSegment - candidateRank}. A positive delta means we are replacing
 * a poor-fit donor with a better-fit candidate — net improvement for the segment. A negative delta
 * means we are replacing a good-fit donor with a worse candidate — unavoidable overhead. By
 * maximising delta we always prefer the swap that does the least harm to segment fitness, so that
 * the resulting CH stays as close to the pure rendezvous ideal as possible.</p>
 *
 * <h2>Algorithm details — primary redistribution</h2>
 * <p>On each iteration:</p>
 * <ol>
 *   <li>Find the node {@code W} with the highest excess {@code primaryOwned(W) - ceil(primaryIdeal(W)) > 0}.
 *       If no node exceeds its primary ceiling, stop.</li>
 *   <li>Scan every segment where {@code W} is primary and look for a backup {@code B} that is
 *       strictly below its own {@code ceil(primaryIdeal(B))}. Among candidates, choose the one
 *       that ranks highest in the rendezvous order for that segment.</li>
 *   <li>Swap: {@code B} becomes primary, {@code W} takes {@code B}'s former backup position.</li>
 * </ol>
 *
 * @see RendezvousConsistentHashFactory
 * @since 16.3
 */
final class SegmentOwnershipBalancer {

   private SegmentOwnershipBalancer() { }

   /**
    * Pluggable scoring strategy for {@link #redistributeTotal}.
    *
    * <p>Given a candidate swap — the donor node's rank in a segment ({@code donorRank}), the
    * replacement candidate's rank in the same segment ({@code candidateRank}), and the
    * candidate's current deficit below ideal ({@code deficit}) — implementations return
    * {@code true} if this swap is strictly better than the current best.</p>
    *
    * <p>The scorer is called once per eligible {@code (segment, candidate)} pair.  It should
    * define a strict total order so that exactly one swap wins each iteration.</p>
    */
   @FunctionalInterface
   interface SwapScorer {
      /**
       * @param donorRank     rendezvous rank of the donor (over-loaded node) in this segment
       * @param candidateRank rendezvous rank of the replacement candidate in this segment
       * @param deficit       {@code ideal - owned} for the candidate (positive = under-loaded)
       * @param bestDonorRank     donor rank of the current best swap (or -1 if none yet)
       * @param bestCandidateRank candidate rank of the current best swap (or MAX if none yet)
       * @param bestDeficit       deficit of the current best swap (or -MAX_VALUE if none yet)
       * @return {@code true} if this swap should replace the current best
       */
      boolean isBetter(int donorRank, int candidateRank, float deficit,
                       int bestDonorRank, int bestCandidateRank, float bestDeficit);
   }

   /** Default scorer: shed weakest segment first (highest donor rank), then best candidate fit. */
   static final SwapScorer SHED_WEAKEST = (donorRank, candidateRank, deficit,
                                           bestDonorRank, bestCandidateRank, bestDeficit) ->
         donorRank > bestDonorRank
               || (donorRank == bestDonorRank && candidateRank < bestCandidateRank)
               || (donorRank == bestDonorRank && candidateRank == bestCandidateRank && deficit > bestDeficit);

   /**
    * Computes per-node ideal ownership counts, capping any node whose raw ideal would exceed
    * {@code numSegments} and redistributing the remainder.
    */
   static float[] computeIdeals(List<Address> members, Map<Address, Float> capacityFactors,
                                 float totalCapacity, int numSegments, int actualNumOwners) {
      int n = members.size();
      float[] ideal = new float[n];

      // Sort indices by descending capacity to apply capping in the right order
      Integer[] order = new Integer[n];
      for (int i = 0; i < n; i++) order[i] = i;
      Arrays.sort(order, (a, b) -> {
         float cfA = capacityFactors != null ? capacityFactors.getOrDefault(members.get(a), 1f) : 1f;
         float cfB = capacityFactors != null ? capacityFactors.getOrDefault(members.get(b), 1f) : 1f;
         return Float.compare(cfB, cfA); // descending
      });

      float remainingCap = totalCapacity;
      int remainingCopies = actualNumOwners * numSegments;
      for (int i : order) {
         float cf = capacityFactors != null ? capacityFactors.getOrDefault(members.get(i), 1f) : 1f;
         if (cf == 0f) {
            ideal[i] = 0f;
            continue;
         }
         float rawIdeal = remainingCopies * cf / remainingCap;
         if (rawIdeal > numSegments) {
            ideal[i] = numSegments;
            remainingCap -= cf;
            remainingCopies -= numSegments;
         } else {
            ideal[i] = rawIdeal;
         }
      }
      return ideal;
   }

   /**
    * Applies total-ownership and primary-ownership redistribution to {@code segmentOwners} in
    * place, producing a result that is within {@code floor/ceil} of each node's ideal for both
    * total and primary ownership.
    *
    * @param segmentOwners   mutable per-segment owner lists (modified in place)
    * @param rankings        per-segment rendezvous ranking (lower index = higher preference)
    * @param members         full ordered member list (index space for {@code owned} arrays)
    * @param capacityFactors per-node capacity factors (may be {@code null} for uniform capacity)
    * @param numSegments     number of segments
    * @param actualNumOwners effective number of owners per segment (min of configured and eligible)
    */
   static void apply(List<Address>[] segmentOwners, List<Address>[] rankings,
                     List<Address> members, Map<Address, Float> capacityFactors,
                     int numSegments, int actualNumOwners) {
      apply(segmentOwners, rankings, members, capacityFactors, numSegments, actualNumOwners, SHED_WEAKEST);
   }

   /**
    * Variant of {@link #apply} that accepts an explicit {@link SwapScorer} for testing alternative
    * scoring strategies.
    */
   static void apply(List<Address>[] segmentOwners, List<Address>[] rankings,
                     List<Address> members, Map<Address, Float> capacityFactors,
                     int numSegments, int actualNumOwners, SwapScorer scorer) {
      float totalCapacity = computeTotalCapacity(members, capacityFactors);
      float[] ideal = computeIdeals(members, capacityFactors, totalCapacity, numSegments, actualNumOwners);
      int[] owned = countOwned(segmentOwners, members);

      redistributeTotal(segmentOwners, rankings, members, owned, ideal, actualNumOwners, scorer);

      if (actualNumOwners > 1) {
         float[] primaryIdeal = computeIdeals(members, capacityFactors, totalCapacity, numSegments, 1);
         int[] primaryOwned = countPrimaryOwned(segmentOwners, members);
         redistributePrimary(segmentOwners, rankings, members, primaryOwned, primaryIdeal);
      }
   }

   // ---- Phase 2a: total ownership redistribution ----------------------------------------

   /**
    * Iteratively moves backup slots from over-loaded nodes to under-loaded nodes until no node
    * exceeds {@code ceil(ideal)} total ownership.
    *
    * <p>The swap to execute each iteration is chosen by the supplied {@link SwapScorer}.  The
    * default scorer ({@link #SHED_WEAKEST}) sheds the segment the donor is weakest in first
    * (highest donor rank), then picks the most natural replacement (lowest candidate rank),
    * breaking ties by largest candidate deficit.</p>
    *
    * <p>When the worst node's excess is entirely at position 0 (it holds only primary slots),
    * a primary-vacating fallback is used: the best-ranked existing backup is promoted to
    * primary (free reorder), then the vacated backup slot is handed to the chosen candidate.</p>
    *
    * <p>Nodes that remain below {@code floor(ideal)} after convergence are intentionally
    * left as-is — see class-level Javadoc for the rationale.</p>
    *
    * @param segmentOwners   mutable per-segment owner lists (modified in place)
    * @param rankings        per-segment rendezvous ranking
    * @param members         full ordered member list
    * @param owned           current per-node total ownership counts (modified in place)
    * @param ideal           per-node ideal total ownership (floating point for proportional capacity)
    * @param actualNumOwners effective owners per segment
    * @param scorer          swap scoring strategy
    */
   static void redistributeTotal(List<Address>[] segmentOwners, List<Address>[] rankings,
                                  List<Address> members, int[] owned, float[] ideal,
                                  int actualNumOwners, SwapScorer scorer) {
      if (actualNumOwners <= 1) {
         // With a single owner per segment, position 0 must be used; handle as primary-only
         redistributeSingleOwner(segmentOwners, rankings, members, owned, ideal);
         return;
      }

      int n = members.size();

      while (true) {
         // Find the most over-loaded node: highest excess above ceil(ideal).
         int worstIdx = -1;
         float worstExcess = 0;
         for (int i = 0; i < n; i++) {
            float excess = owned[i] - ((float) Math.ceil(ideal[i]) + 1);
            if (excess > worstExcess) {
               worstExcess = excess;
               worstIdx = i;
            }
         }
         if (worstIdx == -1) break; // everyone is at or below ceil

         Address worstNode = members.get(worstIdx);

         // Pass 1: backup-slot swaps (worstNode at position >= 1, no primary disruption).
         // Select by: (1) highest donor rank — shed weakest segment first;
         // (2) lowest candidate rank — best natural replacement; (3) largest candidate deficit.
         int bestSegment = -1;
         int bestCandidateIdx = -1;
         int bestDonorRank = -1;
         int bestCandidateRank = Integer.MAX_VALUE;
         float bestDeficit = -Float.MAX_VALUE;

         for (int s = 0; s < segmentOwners.length; s++) {
            List<Address> owners = segmentOwners[s];
            int worstPos = owners.indexOf(worstNode);
            if (worstPos <= 0) continue; // worstNode is primary or absent in this segment

            List<Address> ranking = rankings[s];
            int worstRankInSegment = ranking.indexOf(worstNode);

            for (int rank = 0; rank < ranking.size(); rank++) {
               Address candidate = ranking.get(rank);
               int ci = members.indexOf(candidate);
               if (owned[ci] >= (int) Math.ceil(ideal[ci])) continue; // skip if at or above own ceiling
               if (owners.contains(candidate)) continue;
               float deficit = ideal[ci] - owned[ci];
               if (scorer.isBetter(worstRankInSegment, rank, deficit,
                     bestDonorRank, bestCandidateRank, bestDeficit)) {
                  bestDonorRank = worstRankInSegment;
                  bestCandidateRank = rank;
                  bestDeficit = deficit;
                  bestSegment = s;
                  bestCandidateIdx = ci;
               }
               break; // only need the best candidate per segment (ranking is sorted)
            }
         }

         if (bestSegment != -1) {
            // Backup-slot swap: replace worstNode at its backup position with bestCandidate
            List<Address> owners = segmentOwners[bestSegment];
            int worstPos = owners.indexOf(worstNode);
            owners.set(worstPos, members.get(bestCandidateIdx));
            owned[worstIdx]--;
            owned[bestCandidateIdx]++;
            continue;
         }

         // Pass 2: fallback — worstNode holds excess only as primary; promote a backup and give
         // the vacated backup slot to the best under-loaded candidate.
         // Same shed-weakest-first scoring as pass 1, applied to primary slots.
         bestDonorRank = -1;
         bestCandidateRank = Integer.MAX_VALUE;
         bestDeficit = -Float.MAX_VALUE;
         int bestPromoteIdx = -1; // member index of the backup to promote to primary

         for (int s = 0; s < segmentOwners.length; s++) {
            List<Address> owners = segmentOwners[s];
            if (!owners.get(0).equals(worstNode)) continue; // worstNode must be primary

            List<Address> ranking = rankings[s];
            int worstRankInSegment = ranking.indexOf(worstNode);

            int promoteIdx = findBestBackupToPromote(owners, ranking, members);
            for (int rank = 0; rank < ranking.size(); rank++) {
               Address candidate = ranking.get(rank);
               int ci = members.indexOf(candidate);
               if (owned[ci] >= (int) Math.ceil(ideal[ci])) continue; // skip if at or above own ceiling
               if (owners.contains(candidate)) continue;
               float deficit = ideal[ci] - owned[ci];
               if (scorer.isBetter(worstRankInSegment, rank, deficit,
                     bestDonorRank, bestCandidateRank, bestDeficit)) {
                  bestDonorRank = worstRankInSegment;
                  bestCandidateRank = rank;
                  bestDeficit = deficit;
                  bestSegment = s;
                  bestCandidateIdx = ci;
                  bestPromoteIdx = promoteIdx;
               }
               break;
            }
         }

         if (bestSegment == -1) break; // truly stuck — converged as far as possible

         // Primary-vacating swap: promote bestPromote to primary, put candidate at backup slot
         List<Address> owners = segmentOwners[bestSegment];
         Address newPrimary = members.get(bestPromoteIdx);
         Address candidate = members.get(bestCandidateIdx);
         owners.remove(worstNode);                            // remove worstNode (was at position 0)
         owners.set(owners.indexOf(newPrimary), candidate);  // candidate takes newPrimary's old backup slot
         owners.add(0, newPrimary);                          // newPrimary moves to position 0
         owned[worstIdx]--;
         owned[bestCandidateIdx]++;
      }
   }

   /**
    * Returns the member index of the backup in {@code owners} (positions 1..N-1) that has the
    * best (lowest) rendezvous rank — this is the backup to promote to primary when the current
    * primary must vacate its slot.
    */
   private static int findBestBackupToPromote(List<Address> owners, List<Address> ranking,
                                              List<Address> members) {
      int bestPromoteIdx = -1;
      int bestBackupRank = Integer.MAX_VALUE;
      for (int b = 1; b < owners.size(); b++) {
         int backupRank = ranking.indexOf(owners.get(b));
         if (backupRank < bestBackupRank) {
            bestBackupRank = backupRank;
            bestPromoteIdx = members.indexOf(owners.get(b));
         }
      }
      return bestPromoteIdx;
   }

   /**
    * Variant of {@link #redistributeTotal} for {@code numOwners=1}: every segment has exactly
    * one owner at position 0, so there is no primary/backup distinction — position 0 may be
    * freely reassigned.
    *
    * <p>Among all segments owned by the worst node, pick the swap that maximises
    * {@code delta = worstRankInSegment - candidateRank}, breaking ties by largest candidate
    * deficit. This keeps the resulting assignment as close to the natural rendezvous ranking
    * as possible while still draining the over-loaded node.</p>
    *
    * @param segmentOwners mutable per-segment owner lists (modified in place)
    * @param rankings      per-segment rendezvous ranking
    * @param members       full ordered member list
    * @param owned         current per-node ownership counts (modified in place)
    * @param ideal         per-node ideal ownership
    */
   private static void redistributeSingleOwner(List<Address>[] segmentOwners, List<Address>[] rankings,
                                                List<Address> members, int[] owned, float[] ideal) {
      int n = members.size();

      boolean changed = true;
      while (changed) {
         changed = false;

         int worstIdx = -1;
         float worstExcess = 0;
         for (int i = 0; i < n; i++) {
            // Use ceil (not ceil+1): with numOwners=1 every segment is a primary, so
            // any node above its ideal ceiling is visible imbalance that must be corrected.
            float excess = owned[i] - (float) Math.ceil(ideal[i]);
            if (excess > worstExcess) {
               worstExcess = excess;
               worstIdx = i;
            }
         }
         if (worstIdx == -1) break;

         Address worstNode = members.get(worstIdx);
         int bestSegment = -1;
         int bestCandidateIdx = -1;
         int bestDelta = Integer.MIN_VALUE;
         float bestDeficit = -Float.MAX_VALUE;

         for (int s = 0; s < segmentOwners.length; s++) {
            if (!segmentOwners[s].get(0).equals(worstNode)) continue;
            List<Address> ranking = rankings[s];
            int worstRankInSegment = ranking.indexOf(worstNode);
            for (int rank = 0; rank < ranking.size(); rank++) {
               Address candidate = ranking.get(rank);
               if (candidate.equals(worstNode)) continue;
               int ci = members.indexOf(candidate);
               if (owned[ci] >= (int) Math.ceil(ideal[ci])) continue; // skip if at or above own ceiling
               int delta = worstRankInSegment - rank;
               float deficit = ideal[ci] - owned[ci];
               if (delta > bestDelta || (delta == bestDelta && deficit > bestDeficit)) {
                  bestDelta = delta;
                  bestDeficit = deficit;
                  bestSegment = s;
                  bestCandidateIdx = ci;
               }
               break; // only need best candidate per segment
            }
         }
         if (bestSegment == -1) break;

         segmentOwners[bestSegment].set(0, members.get(bestCandidateIdx));
         owned[worstIdx]--;
         owned[bestCandidateIdx]++;
         changed = true;
      }
   }

   // ---- Phase 2b: primary ownership redistribution --------------------------------------

   /**
    * Iteratively swaps primaries with backups to bring every node within {@code floor/ceil} of
    * its primary ideal. Because only the order within each segment's owner <em>set</em> changes,
    * no data movement occurs — this pass is entirely free from a data-placement perspective.
    *
    * <p>On each iteration: find the node {@code W} with the highest excess
    * {@code primaryOwned(W) - ceil(primaryIdeal(W)) > 0}. Then scan segments where {@code W} is
    * primary and look for a backup {@code B} with
    * {@code primaryOwned(B) < primaryOwned(W) - 1}. Among eligible {@code (segment, B)} pairs,
    * prefer the one where {@code B} ranks highest in the rendezvous order (lowest index) — this
    * keeps the primary assignment as close to the natural rendezvous preference as possible.</p>
    *
    * @param segmentOwners  mutable per-segment owner lists (modified in place)
    * @param rankings       per-segment rendezvous ranking
    * @param members        full ordered member list
    * @param primaryOwned   current per-node primary ownership counts (modified in place)
    * @param primaryIdeal   per-node ideal primary ownership (floating point for proportional capacity)
    */
   static void redistributePrimary(List<Address>[] segmentOwners, List<Address>[] rankings,
                                    List<Address> members, int[] primaryOwned,
                                    float[] primaryIdeal) {
      int n = members.size();
      boolean changed = true;
      while (changed) {
         changed = false;

         int worstIdx = -1;
         float worstExcess = 0;
         for (int i = 0; i < n; i++) {
            // Use ceil (not ceil+1) so that any node above its ideal ceiling is corrected.
            // The +1 buffer used in total redistribution would allow a node to hold two more
            // primaries than its ceiling, causing visible imbalance in small clusters.
            float excess = primaryOwned[i] - (float) Math.ceil(primaryIdeal[i]);
            if (excess > worstExcess) {
               worstExcess = excess;
               worstIdx = i;
            }
         }
         if (worstIdx == -1) break;

         Address worstNode = members.get(worstIdx);
         int bestSegment = -1;
         int bestCandidateIdx = -1;
         int bestRank = Integer.MAX_VALUE;

         for (int s = 0; s < segmentOwners.length; s++) {
            List<Address> owners = segmentOwners[s];
            if (!owners.get(0).equals(worstNode)) continue;

            List<Address> ranking = rankings[s];
            for (int b = 1; b < owners.size(); b++) {
               Address backup = owners.get(b);
               int bi = members.indexOf(backup);
               if (primaryOwned[bi] >= (int) Math.ceil(primaryIdeal[bi])) continue; // skip if at or above own ceiling
               int rank = ranking.indexOf(backup);
               if (rank < bestRank) {
                  bestRank = rank;
                  bestSegment = s;
                  bestCandidateIdx = bi;
               }
            }
         }

         if (bestSegment == -1) break;

         // Swap: promote bestCandidate to primary, demote worstNode to its former backup position
         List<Address> owners = segmentOwners[bestSegment];
         Address newPrimary = members.get(bestCandidateIdx);
         int backupPos = owners.indexOf(newPrimary);
         owners.set(backupPos, worstNode); // worstNode takes the backup position
         owners.set(0, newPrimary);        // newPrimary promoted to position 0
         primaryOwned[worstIdx]--;
         primaryOwned[bestCandidateIdx]++;
         changed = true;
      }
   }

   // ---- Counting helpers ----------------------------------------------------------------

   /**
    * Counts total ownership (across all positions) for each member.
    *
    * @param segmentOwners per-segment owner lists
    * @param members       full ordered member list (defines index space)
    * @return array of length {@code members.size()} with per-member total ownership counts
    */
   static int[] countOwned(List<Address>[] segmentOwners, List<Address> members) {
      int[] owned = new int[members.size()];
      for (List<Address> owners : segmentOwners) {
         for (Address a : owners) owned[members.indexOf(a)]++;
      }
      return owned;
   }

   /**
    * Counts primary ownership (position 0 only) for each member.
    *
    * @param segmentOwners per-segment owner lists
    * @param members       full ordered member list (defines index space)
    * @return array of length {@code members.size()} with per-member primary ownership counts
    */
   static int[] countPrimaryOwned(List<Address>[] segmentOwners, List<Address> members) {
      int[] primaryOwned = new int[members.size()];
      for (List<Address> owners : segmentOwners) {
         if (!owners.isEmpty()) primaryOwned[members.indexOf(owners.get(0))]++;
      }
      return primaryOwned;
   }

   static float computeTotalCapacity(List<Address> members, Map<Address, Float> capacityFactors) {
      if (capacityFactors == null) return members.size();
      float total = 0;
      for (Address m : members) total += capacityFactors.getOrDefault(m, 1f);
      return total;
   }
}
