package org.infinispan.distribution.ch.impl;

import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.distribution.ch.PersistedConsistentHash;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.remoting.transport.Address;

/**
 * Abstract base for rendezvous-based consistent hash factories.
 *
 * <p>Provides the core rendezvous infrastructure: {@link #computeRankings},
 * {@link #computeActualNumOwners}, and the full {@link ConsistentHashFactory} contract
 * ({@code create}, {@code updateMembers}, {@code rebalance}, {@code union},
 * {@code fromPersistentState}). Subclasses override {@link #build} to add load-balancing
 * or history-hinted redistribution on top of the raw rendezvous ranking.</p>
 *
 * <p>Used as the {@code instanceof} target in
 * {@code ClusterCacheStatus.effectiveConsistentHashFactory()} to detect all rendezvous
 * variants and apply the rolling-upgrade version guard.</p>
 *
 * @author wburns
 * @since 16.3
 */
public abstract class PureRendezvousConsistentHashFactory
      extends AbstractConsistentHashFactory<DefaultConsistentHash> {

   @Override
   public DefaultConsistentHash create(int numOwners, int numSegments,
                                       List<Address> members, Map<Address, Float> capacityFactors) {
      if (members.isEmpty())
         throw new IllegalArgumentException("Can't construct a consistent hash without any members");
      if (numOwners <= 0)
         throw new IllegalArgumentException("The number of owners should be greater than 0");
      checkCapacityFactors(members, capacityFactors);

      if (numSegments < members.size()) {
         CONTAINER.debugf("%s: numSegments (%d) < numNodes (%d), " +
               "assignment diversity may be reduced", getClass().getSimpleName(), numSegments, members.size());
      }

      return build(numOwners, numSegments, members, capacityFactors);
   }

   @Override
   public DefaultConsistentHash updateMembers(DefaultConsistentHash baseCH, List<Address> newMembers,
                                              Map<Address, Float> newCapacityFactors) {
      if (newMembers.isEmpty())
         throw new IllegalArgumentException("Can't construct a consistent hash without any members");
      checkCapacityFactors(newMembers, newCapacityFactors);

      boolean sameCapacityFactors = newCapacityFactors == null ? baseCH.getCapacityFactors() == null :
            newCapacityFactors.equals(baseCH.getCapacityFactors());
      if (newMembers.equals(baseCH.getMembers()) && sameCapacityFactors)
         return baseCH;

      // Coverage-only pass: remove leavers, promote next-ranked node for bare coverage.
      // Full rebalance is deferred to rebalance().
      int numSegments = baseCH.getNumSegments();
      int numOwners = baseCH.getNumOwners();
      int actualNumOwners = computeActualNumOwners(numOwners, newMembers, newCapacityFactors);

      List<Address>[] rankings = computeRankings(numSegments, newMembers, newCapacityFactors);

      @SuppressWarnings("unchecked")
      List<Address>[] segmentOwners = new List[numSegments];
      for (int s = 0; s < numSegments; s++) {
         List<Address> existing = new ArrayList<>(baseCH.locateOwnersForSegment(s));
         existing.retainAll(newMembers);
         if (existing.isEmpty()) {
            // Segment lost all owners — promote coverage from rendezvous ranking
            for (Address candidate : rankings[s]) {
               existing.add(candidate);
               if (existing.size() >= actualNumOwners) break;
            }
         }
         segmentOwners[s] = existing;
      }
      return DefaultConsistentHash.create(numOwners, numSegments, newMembers, newCapacityFactors, segmentOwners);
   }

   @Override
   public DefaultConsistentHash rebalance(DefaultConsistentHash baseCH) {
      DefaultConsistentHash rebalanced = build(baseCH.getNumOwners(), baseCH.getNumSegments(),
            baseCH.getMembers(), baseCH.getCapacityFactors());
      return rebalanced.equals(baseCH) ? baseCH : rebalanced;
   }

   @Override
   public DefaultConsistentHash union(DefaultConsistentHash ch1, DefaultConsistentHash ch2) {
      return ch1.union(ch2);
   }

   @Override
   public PersistedConsistentHash<DefaultConsistentHash> fromPersistentState(ScopedPersistentState state,
                                                                              Function<UUID, Address> addressMapper) {
      String consistentHashClass = state.getProperty("consistentHash");
      if (!DefaultConsistentHash.class.getName().equals(consistentHashClass))
         throw CONTAINER.persistentConsistentHashMismatch(this.getClass().getName(), consistentHashClass);
      return DefaultConsistentHash.fromPersistentState(state, addressMapper);
   }

   // ---- Core algorithm ----

   /**
    * Builds a {@link DefaultConsistentHash} from scratch for the given parameters.
    *
    * <p>The base implementation assigns each segment's owners as the top-{@code actualNumOwners}
    * nodes by rendezvous score — pure, no load cap or balance correction. Subclasses override this
    * to add load-balancing passes on top of the raw ranking.</p>
    */
   DefaultConsistentHash build(int numOwners, int numSegments, List<Address> members,
                                Map<Address, Float> capacityFactors) {
      int actualNumOwners = computeActualNumOwners(numOwners, members, capacityFactors);
      List<Address>[] rankings = computeRankings(numSegments, members, capacityFactors);

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
      return DefaultConsistentHash.create(numOwners, numSegments, members, capacityFactors, segmentOwners);
   }

   /**
    * Computes the rendezvous ranking for each segment.
    * Score: {@code murmurHash3(segmentIndex, msb, lsb) / capacityFactor(node)}.
    * Lower score wins. Zero-capacity nodes are excluded.
    */
   protected List<Address>[] computeRankings(int numSegments, List<Address> members,
                                              Map<Address, Float> capacityFactors) {
      List<Address> eligible = new ArrayList<>(members.size());
      for (Address m : members) {
         float cf = capacityFactors != null ? capacityFactors.getOrDefault(m, 1f) : 1f;
         if (cf > 0) eligible.add(m);
      }

      int n = eligible.size();
      long[] nodeLsb = new long[n];
      long[] nodeMsb = new long[n];
      for (int i = 0; i < n; i++) {
         nodeLsb[i] = eligible.get(i).getLeastSignificantBits();
         nodeMsb[i] = eligible.get(i).getMostSignificantBits();
      }

      @SuppressWarnings("unchecked")
      List<Address>[] rankings = new List[numSegments];
      double[] scores = new double[n];
      Integer[] order = new Integer[n];

      for (int s = 0; s < numSegments; s++) {
         for (int i = 0; i < n; i++) {
            int hash = MurmurHash3.hash(new long[]{s, nodeMsb[i], nodeLsb[i]});
            double rawScore = Integer.toUnsignedLong(hash);
            float cf = capacityFactors != null ? capacityFactors.getOrDefault(eligible.get(i), 1f) : 1f;
            scores[i] = rawScore / cf;
            order[i] = i;
         }
         final double[] finalScores = scores;
         Arrays.sort(order, (a, b) -> Double.compare(finalScores[a], finalScores[b]));

         List<Address> ranking = new ArrayList<>(n);
         for (int idx : order) ranking.add(eligible.get(idx));
         rankings[s] = ranking;
      }
      return rankings;
   }

   // ---- Helpers ----

   static int computeActualNumOwners(int numOwners, List<Address> members, Map<Address, Float> capacityFactors) {
      int nodesWithLoad = 0;
      for (Address m : members) {
         float cf = capacityFactors != null ? capacityFactors.getOrDefault(m, 1f) : 1f;
         if (cf > 0) nodesWithLoad++;
      }
      return Math.min(numOwners, nodesWithLoad);
   }
}
