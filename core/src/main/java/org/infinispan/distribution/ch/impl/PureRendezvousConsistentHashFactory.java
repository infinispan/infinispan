package org.infinispan.distribution.ch.impl;

import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

import org.infinispan.commons.hash.MurmurHash3;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.distribution.ch.PersistedConsistentHash;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;

/**
 * A {@link ConsistentHashFactory} that uses pure rendezvous (highest-random-weight) hashing to
 * assign segments to nodes.
 *
 * <p>Each segment's owner list is determined solely by the rendezvous ranking: for each segment,
 * all eligible nodes are scored by {@code murmurHash3(segment, msb, lsb) / capacityFactor} and
 * the top {@code actualNumOwners} nodes are assigned as owners (position 0 = primary).
 * No load-cap, no balance-correction pass, and no primary-swap pass are applied.</p>
 *
 * <p>Because assignment is a pure function of the node UUIDs and capacity factors, the factory is
 * fully deterministic: two caches with the same member list produce the same consistent hash,
 * regardless of topology history.</p>
 *
 * <p>Capacity factors affect the score denominator proportionally, so higher-capacity nodes win
 * more segments in expectation. However, due to hash variance, the actual distribution may deviate
 * from the ideal proportions by a non-trivial amount, especially for small segment counts or
 * unequal cluster sizes. Use {@link RendezvousConsistentHashFactory} for tighter floor/ceil
 * balance guarantees.</p>
 *
 * <p>This factory requires all cluster members to be at or above
 * {@link org.infinispan.remoting.transport.NodeVersion#SIXTEEN_THREE}. When used in a
 * mixed-version cluster, {@code ClusterCacheStatus} automatically falls back to
 * {@link SyncConsistentHashFactory} until all nodes are upgraded.</p>
 *
 * @author wburns
 * @since 16.3
 */
@ProtoTypeId(ProtoStreamTypeIds.PURE_RENDEZVOUS_CONSISTENT_HASH_FACTORY)
public class PureRendezvousConsistentHashFactory extends AbstractConsistentHashFactory<DefaultConsistentHash> {

   private static final PureRendezvousConsistentHashFactory INSTANCE = new PureRendezvousConsistentHashFactory();

   protected PureRendezvousConsistentHashFactory() { }

   @ProtoFactory
   public static PureRendezvousConsistentHashFactory getInstance() {
      return INSTANCE;
   }

   @Override
   public DefaultConsistentHash create(int numOwners, int numSegments,
                                       List<Address> members, Map<Address, Float> capacityFactors) {
      if (members.isEmpty())
         throw new IllegalArgumentException("Can't construct a consistent hash without any members");
      if (numOwners <= 0)
         throw new IllegalArgumentException("The number of owners should be greater than 0");
      checkCapacityFactors(members, capacityFactors);

      if (numSegments < members.size()) {
         CONTAINER.debugf("PureRendezvousConsistentHashFactory: numSegments (%d) < numNodes (%d), " +
               "assignment diversity may be reduced", numSegments, members.size());
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

      // Pre-compute rankings for coverage promotion
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
    * Builds a DefaultConsistentHash using pure rendezvous ranking.
    *
    * <p>For each segment, eligible nodes are sorted by {@code score(node, segment) = hash / cf}
    * (ascending) and the first {@code actualNumOwners} are assigned as owners. No load cap,
    * balance correction, or primary-swap pass is applied.</p>
    *
    * <p>Subclasses may override this method to add load-balancing corrections while still reusing
    * the ranking infrastructure provided here.</p>
    */
   DefaultConsistentHash build(int numOwners, int numSegments, List<Address> members,
                                Map<Address, Float> capacityFactors) {
      int actualNumOwners = computeActualNumOwners(numOwners, members, capacityFactors);

      // Generate rankings for all segment and node combinations
      List<Address>[] rankings = computeRankings(numSegments, members, capacityFactors);

      // The highest ranked is the primary owner and any next in line are the backup(s).
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
    * Score: murmurHash3(segmentIndex, msb, lsb) / capacityFactor(node).
    * Lower score wins (ascending order). Zero-capacity nodes are excluded.
    */
   protected List<Address>[] computeRankings(int numSegments, List<Address> members,
                                              Map<Address, Float> capacityFactors) {
      // Filter out zero-capacity nodes
      List<Address> eligible = new ArrayList<>(members.size());
      for (Address m : members) {
         float cf = capacityFactors != null ? capacityFactors.getOrDefault(m, 1f) : 1f;
         if (cf > 0) eligible.add(m);
      }

      int n = eligible.size();
      // Pre-compute UUID bits for each eligible node
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
            // hash(long[]) — combine segment index with the two UUID halves
            int hash = MurmurHash3.hash(new long[]{s, nodeMsb[i], nodeLsb[i]});
            // Use unsigned int value as score numerator
            double rawScore = Integer.toUnsignedLong(hash);
            float cf = capacityFactors != null ? capacityFactors.getOrDefault(eligible.get(i), 1f) : 1f;
            scores[i] = rawScore / cf;
            order[i] = i;
         }
         // Sort ascending by score — lower is better
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

   @Override
   public boolean equals(Object other) {
      return other != null && other.getClass() == getClass();
   }

   @Override
   public int hashCode() {
      return 5381;
   }
}
