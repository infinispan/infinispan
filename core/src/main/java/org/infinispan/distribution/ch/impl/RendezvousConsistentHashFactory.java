package org.infinispan.distribution.ch.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;

/**
 * A {@link ConsistentHashFactory} that combines rendezvous (highest-random-weight) hashing with
 * a convergent load-balancing post-processing step to guarantee that no node owns more than
 * {@code ceil(ideal)} segments.
 *
 * <p>Extends {@link PureRendezvousConsistentHashFactory}. The {@code build()} method runs in two
 * phases:</p>
 * <ol>
 *   <li><b>Phase 1 — pure rendezvous assignment</b>: for each segment, assign the top-N ranked
 *       nodes by rendezvous score.  This produces the natural, movement-minimal initial
 *       assignment.</li>
 *   <li><b>Phase 2 — load balancing</b>: delegate to {@link SegmentOwnershipBalancer} which drains
 *       any node above {@code ceil(ideal)} by moving its excess slots to under-loaded candidates,
 *       then redistributes primary ownership via free position swaps to achieve tight
 *       {@code floor/ceil} primary balance as well.</li>
 * </ol>
 *
 * <p>Unlike {@link DefaultConsistentHashFactory}, the result is fully deterministic: two caches
 * with the same member list produce the same consistent hash, regardless of topology history.
 * Unlike {@link PureRendezvousConsistentHashFactory}, no node will own more than
 * {@code ceil(ideal)} segments.</p>
 *
 * <p>The load-balancing logic is intentionally isolated in {@link SegmentOwnershipBalancer} so that
 * other initial mapping strategies (e.g. topology-aware rendezvous) can compose with the same
 * redistribution pass without duplication.</p>
 *
 * <p>This factory requires all cluster members to be at or above
 * {@link org.infinispan.remoting.transport.NodeVersion#RENDEZVOUS_CH_MIN_VERSION}. When used in a
 * mixed-version cluster, {@code ClusterCacheStatus} automatically falls back to
 * {@link SyncConsistentHashFactory} until all nodes are upgraded.</p>
 *
 * @author wburns
 * @since 16.3
 * @see SegmentOwnershipBalancer
 */
@ProtoTypeId(ProtoStreamTypeIds.RENDEZVOUS_CONSISTENT_HASH_FACTORY)
public class RendezvousConsistentHashFactory extends PureRendezvousConsistentHashFactory {

   private static final RendezvousConsistentHashFactory INSTANCE = new RendezvousConsistentHashFactory();

   protected RendezvousConsistentHashFactory() { }

   @ProtoFactory
   public static RendezvousConsistentHashFactory getInstance() {
      return INSTANCE;
   }

   /**
    * Builds a floor/ceil-balanced {@link DefaultConsistentHash} in two phases:
    * <ol>
    *   <li><b>Phase 1</b>: pure rendezvous assignment — top-N nodes per segment by score.</li>
    *   <li><b>Phase 2</b>: load balancing via {@link SegmentOwnershipBalancer#apply} — drains
    *       over-assigned nodes and redistributes primary ownership within existing owner sets.</li>
    * </ol>
    */
   @Override
   DefaultConsistentHash build(int numOwners, int numSegments, List<Address> members,
                                Map<Address, Float> capacityFactors) {
      int actualNumOwners = computeActualNumOwners(numOwners, members, capacityFactors);

      // Phase 1: pure rendezvous — establishes the natural, movement-minimal initial assignment
      List<Address>[] rankings = computeRankings(numSegments, members, capacityFactors);

      // Initially order the same as Pure, but the second pass uses this ordering the full rankings to determine
      // if any nodes must be moved around to ensure ceil(ideal) + 1 segments per node
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

      // Phase 2: load balancing — converge to floor/ceil total and primary balance
      SegmentOwnershipBalancer.apply(segmentOwners, rankings, members, capacityFactors,
            numSegments, actualNumOwners);

      return DefaultConsistentHash.create(numOwners, numSegments, members, capacityFactors, segmentOwners);
   }

   @Override
   public boolean equals(Object other) {
      return other != null && other.getClass() == getClass();
   }

   @Override
   public int hashCode() {
      return 6427;
   }
}
