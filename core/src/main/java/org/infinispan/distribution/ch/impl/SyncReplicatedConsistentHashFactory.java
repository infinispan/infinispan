package org.infinispan.distribution.ch.impl;

import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;

/**
 * {@link SyncConsistentHashFactory} adapted for replicated caches, so that the primary owner of a key
 * is the same in replicated and distributed caches.
 *
 * @author Dan Berindei
 * @since 8.2
 */
@ProtoTypeId(ProtoStreamTypeIds.SYNC_REPLICATED_CONSISTENT_HASH)
public class SyncReplicatedConsistentHashFactory implements ConsistentHashFactory<ReplicatedConsistentHash> {

   private static final SyncConsistentHashFactory syncCHF = new SyncConsistentHashFactory();

   @Override
   public ReplicatedConsistentHash create(int numOwners, int numSegments,
         List<Address> members, Map<Address, Float> capacityFactors) {
      DefaultConsistentHash dch = syncCHF.create(1, numSegments, members, capacityFactors);
      List<Address> membersWithoutState = computeMembersWithoutState(members, null, capacityFactors);
      return replicatedFromDefault(dch, membersWithoutState);
   }

   @Override
   public ReplicatedConsistentHash fromPersistentState(ScopedPersistentState state) {
      String consistentHashClass = state.getProperty("consistentHash");
      if (!ReplicatedConsistentHash.class.getName().equals(consistentHashClass))
         throw CONTAINER.persistentConsistentHashMismatch(this.getClass().getName(), consistentHashClass);
      return new ReplicatedConsistentHash(state);
   }

   private ReplicatedConsistentHash replicatedFromDefault(DefaultConsistentHash dch,
                                                          List<Address> membersWithoutState) {
      int numSegments = dch.getNumSegments();
      List<Address> members = dch.getMembers();
      List<Integer> primaryOwners = new ArrayList<>(numSegments);
      for (int segment = 0; segment < numSegments; segment++) {
         primaryOwners.add(members.indexOf(dch.locatePrimaryOwnerForSegment(segment)));
      }
      return new ReplicatedConsistentHash(members, dch.getCapacityFactors(), membersWithoutState, primaryOwners);
   }

   @Override
   public ReplicatedConsistentHash updateMembers(ReplicatedConsistentHash baseCH, List<Address> newMembers,
         Map<Address, Float> actualCapacityFactors) {
      DefaultConsistentHash baseDCH = defaultFromReplicated(baseCH);
      DefaultConsistentHash dch = syncCHF.updateMembers(baseDCH, newMembers, actualCapacityFactors);
      List<Address> membersWithoutState = computeMembersWithoutState(newMembers, baseCH.getMembers(), actualCapacityFactors);
      return replicatedFromDefault(dch, membersWithoutState);
   }

   private DefaultConsistentHash defaultFromReplicated(ReplicatedConsistentHash baseCH) {
      int numSegments = baseCH.getNumSegments();
      List<Address>[] baseSegmentOwners = new List[numSegments];
      for (int segment = 0; segment < numSegments; segment++) {
         baseSegmentOwners[segment] = Collections.singletonList(baseCH.locatePrimaryOwnerForSegment(segment));
      }
      return new DefaultConsistentHash(1,
            numSegments, baseCH.getMembers(), baseCH.getCapacityFactors(), baseSegmentOwners);
   }

   @Override
   public ReplicatedConsistentHash rebalance(ReplicatedConsistentHash baseCH) {
      return create(baseCH.getNumOwners(), baseCH.getNumSegments(), baseCH.getMembers(), baseCH.getCapacityFactors());
   }

   @Override
   public ReplicatedConsistentHash union(ReplicatedConsistentHash ch1, ReplicatedConsistentHash ch2) {
      return ch1.union(ch2);
   }

   static List<Address> computeMembersWithoutState(List<Address> newMembers, List<Address> oldMembers, Map<Address, Float> capacityFactors) {
      List<Address> membersWithoutState = Collections.emptyList();
      if (capacityFactors != null) {
         boolean hasNodeWithCapacity = false;
         for (Address a : newMembers) {
            float capacityFactor = capacityFactors.get(a);
            if (capacityFactor != 0f && capacityFactor != 1f) {
               throw new IllegalArgumentException("Invalid replicated cache capacity factor for node " + a);
            }
            if (capacityFactor == 0f || (oldMembers != null && !oldMembers.contains(a))) {
               if (membersWithoutState.isEmpty()) {
                  membersWithoutState = new ArrayList<>();
               }
               membersWithoutState.add(a);
            } else {
               hasNodeWithCapacity = true;
            }
         }
         if (!hasNodeWithCapacity) {
            throw new IllegalArgumentException("There must be at least one node with a non-zero capacity factor");
         }
      }
      return membersWithoutState;
   }
}
