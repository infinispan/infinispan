package org.infinispan.distribution.ch.impl;

import static org.infinispan.distribution.ch.impl.AbstractConsistentHash.STATE_CAPACITY_FACTOR;
import static org.infinispan.distribution.ch.impl.AbstractConsistentHash.STATE_CAPACITY_FACTORS;
import static org.infinispan.distribution.ch.impl.AbstractConsistentHash.writeAddressToState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.PersistedConsistentHash;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.marshall.protostream.impl.MarshallableMap;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;

/**
 * Special implementation of {@link ConsistentHash} for replicated caches.
 * The hash-space has several segments owned by all members and the primary ownership of each segment is evenly
 * spread between members.
 *
 * @author Dan Berindei
 * @author anistor@redhat.com
 * @since 5.2
 */
@ProtoTypeId(ProtoStreamTypeIds.REPLICATED_CONSISTENT_HASH)
public class ReplicatedConsistentHash implements ConsistentHash {

   private static final String STATE_PRIMARY_OWNERS = "primaryOwners.%d";
   private static final String STATE_PRIMARY_OWNERS_COUNT = "primaryOwners";
   private final List<Integer> primaryOwners;
   private final List<Address> members;
   private final List<Address> membersWithState;
   private final Set<Address> membersWithStateSet;
   private final List<Address> membersWithoutState;

   private final Map<Address, Float> capacityFactors;
   private final Set<Integer> segments;

   public ReplicatedConsistentHash(List<Address> members, List<Integer> primaryOwners) {
      this(members, null, Collections.emptyList(), primaryOwners);
   }

   public ReplicatedConsistentHash(List<Address> members, Map<Address, Float> capacityFactors, List<Address> membersWithoutState, List<Integer> primaryOwners) {
      this.members = List.copyOf(members);
      this.membersWithoutState = List.copyOf(membersWithoutState);
      this.membersWithState = computeMembersWithState(members, membersWithoutState);
      this.membersWithStateSet = Set.copyOf(this.membersWithState);
      this.primaryOwners = primaryOwners;
      this.capacityFactors = capacityFactors == null ? null : Map.copyOf(capacityFactors);
      this.segments = IntSets.immutableRangeSet(primaryOwners.size());
   }

   @ProtoFactory
   static ReplicatedConsistentHash protoFactory(List<Address> members, List<Integer> primaryOwners,
                                                MarshallableMap<Address, Float> capacityFactors,
                                                List<Address> membersWithoutState) {
      return new ReplicatedConsistentHash(
            members,
            MarshallableMap.unwrap(capacityFactors),
            membersWithoutState,
            primaryOwners
      );
   }

   @ProtoField(1)
   @Override
   public List<Address> getMembers() {
      return members;
   }

   @ProtoField(2)
   List<Integer> getPrimaryOwners() {
      return primaryOwners;
   }

   @ProtoField(3)
   MarshallableMap<Address, Float> capacityFactors() {
      return MarshallableMap.create(capacityFactors);
   }

   @ProtoField(4)
   List<Address> getMembersWithoutState() {
      return membersWithoutState;
   }

   public ReplicatedConsistentHash union(ReplicatedConsistentHash ch2) {
      if (this.getNumSegments() != ch2.getNumSegments())
         throw new IllegalArgumentException("The consistent hash objects must have the same number of segments");

      List<Address> unionMembers = new ArrayList<>(this.members);
      for (Address member : ch2.getMembers()) {
         if (!members.contains(member)) {
            unionMembers.add(member);
         }
      }

      List<Address> unionMembersWithoutState = new ArrayList<>(this.membersWithoutState);
      for (Address member : ch2.membersWithoutState) {
         if (!ch2.membersWithStateSet.contains(member) && !unionMembersWithoutState.contains(member)) {
            unionMembersWithoutState.add(member);
         }
      }

      int segments = this.getNumSegments();
      List<Integer> primaryOwners = new ArrayList<>(segments);
      for (int segmentId = 0; segmentId < segments; segmentId++) {
         Address primaryOwner = this.locatePrimaryOwnerForSegment(segmentId);
         int primaryOwnerIndex = unionMembers.indexOf(primaryOwner);
         primaryOwners.add(primaryOwnerIndex);
      }

      Map<Address, Float> unionCapacityFactors;
      if (capacityFactors == null && ch2.capacityFactors == null) {
         unionCapacityFactors = null;
      } else if (capacityFactors == null) {
         unionCapacityFactors = new HashMap<>(ch2.capacityFactors);
         for (Address address : members) {
            unionCapacityFactors.put(address, 1.0f);
         }
      } else if (ch2.capacityFactors == null) {
         unionCapacityFactors = new HashMap<>(capacityFactors);
         for (Address address : ch2.members) {
            unionCapacityFactors.put(address, 1.0f);
         }
      } else {
         unionCapacityFactors = new HashMap<>(capacityFactors);
         unionCapacityFactors.putAll(ch2.capacityFactors);
      }
      return new ReplicatedConsistentHash(unionMembers, unionCapacityFactors, unionMembersWithoutState, primaryOwners);
   }

   static PersistedConsistentHash<ReplicatedConsistentHash> fromPersistentScope(ScopedPersistentState state, Function<UUID, Address> addressMapper) {
      var members = parseMembers(state, ConsistentHashPersistenceConstants.STATE_MEMBERS,
            ConsistentHashPersistenceConstants.STATE_MEMBER, addressMapper, true);
      var missingUuids = new HashSet<>(members.missingUuids());
      var membersWithoutState = parseMembers(state, ConsistentHashPersistenceConstants.STATE_MEMBERS_NO_ENTRIES,
            ConsistentHashPersistenceConstants.STATE_MEMBER_NO_ENTRIES, addressMapper, false);
      missingUuids.addAll(membersWithoutState.missingUuids());
      var primaryOwners = parsePrimaryOwners(state);

      var ch = new ReplicatedConsistentHash(members.members(), members.capacityFactors(), membersWithoutState.members(), primaryOwners);
      return new PersistedConsistentHash<>(ch, missingUuids);
   }

   private static PersistedMembers parseMembers(ScopedPersistentState state, String numMembersPropertyName,
                                                String memberPropertyFormat, Function<UUID, Address> addressMapper, boolean parseCapacityFactors) {
      String property = state.getProperty(numMembersPropertyName);
      if (property == null) {
         return new PersistedMembers(List.of(), null, Set.of());
      }
      var numMembers = Integer.parseInt(property);
      var members = new ArrayList<Address>(numMembers);
      var missingUuids = new ArrayList<UUID>(numMembers);

      Map<Address, Float> capacityFactors = null;
      if (parseCapacityFactors) {
         capacityFactors = new HashMap<>();
      }
      var numCapacityFactorsString = state.getProperty(STATE_CAPACITY_FACTORS);
      var version11State = numCapacityFactorsString == null;

      for (int i = 0; i < numMembers; i++) {
         var uuid = UUID.fromString(state.getProperty(String.format(memberPropertyFormat, i)));
         var address = addressMapper.apply(uuid);
         if (address == null) {
            missingUuids.add(uuid);
         } else {
            members.add(address);
            if (capacityFactors == null) {
               continue;
            }
            if (version11State) {
               capacityFactors.put(address, 1f);
               continue;
            }
            var cf = state.getProperty(String.format(STATE_CAPACITY_FACTOR, i));
            if (cf != null) {
               capacityFactors.put(address, Float.parseFloat(cf));
            }
         }
      }
      return new PersistedMembers(members, capacityFactors, missingUuids);
   }

   private static List<Integer> parsePrimaryOwners(ScopedPersistentState state) {
      int numPrimaryOwners = state.getIntProperty(STATE_PRIMARY_OWNERS_COUNT);
      List<Integer> primaryOwners = new ArrayList<>(numPrimaryOwners);
      for (int i = 0; i < numPrimaryOwners; i++) {
         primaryOwners.add(state.getIntProperty(String.format(STATE_PRIMARY_OWNERS, i)));
      }
      return primaryOwners;
   }

   @Override
   public int getNumSegments() {
      return primaryOwners.size();
   }

   public int getNumOwners() {
      return membersWithState.size();
   }

   @Override
   public List<Address> locateOwnersForSegment(int segmentId) {
      Address primaryOwner = locatePrimaryOwnerForSegment(segmentId);
      List<Address> owners = new ArrayList<>(membersWithState.size());
      owners.add(primaryOwner);
      for (Address member : membersWithState) {
         if (!member.equals(primaryOwner)) {
            owners.add(member);
         }
      }
      return owners;
   }

   @Override
   public Address locatePrimaryOwnerForSegment(int segmentId) {
      return members.get(primaryOwners.get(segmentId));
   }

   @Override
   public Set<Integer> getSegmentsForOwner(Address owner) {
      if (owner == null) {
         throw new IllegalArgumentException("owner cannot be null");
      }

      if (membersWithStateSet.contains(owner))
         return segments;

      return IntSets.immutableEmptySet();
   }

   @Override
   public Set<Integer> getPrimarySegmentsForOwner(Address owner) {
      int index = members.indexOf(owner);
      if (index == -1) {
         return IntSets.immutableEmptySet();
      }
      IntSet primarySegments = IntSets.mutableEmptySet(primaryOwners.size());
      for (int i = 0; i < primaryOwners.size(); ++i) {
         if (primaryOwners.get(i) == index) {
            primarySegments.set(i);
         }
      }
      return primarySegments;
   }

   @Override
   public String getRoutingTableAsString() {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < primaryOwners.size(); i++) {
         if (i > 0) {
            sb.append(", ");
         }
         sb.append(i).append(": ").append(primaryOwners.get(i));
      }
      if (!membersWithoutState.isEmpty()) {
         sb.append("none:");
         for (Address a : membersWithoutState) {
            sb.append(' ').append(a);
         }
      }
      return sb.toString();
   }

   @Override
   public boolean isSegmentLocalToNode(Address nodeAddress, int segmentId) {
      return membersWithStateSet.contains(nodeAddress);
   }

   @Override

   public boolean isReplicated() {
      return true;
   }

   public void toScopedState(ScopedPersistentState state, Function<Address, UUID> addressMapper) {
      state.setProperty(ConsistentHashPersistenceConstants.STATE_CONSISTENT_HASH, this.getClass().getName());
      writeAddressToState(state, members, ConsistentHashPersistenceConstants.STATE_MEMBERS, ConsistentHashPersistenceConstants.STATE_MEMBER, addressMapper);
      writeAddressToState(state, membersWithoutState, ConsistentHashPersistenceConstants.STATE_MEMBERS_NO_ENTRIES, ConsistentHashPersistenceConstants.STATE_MEMBER_NO_ENTRIES, addressMapper);
      state.setProperty(STATE_CAPACITY_FACTORS, Integer.toString(capacityFactors.size()));
      for (int i = 0; i < members.size(); i++) {
         state.setProperty(String.format(STATE_CAPACITY_FACTOR, i),
                           capacityFactors.get(members.get(i)).toString());
      }
      state.setProperty(STATE_PRIMARY_OWNERS_COUNT, Integer.toString(primaryOwners.size()));
      for (int i = 0; i < primaryOwners.size(); i++) {
         state.setProperty(String.format(STATE_PRIMARY_OWNERS, i), Integer.toString(primaryOwners.get(i)));
      }
   }

   @Override
   public Map<Address, Float> getCapacityFactors() {
      return capacityFactors;
   }

   private List<Address> computeMembersWithState(List<Address> members, List<Address> membersWithoutState) {
      if (membersWithoutState.isEmpty()) {
         return members;
      } else {
         List<Address> membersWithState = new ArrayList<>(members);
         membersWithState.removeAll(membersWithoutState);
         return List.copyOf(membersWithState);
      }
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder("ReplicatedConsistentHash{");
      sb.append("ns = ").append(primaryOwners.size());
      sb.append(", owners = (").append(members.size()).append(")[");

      int[] primaryOwned = new int[members.size()];
      for (int primaryOwner : primaryOwners) {
         primaryOwned[primaryOwner]++;
      }

      boolean first = true;
      for (int i = 0; i < members.size(); i++) {
         Address a = members.get(i);
         if (first) {
            first = false;
         } else {
            sb.append(", ");
         }
         sb.append(a).append(": ").append(primaryOwned[i]);
         sb.append("+");
         if (membersWithStateSet.contains(a)) {
            sb.append(getNumSegments() - primaryOwned[i]);
         } else {
            sb.append("0");
         }
      }
      sb.append("]}");
      return sb.toString();
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((members == null) ? 0 : members.hashCode());
      result = prime * result + ((membersWithoutState == null) ? 0 : membersWithoutState.hashCode());
      result = prime * result + primaryOwners.hashCode();
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      ReplicatedConsistentHash other = (ReplicatedConsistentHash) obj;
      if (members == null) {
         if (other.members != null)
            return false;
      } else if (!members.equals(other.members))
         return false;
      if (membersWithoutState == null) {
         if (other.membersWithoutState != null)
            return false;
      } else if (!membersWithoutState.equals(other.membersWithoutState))
         return false;
      return Objects.equals(primaryOwners, other.primaryOwners);
   }
}
