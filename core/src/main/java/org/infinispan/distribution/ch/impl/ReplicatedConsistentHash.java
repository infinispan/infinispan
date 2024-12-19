package org.infinispan.distribution.ch.impl;

import static org.infinispan.distribution.ch.impl.AbstractConsistentHash.STATE_CAPACITY_FACTOR;
import static org.infinispan.distribution.ch.impl.AbstractConsistentHash.STATE_CAPACITY_FACTORS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.marshall.protostream.impl.MarshallableCollection;
import org.infinispan.marshall.protostream.impl.MarshallableMap;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.topology.PersistentUUID;

/**
 * Special implementation of {@link org.infinispan.distribution.ch.ConsistentHash} for replicated caches.
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
   private final int[] primaryOwners;
   private final List<Address> members;
   private final List<Address> membersWithState;
   private final Set<Address> membersWithStateSet;
   private final List<Address> membersWithoutState;

   private final Map<Address, Float> capacityFactors;
   private final Set<Integer> segments;

   public ReplicatedConsistentHash(List<Address> members, int[] primaryOwners) {
      this(members, null, Collections.emptyList(), primaryOwners);
   }

   public ReplicatedConsistentHash(List<Address> members, Map<Address, Float> capacityFactors, List<Address> membersWithoutState, int[] primaryOwners) {
      this.members = List.copyOf(members);
      this.membersWithoutState = List.copyOf(membersWithoutState);
      this.membersWithState = computeMembersWithState(members, membersWithoutState);
      this.membersWithStateSet = Set.copyOf(this.membersWithState);
      this.primaryOwners = primaryOwners;
      this.capacityFactors = capacityFactors == null ? null : Map.copyOf(capacityFactors);
      this.segments = IntSets.immutableRangeSet(primaryOwners.length);
   }

   @ProtoFactory
   static ReplicatedConsistentHash protoFactory(List<JGroupsAddress> jGroupsMembers, int[] primaryOwners,
                                                MarshallableMap<Address, Float> capacityFactors,
                                                MarshallableCollection<Address> membersWithoutState) {
      return new ReplicatedConsistentHash(
            (List<Address>)(List<?>) jGroupsMembers,
            MarshallableMap.unwrap(capacityFactors),
            MarshallableCollection.unwrap(membersWithoutState, ArrayList::new),
            primaryOwners
      );
   }

   // TODO no need for the casting if ConsistentHash interface updated to use `<? extends Address>`
   @ProtoField(number = 1, collectionImplementation = ArrayList.class)
   List<JGroupsAddress> getJGroupsMembers() {
      return (List<JGroupsAddress>)(List<?>) members;
   }

   @ProtoField(2)
   int[] getPrimaryOwners() {
      return primaryOwners;
   }

   @ProtoField(3)
   MarshallableMap<Address, Float> capacityFactors() {
      return MarshallableMap.create(capacityFactors);
   }

   @ProtoField(4)
   MarshallableCollection<Address> getMembersWithoutState() {
      return MarshallableCollection.create(membersWithoutState);
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

      int[] primaryOwners = new int[this.getNumSegments()];
      for (int segmentId = 0; segmentId < primaryOwners.length; segmentId++) {
         Address primaryOwner = this.locatePrimaryOwnerForSegment(segmentId);
         int primaryOwnerIndex = unionMembers.indexOf(primaryOwner);
         primaryOwners[segmentId] = primaryOwnerIndex;
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

   ReplicatedConsistentHash(ScopedPersistentState state) {
      List<Address> members = parseMembers(state, ConsistentHashPersistenceConstants.STATE_MEMBERS,
                        ConsistentHashPersistenceConstants.STATE_MEMBER);
      List<Address> membersWithoutState = parseMembers(state, ConsistentHashPersistenceConstants.STATE_MEMBERS_NO_ENTRIES,
                                                   ConsistentHashPersistenceConstants.STATE_MEMBER_NO_ENTRIES);
      Map<Address, Float> capacityFactors = parseCapacityFactors(state, members);
      int[] primaryOwners = parsePrimaryOwners(state);

      this.members = List.copyOf(members);
      this.membersWithoutState = List.copyOf(membersWithoutState);
      this.membersWithState = computeMembersWithState(members, membersWithoutState);
      this.membersWithStateSet = Set.copyOf(this.membersWithState);
      this.primaryOwners = primaryOwners;
      this.capacityFactors = Map.copyOf(capacityFactors);
      this.segments = IntSets.immutableRangeSet(this.primaryOwners.length);
   }

   private static List<Address> parseMembers(ScopedPersistentState state, String numMembersPropertyName,
                                             String memberPropertyFormat) {
      String property = state.getProperty(numMembersPropertyName);
      if (property == null) {
          return Collections.emptyList();
      }
      int numMembers = Integer.parseInt(property);
      List<Address> members = new ArrayList<>(numMembers);
      for (int i = 0; i < numMembers; i++) {
         PersistentUUID uuid = PersistentUUID.fromString(state.getProperty(String.format(memberPropertyFormat, i)));
         members.add(uuid);
      }
      return members;
   }

   private static Map<Address, Float> parseCapacityFactors(ScopedPersistentState state,
                                                           List<Address> members) {
      String numCapacityFactorsString = state.getProperty(STATE_CAPACITY_FACTORS);
      if (numCapacityFactorsString == null) {
         // Cache state version 11 did not have capacity factors
         Map<Address, Float> map = new HashMap<>();
         for (Address a : members) {
            map.put(a, 1f);
         }
         return map;
      }

      int numCapacityFactors = Integer.parseInt(numCapacityFactorsString);
      Map<Address, Float> capacityFactors = new HashMap<>(numCapacityFactors * 2);
      for (int i = 0; i < numCapacityFactors; i++) {
         float capacityFactor = Float.parseFloat(state.getProperty(String.format(STATE_CAPACITY_FACTOR, i)));
         capacityFactors.put(members.get(i), capacityFactor);
      }
      return capacityFactors;
   }

   private static int[] parsePrimaryOwners(ScopedPersistentState state) {
      int numPrimaryOwners = state.getIntProperty(STATE_PRIMARY_OWNERS_COUNT);
      int[] primaryOwners = new int[numPrimaryOwners];
      for (int i = 0; i < numPrimaryOwners; i++) {
         primaryOwners[i] = state.getIntProperty(String.format(STATE_PRIMARY_OWNERS, i));
      }
      return primaryOwners;
   }

   @Override
   public int getNumSegments() {
      return primaryOwners.length;
   }

   public int getNumOwners() {
      return membersWithState.size();
   }

   @Override
   public List<Address> getMembers() {
      return members;
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
      return members.get(primaryOwners[segmentId]);
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
      IntSet primarySegments = IntSets.mutableEmptySet(primaryOwners.length);
      for (int i = 0; i < primaryOwners.length; ++i) {
         if (primaryOwners[i] == index) {
            primarySegments.set(i);
         }
      }
      return primarySegments;
   }

   @Override
   public String getRoutingTableAsString() {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < primaryOwners.length; i++) {
         if (i > 0) {
            sb.append(", ");
         }
         sb.append(i).append(": ").append(primaryOwners[i]);
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

   public void toScopedState(ScopedPersistentState state) {
      state.setProperty(ConsistentHashPersistenceConstants.STATE_CONSISTENT_HASH, this.getClass().getName());
      state.setProperty(ConsistentHashPersistenceConstants.STATE_MEMBERS, Integer.toString(members.size()));
      for (int i = 0; i < members.size(); i++) {
         state.setProperty(String.format(ConsistentHashPersistenceConstants.STATE_MEMBER, i),
                           members.get(i).toString());
      }
      state.setProperty(ConsistentHashPersistenceConstants.STATE_MEMBERS_NO_ENTRIES, Integer.toString(membersWithoutState.size()));
      for (int i = 0; i < membersWithoutState.size(); i++) {
         state.setProperty(String.format(ConsistentHashPersistenceConstants.STATE_MEMBER_NO_ENTRIES, i),
                           membersWithoutState.get(i).toString());
      }
      state.setProperty(STATE_CAPACITY_FACTORS, Integer.toString(capacityFactors.size()));
      for (int i = 0; i < members.size(); i++) {
         state.setProperty(String.format(STATE_CAPACITY_FACTOR, i),
                           capacityFactors.get(members.get(i)).toString());
      }
      state.setProperty(STATE_PRIMARY_OWNERS_COUNT, Integer.toString(primaryOwners.length));
      for (int i = 0; i < primaryOwners.length; i++) {
         state.setProperty(String.format(STATE_PRIMARY_OWNERS, i), Integer.toString(primaryOwners[i]));
      }
   }

   @Override
   public ConsistentHash remapAddresses(UnaryOperator<Address> remapper) {
      List<Address> remappedMembers = new ArrayList<>(members.size());
      for (Address member : members) {
         Address a = remapper.apply(member);
         if (a == null) {
            return null;
         }
         remappedMembers.add(a);
      }
      List<Address> remappedMembersWithoutState = new ArrayList<>(membersWithoutState.size());
      for (Address member : membersWithoutState) {
         Address a = remapper.apply(member);
         if (a == null) {
            return null;
         }
         remappedMembersWithoutState.add(a);
      }
      Map<Address, Float> remappedCapacityFactors = null;
      if (capacityFactors != null) {
         remappedCapacityFactors = new HashMap<>(members.size());
         for (Address member : members) {
            remappedCapacityFactors.put(remapper.apply(member), capacityFactors.get(member));
         }
      }
      return new ReplicatedConsistentHash(remappedMembers, remappedCapacityFactors, remappedMembersWithoutState, primaryOwners);
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
      sb.append("ns = ").append(primaryOwners.length);
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
      result = prime * result + Arrays.hashCode(primaryOwners);
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
      if (!Arrays.equals(primaryOwners, other.primaryOwners))
         return false;
      return true;
   }
}
