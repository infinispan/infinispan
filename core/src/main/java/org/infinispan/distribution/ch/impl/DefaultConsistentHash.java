package org.infinispan.distribution.ch.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.Immutables;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.topology.PersistentUUID;

import net.jcip.annotations.Immutable;

/**
 * Default {@link ConsistentHash} implementation. This object is immutable.
 *
 * Every segment must have a primary owner.
 *
 * @author Dan Berindei
 * @author anistor@redhat.com
 * @since 5.2
 */
@Immutable
@ProtoTypeId(ProtoStreamTypeIds.DEFAULT_CONSISTENT_HASH)
public class DefaultConsistentHash extends AbstractConsistentHash {
   // State constants
   private static final String STATE_NUM_OWNERS = "numOwners";
   private static final String STATE_SEGMENT_OWNER = "segmentOwner.%d.%d";
   private static final String STATE_SEGMENT_OWNERS = "segmentOwners";
   private static final String STATE_SEGMENT_OWNER_COUNT = "segmentOwner.%d.num";

   private final int numOwners;
   private transient final int hashCode;

   /**
    * The routing table.
    */
   private final List<Address>[] segmentOwners;

   public DefaultConsistentHash(int numOwners, int numSegments, List<Address> members,
                                Map<Address, Float> capacityFactors, List<Address>[] segmentOwners) {
      super(numSegments, members, capacityFactors);
      if (numOwners < 1)
         throw new IllegalArgumentException("The number of owners must be strictly positive");

      this.numOwners = numOwners;
      this.segmentOwners = new List[numSegments];
      for (int s = 0; s < numSegments; s++) {
         if (segmentOwners[s] == null || segmentOwners[s].isEmpty()) {
            throw new IllegalArgumentException("Segment owner list cannot be null or empty");
         }
         this.segmentOwners[s] = List.copyOf(segmentOwners[s]);
      }
      this.hashCode = hashCodeInternal();
   }

   DefaultConsistentHash(ScopedPersistentState state) {
      super(state);
      this.numOwners = Integer.parseInt(state.getProperty(STATE_NUM_OWNERS));
      int numSegments = parseNumSegments(state);
      this.segmentOwners = new List[numSegments];
      for (int i = 0; i < segmentOwners.length; i++) {
         int segmentOwnerCount = Integer.parseInt(state.getProperty(String.format(STATE_SEGMENT_OWNER_COUNT, i)));
         segmentOwners[i] = new ArrayList<>();
         for (int j = 0; j < segmentOwnerCount; j++) {
            PersistentUUID uuid = PersistentUUID.fromString(state.getProperty(String.format(STATE_SEGMENT_OWNER, i, j)));
            segmentOwners[i].add(uuid);
         }
      }
      this.hashCode = hashCodeInternal();
   }

   @ProtoFactory
   DefaultConsistentHash(List<JGroupsAddress> jGroupsMembers, List<Float> capacityFactorsList, int numOwners, List<Integer> segmentOwners) {
      super(segmentOwners.size(), (List<Address>)(List<?>) jGroupsMembers, capacityFactorsList);
      if (numOwners < 1)
         throw new IllegalArgumentException("The number of owners must be strictly positive");

      this.numOwners = numOwners;

      int segmentOwnersLength = segmentOwners.get(0);
      this.segmentOwners = new List[segmentOwnersLength];

      int idx = 0;
      int marshalledArrIdx = 1;
      while (marshalledArrIdx < segmentOwners.size()) {
         int size = segmentOwners.get(marshalledArrIdx++);
         Address[] owners = new Address[size];
         for (int j = 0; j < size; j++) {
            int ownerIndex = segmentOwners.get(marshalledArrIdx++);
            owners[j] = members.get(ownerIndex);
         }
         this.segmentOwners[idx++] = Immutables.immutableListWrap(owners);
      }
      for (int i = 0; i < segmentOwnersLength; i++) {
         if (this.segmentOwners[i] == null || this.segmentOwners[i].isEmpty()) {
            throw new IllegalArgumentException("Segment owner list cannot be null or empty");
         }
      }
      this.hashCode = hashCodeInternal();
   }

   @ProtoField(1)
   List<JGroupsAddress> getJGroupsMembers() {
      return (List<JGroupsAddress>)(List<?>) members;
   }

   @ProtoField(number = 2, name = "capacityFactors")
   List<Float> getCapacityFactorsList() {
      return capacityFactors;
   }

   @ProtoField(3)
   public int getNumOwners() {
      return numOwners;
   }

   @ProtoField(4)
   List<Integer> getSegmentOwners() {
      // Approximate final size of array
      List<Integer> ownersList = new ArrayList<>((segmentOwners.length + 1) * segmentOwners[0].size() + 1);
      ownersList.add(segmentOwners.length);

      // Avoid computing the identityHashCode for every ImmutableListCopy/Address
      HashMap<Address, Integer> memberIndexes = getMemberIndexMap(members);
      for (List<Address> owners : segmentOwners) {
         ownersList.add(owners.size());
         for (Address owner : owners) {
            ownersList.add(memberIndexes.get(owner));
         }
      }
      return ownersList;
   }

   @Override
   public int getNumSegments() {
      return segmentOwners.length;
   }

   @Override
   public Set<Integer> getSegmentsForOwner(Address owner) {
      if (owner == null) {
         throw new IllegalArgumentException("owner cannot be null");
      }
      if (!members.contains(owner)) {
         return IntSets.immutableEmptySet();
      }

      IntSet segments = IntSets.mutableEmptySet(segmentOwners.length);
      for (int segment = 0; segment < segmentOwners.length; segment++) {
         if (segmentOwners[segment].contains(owner)) {
            segments.set(segment);
         }
      }
      return segments;
   }

   @Override
   public Set<Integer> getPrimarySegmentsForOwner(Address owner) {
      if (owner == null) {
         throw new IllegalArgumentException("owner cannot be null");
      }
      if (!members.contains(owner)) {
         return IntSets.immutableEmptySet();
      }

      IntSet segments = IntSets.mutableEmptySet(segmentOwners.length);
      for (int segment = 0; segment < segmentOwners.length; segment++) {
         if (owner.equals(segmentOwners[segment].get(0))) {
            segments.set(segment);
         }
      }
      return segments;
   }

   @Override
   public List<Address> locateOwnersForSegment(int segmentId) {
      return segmentOwners[segmentId];
   }

   @Override
   public Address locatePrimaryOwnerForSegment(int segmentId) {
      return segmentOwners[segmentId].get(0);
   }

   @Override
   public boolean isSegmentLocalToNode(Address nodeAddress, int segmentId) {
      return segmentOwners[segmentId].contains(nodeAddress);
   }

   @Override
   public int hashCode() {
      return hashCode;
   }

   private int hashCodeInternal() {
      int result = numOwners;
      result = 31 * result + members.hashCode();
      result = 31 * result + Arrays.hashCode(segmentOwners);
      return result;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DefaultConsistentHash that = (DefaultConsistentHash) o;

      if (numOwners != that.numOwners) return false;
      if (segmentOwners.length != that.segmentOwners.length) return false;
      if (!members.equals(that.members)) return false;
      for (int i = 0; i < segmentOwners.length; i++) {
         if (!segmentOwners[i].equals(that.segmentOwners[i]))
            return false;
      }

      return true;
   }

   @Override
   public String toString() {
      OwnershipStatistics stats = new OwnershipStatistics(this, members);
      StringBuilder sb = new StringBuilder("DefaultConsistentHash{");
      sb.append("ns=").append(segmentOwners.length);
      sb.append(", owners = (").append(members.size()).append(")[");
      boolean first = true;
      for (Address a : members) {
         if (first) {
            first = false;
         } else {
            sb.append(", ");
         }
         int primaryOwned = stats.getPrimaryOwned(a);
         int owned = stats.getOwned(a);
         sb.append(a).append(": ").append(primaryOwned).append('+').append(owned - primaryOwned);
      }
      sb.append("]}");
      return sb.toString();
   }

   @Override
   public String getRoutingTableAsString() {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < segmentOwners.length; i++) {
         if (i > 0) {
            sb.append(", ");
         }
         sb.append(i).append(':');
         for (int j = 0; j < segmentOwners[i].size(); j++) {
            sb.append(' ').append(members.indexOf(segmentOwners[i].get(j)));
         }
      }
      return sb.toString();
   }

   /**
    * Merges two consistent hash objects that have the same number of segments, numOwners and hash function.
    * For each segment, the primary owner of the first CH has priority, the other primary owners become backups.
    */
   public DefaultConsistentHash union(DefaultConsistentHash dch2) {
      checkSameHashAndSegments(dch2);
      if (numOwners != dch2.getNumOwners()) {
         throw new IllegalArgumentException("The consistent hash objects must have the same number of owners");
      }

      List<Address> unionMembers = new ArrayList<>(this.members);
      mergeLists(unionMembers, dch2.getMembers());

      List<Address>[] unionSegmentOwners = new List[segmentOwners.length];
      for (int i = 0; i < segmentOwners.length; i++) {
         unionSegmentOwners[i] = new ArrayList<>(locateOwnersForSegment(i));
         mergeLists(unionSegmentOwners[i], dch2.locateOwnersForSegment(i));
      }

      Map<Address, Float> unionCapacityFactors = unionCapacityFactors(dch2);
      return new DefaultConsistentHash(numOwners, unionSegmentOwners.length, unionMembers, unionCapacityFactors, unionSegmentOwners);
   }

   public String prettyPrintOwnership() {
      StringBuilder sb = new StringBuilder();
      for (Address member : getMembers()) {
         sb.append("\n").append(member).append(":");
         for (int segment = 0; segment < segmentOwners.length; segment++) {
            int index = segmentOwners[segment].indexOf(member);
            if (index >= 0) {
               sb.append(' ').append(segment);
               if (index == 0) {
                  sb.append('\'');
               }
            }
         }
      }
      return sb.toString();
   }

   @Override
   public void toScopedState(ScopedPersistentState state) {
      super.toScopedState(state);
      state.setProperty(STATE_NUM_OWNERS, numOwners);
      state.setProperty(STATE_SEGMENT_OWNERS, segmentOwners.length);
      for (int i = 0; i < segmentOwners.length; i++) {
         List<Address> segmentOwnerAddresses = segmentOwners[i];
         state.setProperty(String.format(STATE_SEGMENT_OWNER_COUNT, i), segmentOwnerAddresses.size());
         for(int j = 0; j < segmentOwnerAddresses.size(); j++) {
            state.setProperty(String.format(STATE_SEGMENT_OWNER, i, j),
                  segmentOwnerAddresses.get(j).toString());
         }
      }
   }

   @Override
   public ConsistentHash remapAddresses(UnaryOperator<Address> remapper) {
      List<Address> remappedMembers = remapMembers(remapper, false);
      if (remappedMembers == null) return null;
      // At this point, all members are present in the remapper.
      Map<Address, Float> remappedCapacityFactors = remapCapacityFactors(remapper, false);
      List<Address>[] remappedSegmentOwners = remapSegmentOwners(remapper, false);
      return new DefaultConsistentHash(this.numOwners, this.segmentOwners.length, remappedMembers,
            remappedCapacityFactors, remappedSegmentOwners);
   }

   @Override
   public ConsistentHash remapAddressRemoveMissing(UnaryOperator<Address> remapper) {
      List<Address> remappedMembers = remapMembers(remapper, true);
      if (remappedMembers == null) return null;
      Map<Address, Float> remappedCapacityFactors = remapCapacityFactors(remapper, true);
      List<Address>[] remappedSegmentOwners = remapSegmentOwners(remapper, true);
      return new DefaultConsistentHash(this.numOwners, this.segmentOwners.length, remappedMembers,
            remappedCapacityFactors, remappedSegmentOwners);
   }

   private List<Address>[] remapSegmentOwners(UnaryOperator<Address> remapper, boolean allowMissing) {
      List<Address>[] remappedSegmentOwners = new List[segmentOwners.length];
      for(int i=0; i < segmentOwners.length; i++) {
         List<Address> remappedOwners = new ArrayList<>(segmentOwners[i].size());
         for (Address address : segmentOwners[i]) {
            Address a = remapper.apply(address);
            if (a == null) {
               if (allowMissing) continue;
               return null;
            }
            remappedOwners.add(a);
         }
         remappedSegmentOwners[i] = remappedOwners;
      }
      return remappedSegmentOwners;
   }
}
