package org.infinispan.distribution.ch.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.marshall.InstanceReusingAdvancedExternalizer;
import org.infinispan.commons.util.Immutables;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.Address;
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
public class DefaultConsistentHash extends AbstractConsistentHash {
   // State constants
   private static final String STATE_NUM_OWNERS = "numOwners";
   private static final String STATE_SEGMENT_OWNER = "segmentOwner.%d.%d";
   private static final String STATE_SEGMENT_OWNERS = "segmentOwners";
   private static final String STATE_SEGMENT_OWNER_COUNT = "segmentOwner.%d.num";

   private final int numOwners;

   /**
    * The routing table.
    */
   private final List<Address>[] segmentOwners;

   public DefaultConsistentHash(Hash hashFunction, int numOwners, int numSegments, List<Address> members,
                                Map<Address, Float> capacityFactors, List<Address>[] segmentOwners) {
      super(hashFunction, numSegments, members, capacityFactors);
      if (numOwners < 1)
         throw new IllegalArgumentException("The number of owners must be strictly positive");

      this.numOwners = numOwners;
      this.segmentOwners = new List[numSegments];
      for (int i = 0; i < numSegments; i++) {
         if (segmentOwners[i] == null || segmentOwners[i].isEmpty()) {
            throw new IllegalArgumentException("Segment owner list cannot be null or empty");
         }
         this.segmentOwners[i] = Immutables.immutableListCopy(segmentOwners[i]);
      }
   }

   // Only used by the externalizer, so we can skip copying collections
   private DefaultConsistentHash(Hash hashFunction, int numOwners, int numSegments, List<Address> members,
         float[] capacityFactors, List<Address>[] segmentOwners) {
      super(hashFunction, numSegments, members, capacityFactors);
      if (numOwners < 1)
         throw new IllegalArgumentException("The number of owners must be strictly positive");

      this.numOwners = numOwners;
      for (int i = 0; i < numSegments; i++) {
         if (segmentOwners[i] == null || segmentOwners[i].size() == 0) {
            throw new IllegalArgumentException("Segment owner list cannot be null or empty");
         }
      }
      this.segmentOwners = segmentOwners;
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
         throw new IllegalArgumentException("Node " + owner + " is not a member");
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
         throw new IllegalArgumentException("Node " + owner + " is not a member");
      }

      IntSet segments = IntSets.mutableEmptySet(segmentOwners.length);
      for (int segment = 0; segment < segmentOwners.length; segment++) {
         if (owner.equals(segmentOwners[segment].get(0))) {
            segments.set(segment);
         }
      }
      return segments;
   }

   /**
    * @deprecated Since 8.2, use {@link HashFunctionPartitioner#getSegmentEndHashes()} instead.
    */
   @Deprecated
   public List<Integer> getSegmentEndHashes() {
      int numSegments = segmentOwners.length;
      List<Integer> hashes = new ArrayList<>(numSegments);
      for (int i = 0; i < numSegments; i++) {
         hashes.add(((i + 1) % numSegments) * segmentSize);
      }
      return hashes;
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
   public int getNumOwners() {
      return numOwners;
   }

   @Override
   public boolean isSegmentLocalToNode(Address nodeAddress, int segmentId) {
      return segmentOwners[segmentId].contains(nodeAddress);
   }

   @Override
   public int hashCode() {
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
      if (!hashFunction.equals(that.hashFunction)) return false;
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
      for (Address a : members) {
         if (sb.length() > 0) {
            sb.append("\n  ");
         }
         Set<Integer> primarySegments = getPrimarySegmentsForOwner(a);
         sb.append(a).append(" primary: ").append(primarySegments);
         Set<Integer> backupSegments = getSegmentsForOwner(a);
         backupSegments.removeAll(primarySegments);
         sb.append(", backup: ").append(backupSegments);
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
      return new DefaultConsistentHash(hashFunction, numOwners, unionSegmentOwners.length, unionMembers, unionCapacityFactors, unionSegmentOwners);
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
      List<Address> remappedMembers = remapMembers(remapper);
      if (remappedMembers == null) return null;
      Map<Address, Float> remappedCapacityFactors = remapCapacityFactors(remapper);
      List<Address>[] remappedSegmentOwners = new List[segmentOwners.length];
      for(int i=0; i < segmentOwners.length; i++) {
         List<Address> remappedOwners = new ArrayList<>(segmentOwners[i].size());
         for (Address address : segmentOwners[i]) {
            remappedOwners.add(remapper.apply(address));
         }
         remappedSegmentOwners[i] = remappedOwners;
      }

      return new DefaultConsistentHash(this.hashFunction, this.numOwners, this.segmentOwners.length, remappedMembers,
            remappedCapacityFactors, remappedSegmentOwners);
   }

   public static class Externalizer extends InstanceReusingAdvancedExternalizer<DefaultConsistentHash> {

      @Override
      public void doWriteObject(ObjectOutput output, DefaultConsistentHash ch) throws IOException {
         output.writeInt(ch.segmentOwners.length);
         output.writeInt(ch.numOwners);
         output.writeObject(ch.members);
         output.writeObject(ch.capacityFactors);
         output.writeObject(ch.hashFunction);

         // Avoid computing the identityHashCode for every ImmutableListCopy/Address
         HashMap<Address, Integer> memberIndexes = getMemberIndexMap(ch.members);
         for (int i = 0; i < ch.segmentOwners.length; i++) {
            List<Address> owners = ch.segmentOwners[i];
            output.writeInt(owners.size());
            for (Address owner : owners) {
               output.writeInt(memberIndexes.get(owner));
            }
         }
      }

      @Override
      @SuppressWarnings("unchecked")
      public DefaultConsistentHash doReadObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         int numSegments = unmarshaller.readInt();
         int numOwners = unmarshaller.readInt();
         List<Address> members = (List<Address>) unmarshaller.readObject();
         float[] capacityFactors = (float[]) unmarshaller.readObject();
         Hash hash = (Hash) unmarshaller.readObject();

         List<Address>[] segmentOwners = new List[numSegments];
         for (int i = 0; i < numSegments; i++) {
            int size = unmarshaller.readInt();
            Address[] owners = new Address[size];
            for (int j = 0; j < size; j++) {
               int ownerIndex = unmarshaller.readInt();
               owners[j] = members.get(ownerIndex);
            }
            segmentOwners[i] = Immutables.immutableListWrap(owners);
         }

         return new DefaultConsistentHash(hash, numOwners, numSegments, members, capacityFactors, segmentOwners);
      }

      private HashMap<Address, Integer> getMemberIndexMap(List<Address> members) {
         HashMap<Address, Integer> memberIndexes = new HashMap<>(members.size());
         for (int i = 0; i < members.size(); i++) {
            memberIndexes.put(members.get(i), i);
         }
         return memberIndexes;
      }

      @Override
      public Integer getId() {
         return Ids.DEFAULT_CONSISTENT_HASH;
      }

      @Override
      public Set<Class<? extends DefaultConsistentHash>> getTypeClasses() {
         return Collections.singleton(DefaultConsistentHash.class);
      }
   }
}
