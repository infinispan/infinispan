package org.infinispan.distribution.ch.impl;

import net.jcip.annotations.Immutable;
import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.marshall.InstanceReusingAdvancedExternalizer;
import org.infinispan.commons.util.Immutables;
import org.infinispan.commons.util.Util;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.Address;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Default {@link ConsistentHash} implementation. This object is immutable.
 *
 * @author Dan Berindei
 * @author anistor@redhat.com
 * @since 5.2
 */
@Immutable
public class DefaultConsistentHash implements ConsistentHash {

   private final Hash hashFunction;
   private final int numOwners;

   /**
    * The membership of the cache topology that uses this CH.
    */
   private final List<Address> members;
   private final float[] capacityFactors;

   /**
    * The routing table.
    */
   private final List<Address>[] segmentOwners;
   private final int segmentSize;

   public DefaultConsistentHash(Hash hashFunction, int numOwners, int numSegments, List<Address> members,
                                Map<Address, Float> capacityFactors, List<Address>[] segmentOwners) {
      if (numSegments < 1)
         throw new IllegalArgumentException("The number of segments must be strictly positive");
      if (numOwners < 1)
         throw new IllegalArgumentException("The number of owners must be strictly positive");

      this.numOwners = numOwners;
      this.hashFunction = hashFunction;
      this.members = new ArrayList<Address>(members);
      if (capacityFactors == null) {
         this.capacityFactors = null;
      } else {
         this.capacityFactors = new float[members.size()];
         for (int i = 0; i < this.capacityFactors.length; i++) {
            this.capacityFactors[i] = capacityFactors.get(members.get(i));
         }
      }
      this.segmentOwners = new List[numSegments];
      for (int i = 0; i < numSegments; i++) {
         if (segmentOwners[i] == null || segmentOwners[i].isEmpty()) {
            throw new IllegalArgumentException("Segment owner list cannot be null or empty");
         }
         this.segmentOwners[i] = Immutables.immutableListCopy(segmentOwners[i]);
      }
      this.segmentSize = Util.getSegmentSize(numSegments);
   }

   // Only used by the externalizer, so we can skip copying collections
   private DefaultConsistentHash(Hash hashFunction, int numOwners, int numSegments, List<Address> members,
         float[] capacityFactors, List<Address>[] segmentOwners) {
      if (numSegments < 1)
         throw new IllegalArgumentException("The number of segments must be strictly positive");
      if (numOwners < 1)
         throw new IllegalArgumentException("The number of owners must be strictly positive");

      this.numOwners = numOwners;
      this.hashFunction = hashFunction;
      this.members = members;
      this.capacityFactors = capacityFactors;
      for (int i = 0; i < numSegments; i++) {
         if (segmentOwners[i] == null || segmentOwners[i].size() == 0) {
            throw new IllegalArgumentException("Segment owner list cannot be null or empty");
         }
      }
      this.segmentOwners = segmentOwners;
      this.segmentSize = Util.getSegmentSize(numSegments);
   }

   @Override
   public Hash getHashFunction() {
      return hashFunction;
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

      Set<Integer> segments = new HashSet<Integer>();
      for (int segment = 0; segment < segmentOwners.length; segment++) {
         if (segmentOwners[segment].contains(owner)) {
            segments.add(segment);
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

      Set<Integer> segments = new HashSet<Integer>();
      for (int segment = 0; segment < segmentOwners.length; segment++) {
         if (owner.equals(segmentOwners[segment].get(0))) {
            segments.add(segment);
         }
      }
      return segments;
   }

   @Override
   public int getSegment(Object key) {
      // The result must always be positive, so we make sure the dividend is positive first
      return getNormalizedHash(key) / segmentSize;
   }

   public int getNormalizedHash(Object key) {
      return hashFunction.hash(key) & Integer.MAX_VALUE;
   }

   public List<Integer> getSegmentEndHashes() {
      int numSegments = segmentOwners.length;
      List<Integer> hashes = new ArrayList<Integer>(numSegments);
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
   public List<Address> getMembers() {
      return members;
   }

   @Override
   public int getNumOwners() {
      return numOwners;
   }

   @Override
   public Address locatePrimaryOwner(Object key) {
      return locatePrimaryOwnerForSegment(getSegment(key));
   }

   @Override
   public List<Address> locateOwners(Object key) {
      return locateOwnersForSegment(getSegment(key));
   }

   @Override
   public Set<Address> locateAllOwners(Collection<Object> keys) {
      // Use a HashSet assuming most of the time the number of keys is small.
      HashSet<Integer> segments = new HashSet<Integer>();
      for (Object key : keys) {
         segments.add(getSegment(key));
      }
      HashSet<Address> ownersUnion = new HashSet<Address>();
      for (Integer segment : segments) {
         ownersUnion.addAll(segmentOwners[segment]);
      }
      return ownersUnion;
   }

   @Override
   public boolean isKeyLocalToNode(Address nodeAddress, Object key) {
      int segment = getSegment(key);
      for (Address a : segmentOwners[segment]) {
         if (a.equals(nodeAddress))
            return true;
      }
      return false;
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
      for (int i = 0; i < segmentOwners.length; i++) {
         if (i > 0) {
            sb.append(", ");
         }
         sb.append(i).append(":");
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
      if (!hashFunction.equals(dch2.getHashFunction())) {
         throw new IllegalArgumentException("The consistent hash objects must have the same hash function");
      }
      int numSegments = getNumSegments();
      if (numSegments != dch2.getNumSegments()) {
         throw new IllegalArgumentException("The consistent hash objects must have the same number of segments");
      }
      if (numOwners != dch2.getNumOwners()) {
         throw new IllegalArgumentException("The consistent hash objects must have the same number of owners");
      }

      List<Address> unionMembers = new ArrayList<Address>(this.members);
      mergeLists(unionMembers, dch2.getMembers());

      List<Address>[] unionSegmentOwners = new List[numSegments];
      for (int i = 0; i < numSegments; i++) {
         unionSegmentOwners[i] = new ArrayList<Address>(locateOwnersForSegment(i));
         mergeLists(unionSegmentOwners[i], dch2.locateOwnersForSegment(i));
      }

      Map<Address, Float> unionCapacityFactors = null;
      if (this.capacityFactors != null || dch2.capacityFactors != null) {
         unionCapacityFactors = new HashMap<Address, Float>();
         if (this.capacityFactors != null) {
            unionCapacityFactors.putAll(this.getCapacityFactors());
         } else {
            for (Address node : this.members) {
               unionCapacityFactors.put(node, 1.0f);
            }
         }
         if (dch2.capacityFactors != null) {
            unionCapacityFactors.putAll(dch2.getCapacityFactors());
         } else {
            for (Address node : dch2.members) {
               unionCapacityFactors.put(node, 1.0f);
            }
         }
      }
      return new DefaultConsistentHash(hashFunction, numOwners, numSegments, unionMembers, unionCapacityFactors, unionSegmentOwners);
   }

   /**
    * Adds all elements from <code>src</code> list that do not already exist in <code>dest</code> list to the latter.
    *
    * @param dest List where elements are added
    * @param src List of elements to add - this is never modified
    */
   private void mergeLists(List<Address> dest, List<Address> src) {
      for (Address node : src) {
         if (!dest.contains(node)) {
            dest.add(node);
         }
      }
   }

   public Map<Address, Float> getCapacityFactors() {
      if (capacityFactors == null)
         return null;

      Map<Address, Float> capacityFactorsMap = new HashMap<>(members.size());
      for (int i = 0; i < members.size(); i++) {
         capacityFactorsMap.put(members.get(i), capacityFactors[i]);
      }
      return capacityFactorsMap;
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
            for (int j = 0; j < owners.size(); j++) {
               output.writeInt(memberIndexes.get(owners.get(j)));
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
         return Collections.<Class<? extends DefaultConsistentHash>>singleton(DefaultConsistentHash.class);
      }
   }
}
