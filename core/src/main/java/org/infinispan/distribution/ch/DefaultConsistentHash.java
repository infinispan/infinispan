package org.infinispan.distribution.ch;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.jcip.annotations.Immutable;
import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Immutables;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.Address;

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
   private final int numSegments;

   /**
    * The membership of the cache topology that uses this CH.
    */
   private final List<Address> members;
   private Map<Address, Float> capacityFactors;

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
      this.numSegments = numSegments;
      this.hashFunction = hashFunction;
      this.members = new ArrayList<Address>(members);
      this.capacityFactors = capacityFactors != null ? new HashMap<Address, Float>(capacityFactors) : null;
      this.segmentOwners = new List[numSegments];
      for (int i = 0; i < numSegments; i++) {
         if (segmentOwners[i] == null || segmentOwners[i].isEmpty()) {
            throw new IllegalArgumentException("Segment owner list cannot be null or empty");
         }
         this.segmentOwners[i] = Immutables.immutableListCopy(segmentOwners[i]);
      }
      this.segmentSize = Util.getSegmentSize(numSegments);
   }

   @Override
   public Hash getHashFunction() {
      return hashFunction;
   }

   @Override
   public int getNumSegments() {
      return numSegments;
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
   public int getSegment(Object key) {
      // The result must always be positive, so we make sure the dividend is positive first
      return getNormalizedHash(key) / segmentSize;
   }

   public int getNormalizedHash(Object key) {
      return hashFunction.hash(key) & Integer.MAX_VALUE;
   }

   public List<Integer> getSegmentEndHashes() {
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
      int result = hashFunction.hashCode();
      result = 31 * result + numOwners;
      result = 31 * result + numSegments;
      result = 31 * result + members.hashCode();
      return result;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DefaultConsistentHash that = (DefaultConsistentHash) o;

      if (numOwners != that.numOwners) return false;
      if (numSegments != that.numSegments) return false;
      if (!hashFunction.equals(that.hashFunction)) return false;
      if (!members.equals(that.members)) return false;
      for (int i = 0; i < numSegments; i++) {
         if (!segmentOwners[i].equals(that.segmentOwners[i]))
            return false;
      }

      return true;
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder("DefaultConsistentHash{");
      sb.append("numSegments=").append(numSegments);
      sb.append(", numOwners=").append(numOwners);
      sb.append(", members=").append(members);
      sb.append('}');
      return sb.toString();
   }

   @Override
   public String getRoutingTableAsString() {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < numSegments; i++) {
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
            unionCapacityFactors.putAll(this.capacityFactors);
         } else {
            for (Address node : this.members) {
               unionCapacityFactors.put(node, 1.0f);
            }
         }
         if (dch2.capacityFactors != null) {
            unionCapacityFactors.putAll(dch2.capacityFactors);
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
      return capacityFactors;
   }

   public static class Externalizer extends AbstractExternalizer<DefaultConsistentHash> {

      @Override
      public void writeObject(ObjectOutput output, DefaultConsistentHash ch) throws IOException {
         output.writeInt(ch.numSegments);
         output.writeInt(ch.numOwners);
         output.writeObject(ch.members);
         output.writeObject(ch.capacityFactors);
         output.writeObject(ch.hashFunction);
         output.writeObject(ch.segmentOwners);
      }

      @Override
      @SuppressWarnings("unchecked")
      public DefaultConsistentHash readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         int numSegments = unmarshaller.readInt();
         int numOwners = unmarshaller.readInt();
         List<Address> members = (List<Address>) unmarshaller.readObject();
         Map<Address, Float> capacityFactors = (Map<Address, Float>)unmarshaller.readObject();
         Hash hash = (Hash) unmarshaller.readObject();
         List<Address>[] owners = (List<Address>[]) unmarshaller.readObject();

         return new DefaultConsistentHash(hash, numOwners, numSegments, members, capacityFactors, owners);
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
