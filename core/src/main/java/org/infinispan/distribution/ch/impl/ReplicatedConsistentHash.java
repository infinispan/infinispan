package org.infinispan.distribution.ch.impl;

import org.infinispan.commons.hash.Hash;
import org.infinispan.commons.marshall.InstanceReusingAdvancedExternalizer;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.Address;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

/**
 * Special implementation of {@link org.infinispan.distribution.ch.ConsistentHash} for replicated caches.
 * The hash-space has several segments owned by all members and the primary ownership of each segment is evenly
 * spread between members.
 *
 * @author Dan Berindei
 * @author anistor@redhat.com
 * @since 5.2
 */
public class ReplicatedConsistentHash implements ConsistentHash {

   private final Hash hashFunction;
   private final int[] primaryOwners;
   private final List<Address> members;
   private final Set<Address> membersSet;
   private final Set<Integer> segments;

   public ReplicatedConsistentHash(Hash hashFunction, List<Address> members, int[] primaryOwners) {
      this.hashFunction = hashFunction;
      this.members = Collections.unmodifiableList(new ArrayList<Address>(members));
      this.membersSet = Collections.unmodifiableSet(new HashSet<Address>(members));
      this.primaryOwners = primaryOwners;
      Set<Integer> segmentIds = new HashSet<Integer>(primaryOwners.length);
      for (int i = 0; i < primaryOwners.length; i++) {
         segmentIds.add(i);
      }
      segments = Collections.unmodifiableSet(segmentIds);
   }

   @Override
   public int getNumSegments() {
      return primaryOwners.length;
   }

   @Override
   public int getNumOwners() {
      return members.size();
   }

   @Override
   public List<Address> getMembers() {
      return members;
   }

   @Override
   public Hash getHashFunction() {
      return hashFunction;
   }

   @Override
   public int getSegment(Object key) {
      // The result must always be positive, so we make sure the dividend is positive first
      return (hashFunction.hash(key) & Integer.MAX_VALUE) % primaryOwners.length;
   }

   @Override
   public List<Address> locateOwnersForSegment(int segmentId) {
      Address primaryOwner = locatePrimaryOwnerForSegment(segmentId);
      List<Address> owners = new ArrayList<Address>(members.size());
      owners.add(primaryOwner);
      for (Address member : members) {
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
      if (!membersSet.contains(owner)) {
         throw new IllegalArgumentException("The node is not a member : " + owner);
      }
      return segments;
   }

   @Override
   public Set<Integer> getPrimarySegmentsForOwner(Address owner) {
      int index = members.indexOf(owner);
      if (index == -1) {
         throw new IllegalArgumentException("The node is not a member : " + owner);
      }
      Set<Integer> primarySegments = new HashSet<Integer>();
      for (int i = 0; i < primaryOwners.length; ++i) {
         if (primaryOwners[i] == index) {
            primarySegments.add(i);
         }
      }
      return primarySegments;
   }

   @Override
   public String getRoutingTableAsString() {
      return Arrays.toString(primaryOwners);
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
      return membersSet;
   }

   @Override
   public boolean isKeyLocalToNode(Address nodeAddress, Object key) {
      return membersSet.contains(nodeAddress);
   }

   @Override
   public String toString() {
      OwnershipStatistics stats = new OwnershipStatistics(this, members);
      StringBuilder sb = new StringBuilder("ReplicatedConsistentHash{");
      sb.append("ns = ").append(segments.size());
      sb.append(", owners = (").append(members.size()).append(")[");
      boolean first = true;
      for (Address a : members) {
         if (first) {
            first = false;
         } else {
            sb.append(", ");
         }
         int primaryOwned = stats.getPrimaryOwned(a);
         sb.append(a).append(": ").append(primaryOwned);
      }
      sb.append("]}");
      return sb.toString();
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((hashFunction == null) ? 0 : hashFunction.hashCode());
      result = prime * result + ((members == null) ? 0 : members.hashCode());
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
      if (hashFunction == null) {
         if (other.hashFunction != null)
            return false;
      } else if (!hashFunction.equals(other.hashFunction))
         return false;
      if (members == null) {
         if (other.members != null)
            return false;
      } else if (!members.equals(other.members))
         return false;
      if (!Arrays.equals(primaryOwners, other.primaryOwners))
         return false;
      return true;
   }



   public static class Externalizer extends InstanceReusingAdvancedExternalizer<ReplicatedConsistentHash> {

      @Override
      public void doWriteObject(ObjectOutput output, ReplicatedConsistentHash ch) throws IOException {
         output.writeObject(ch.hashFunction);
         output.writeObject(ch.members);
         output.writeObject(ch.primaryOwners);
      }

      @Override
      @SuppressWarnings("unchecked")
      public ReplicatedConsistentHash doReadObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         Hash hashFunction = (Hash) unmarshaller.readObject();
         List<Address> members = (List<Address>) unmarshaller.readObject();
         int[] primaryOwners = (int[]) unmarshaller.readObject();
         return new ReplicatedConsistentHash(hashFunction, members, primaryOwners);
      }

      @Override
      public Integer getId() {
         return Ids.REPLICATED_CONSISTENT_HASH;
      }

      @Override
      public Set<Class<? extends ReplicatedConsistentHash>> getTypeClasses() {
         return Collections.<Class<? extends ReplicatedConsistentHash>>singleton(ReplicatedConsistentHash.class);
      }
   }
}
