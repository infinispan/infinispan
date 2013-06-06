/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.distribution.ch;

import org.infinispan.commons.hash.Hash;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
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
      StringBuilder sb = new StringBuilder("ReplicatedConsistentHash{");
      sb.append("members=").append(members);
      sb.append(", numSegments=").append(primaryOwners.length);
      sb.append(", primaryOwners=").append(Arrays.toString(primaryOwners));
      sb.append('}');
      return sb.toString();
   }

   public static class Externalizer extends AbstractExternalizer<ReplicatedConsistentHash> {

      @Override
      public void writeObject(ObjectOutput output, ReplicatedConsistentHash ch) throws IOException {
         output.writeObject(ch.hashFunction);
         output.writeObject(ch.members);
         output.writeObject(ch.primaryOwners);
      }

      @Override
      @SuppressWarnings("unchecked")
      public ReplicatedConsistentHash readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
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
