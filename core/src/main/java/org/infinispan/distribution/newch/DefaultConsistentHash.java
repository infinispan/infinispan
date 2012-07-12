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

package org.infinispan.distribution.newch;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

import org.infinispan.commons.hash.Hash;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;

/**
 * Default {@link ConsistentHash} implementation. This object is immutable.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class DefaultConsistentHash implements AdvancedConsistentHash {

   private final Hash hashFunction;
   private final int numSegments;
   private final int numOwners;
   private final List<Address> members;
   private final Address[][] segmentOwners;

   public DefaultConsistentHash(Hash hashFunction, int numSegments, int numOwners, List<Address> members,
                                List<Address>[] segmentOwners) {
      if (numSegments <= 1)
         throw new IllegalArgumentException("The number of segments must be strictly positive");
      if (numOwners <= 1)
         throw new IllegalArgumentException("The number of owners must be strictly positive");

      this.numSegments = numSegments;
      this.numOwners = numOwners;
      this.hashFunction = hashFunction;
      this.members = new ArrayList<Address>(members);
      this.segmentOwners = new Address[numSegments][];
      for (int i = 0; i < numSegments; i++) {
         if (segmentOwners[i] == null || segmentOwners[i].isEmpty()) {
            throw new IllegalArgumentException("Segment owner list cannot be null or empty");
         }
         this.segmentOwners[i] = segmentOwners[i].toArray(new Address[segmentOwners[i].size()]);
      }
   }

   public Hash getHashFunction() {
      return hashFunction;
   }

   @Override
   public int getNumSegments() {
      return numSegments;
   }

   @Override
   public int getSegment(Object key) {
      return Math.abs(hashFunction.hash(key) % numSegments);
   }

   @Override
   public List<Address> locateOwnersForSegment(int segmentId) {
      // TODO Optimize away the new ArrayList, write instead an array-backed readonly list
      return new ArrayList<Address>(Arrays.asList(segmentOwners[segmentId]));
   }

   @Override
   public Address locatePrimaryOwnerForSegment(int segmentId) {
      return segmentOwners[segmentId][0];
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
      HashSet<Address> owners = new HashSet<Address>();
      for (Integer segment : segments) {
         Collections.addAll(owners, segmentOwners[segment]);
      }
      return owners;
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
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DefaultConsistentHash that = (DefaultConsistentHash) o;

      if (numOwners != that.numOwners) return false;
      if (numSegments != that.numSegments) return false;
      if (!hashFunction.equals(that.hashFunction)) return false;
      if (!members.equals(that.members)) return false;
      for (int i = 0; i < numSegments; i++) {
         if (!Arrays.equals(segmentOwners[i], that.segmentOwners[i]))
            return false;
      }

      return true;
   }

   @Override
   public String toString() {
      return "DefaultConsistentHash{" +
            "members=" + members +
            ", numOwners=" + numOwners +
            ", numSegments=" + numSegments +
            '}';
   }

   public static class Externalizer extends AbstractExternalizer<DefaultConsistentHash> {

      @Override
      public void writeObject(ObjectOutput output, DefaultConsistentHash ch) throws IOException {
         output.writeInt(ch.numSegments);
         output.writeInt(ch.numOwners);
         output.writeObject(ch.members);
         output.writeObject(ch.hashFunction);
         output.writeObject(ch.segmentOwners);
      }

      @Override
      public DefaultConsistentHash readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         int numSegments = unmarshaller.readInt();
         int numOwners = unmarshaller.readInt();
         List<Address> members = (List<Address>) unmarshaller.readObject();
         Hash hash = (Hash) unmarshaller.readObject();
         Address[][] segmentOwners = (Address[][]) unmarshaller.readObject();

         List<Address>[] segmentOwnerList = new List[segmentOwners.length];
         for (int i = 0; i < segmentOwners.length; i++) {
            segmentOwnerList[i] = Arrays.asList(segmentOwners[i]);
         }
         return new DefaultConsistentHash(hash, numSegments, numOwners, members, segmentOwnerList);
      }

      @Override
      public Integer getId() {
         return Ids.DEFAULT_CONSISTENT_HASH;
      }

      @Override
      public Set<Class<? extends DefaultConsistentHash>> getTypeClasses() {
         return Util.<Class<? extends DefaultConsistentHash>>asSet(DefaultConsistentHash.class);
      }
   }
}
