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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import org.infinispan.commons.hash.Hash;
import org.infinispan.remoting.transport.Address;

/**
 * Default {@link NewConsistentHash} implementation.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class NewDefaultConsistentHash implements NewAdvancedConsistentHash {
   private static final Address[] ADDRESS_ARRAY_TEMPLATE = new Address[0];

   private final Hash hashFunction;
   private final int numSegments;
   private final int numOwners;
   private final List<Address> members;
   private final Address[][] ownerLists;

   public NewDefaultConsistentHash(Hash hashFunction, int numSegments, int numOwners, List<Address> members,
                                   List<Address>[] theOwnerLists) {
      this.numSegments = numSegments;
      this.numOwners = numOwners;
      this.hashFunction = hashFunction;
      // assume the user will not modify the collections after passing them to the constructor
      this.members = new ArrayList<Address>(members);
      this.ownerLists = new Address[numSegments][];
      for (int i = 0; i < numSegments; i++) {
         this.ownerLists[i] = theOwnerLists[i].toArray(ADDRESS_ARRAY_TEMPLATE);
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
   public int getHashWheelSegment(Object key) {
      return Math.abs(hashFunction.hash(key) % numSegments);
   }

   @Override
   public List<Address> locateOwnersForSegment(int segmentId) {
      // TODO Optimize away the new ArrayList, write instead an array-backed readonly list
      return new ArrayList<Address>(Arrays.asList(ownerLists[segmentId]));
   }

   @Override
   public Address locatePrimaryOwnerForSegment(int segmentId) {
      return ownerLists[segmentId][0];
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
      return locatePrimaryOwnerForSegment(getHashWheelSegment(key));
   }

   @Override
   public List<Address> locateOwners(Object key) {
      return locateOwnersForSegment(getHashWheelSegment(key));
   }

   @Override
   public Collection<Address> locateAllOwners(Collection<Object> keys) {
      // Use a HashSet assuming most of the time the number of keys is small.
      HashSet<Integer> segments = new HashSet<Integer>(keys.size());
      for (Object key : keys) {
         segments.add(getHashWheelSegment(key));
      }
      HashSet<Address> owners = new HashSet<Address>(segments.size());
      for (Integer segment : segments) {
         owners.addAll(locateOwnersForSegment(segment));
      }
      return owners;
   }

   @Override
   public boolean isKeyLocalToNode(Address nodeAddress, Object key) {
      int segment = getHashWheelSegment(key);
      for (Address a : ownerLists[segment]) {
         if (a.equals(nodeAddress))
            return true;
      }
      return false;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      NewDefaultConsistentHash that = (NewDefaultConsistentHash) o;

      if (numOwners != that.numOwners) return false;
      if (numSegments != that.numSegments) return false;
      if (!hashFunction.equals(that.hashFunction)) return false;
      if (!members.equals(that.members)) return false;
      for (int i = 0; i < numSegments; i++) {
         if (!Arrays.equals(ownerLists[i], that.ownerLists[i]))
            return false;
      }

      return true;
   }

   @Override
   public String toString() {
      return "NewDefaultConsistentHash{" +
            "members=" + members +
            ", numOwners=" + numOwners +
            ", numSegments=" + numSegments +
            '}';
   }
}
