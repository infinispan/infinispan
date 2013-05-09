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

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Factory for ReplicatedConsistentHash.
 *
 * @author Dan Berindei
 * @author anistor@redhat.com
 * @since 5.2
 */
public class ReplicatedConsistentHashFactory implements ConsistentHashFactory<ReplicatedConsistentHash> {

   /**
    * @param hashFunction The hash function to use on top of the keys' own {@code hashCode()} implementation.
    * @param numOwners    this implementation ignores this parameter.
    * @param numSegments  number of hash-space segments.
    * @param members      the list of cache members.
    * @see ConsistentHashFactory#create(org.infinispan.commons.hash.Hash, int, int, java.util.List)
    */
   @Override
   public ReplicatedConsistentHash create(Hash hashFunction, int numOwners, int numSegments, List<Address> members) {
      int[] primaryOwners = new int[numSegments];
      for (int i = 0; i < numSegments; i++) {
         primaryOwners[i] = i % members.size();
      }
      return new ReplicatedConsistentHash(hashFunction, members, primaryOwners);
   }

   @Override
   public ReplicatedConsistentHash updateMembers(ReplicatedConsistentHash baseCH, List<Address> newMembers) {
      if (newMembers.equals(baseCH.getMembers()))
         return baseCH;

      // recompute primary ownership based on the new list of members (removes leavers)
      int numSegments = baseCH.getNumSegments();
      int[] primaryOwners = new int[numSegments];
      int[] nodeUsage = new int[newMembers.size()];
      boolean foundOrphanSegments = false;
      for (int segmentId = 0; segmentId < numSegments; segmentId++) {
         Address primaryOwner = baseCH.locatePrimaryOwnerForSegment(segmentId);
         int primaryOwnerIndex = newMembers.indexOf(primaryOwner);
         primaryOwners[segmentId] = primaryOwnerIndex;
         if (primaryOwnerIndex == -1) {
            foundOrphanSegments = true;
         } else {
            nodeUsage[primaryOwnerIndex]++;
         }
      }

      // ensure leavers are replaced with existing members so no segments are orphan
      if (foundOrphanSegments) {
         for (int i = 0; i < numSegments; i++) {
            if (primaryOwners[i] == -1) {
               int leastUsed = findLeastUsedNode(nodeUsage);
               primaryOwners[i] = leastUsed;
               nodeUsage[leastUsed]++;
            }
         }
      }

      // ensure even spread of ownership
      int minSegmentsPerPrimaryOwner = numSegments / newMembers.size();
      for (int node = 0; node < nodeUsage.length; node++) {
         if (nodeUsage[node] < minSegmentsPerPrimaryOwner) {
            int mostUsed = findMostUsedNode(nodeUsage);
            if (Math.abs(nodeUsage[node] - nodeUsage[mostUsed]) > 1) {
               transferOwnership(mostUsed, node, primaryOwners, nodeUsage);
            }
         }
      }

      return new ReplicatedConsistentHash(baseCH.getHashFunction(), newMembers, primaryOwners);
   }

   private void transferOwnership(int oldOwner, int newOwner, int[] primaryOwners, int[] nodeUsage) {
      for (int segmentId = 0; segmentId < primaryOwners.length; segmentId++) {
         if (primaryOwners[segmentId] == oldOwner) {
            primaryOwners[segmentId] = newOwner;
            nodeUsage[oldOwner]--;
            nodeUsage[newOwner]++;
         }
      }
   }

   private int findLeastUsedNode(int[] nodeUsage) {
      int res = 0;
      for (int node = 1; node < nodeUsage.length; node++) {
         if (nodeUsage[node] < nodeUsage[res]) {
            res = node;
         }
      }
      return res;
   }

   private int findMostUsedNode(int[] nodeUsage) {
      int res = 0;
      for (int node = 1; node < nodeUsage.length; node++) {
         if (nodeUsage[node] > nodeUsage[res]) {
            res = node;
         }
      }
      return res;
   }

   @Override
   public ReplicatedConsistentHash rebalance(ReplicatedConsistentHash baseCH) {
      return baseCH;
   }

   @Override
   public ReplicatedConsistentHash union(ReplicatedConsistentHash ch1, ReplicatedConsistentHash ch2) {
      if (!ch1.getHashFunction().equals(ch2.getHashFunction())) {
         throw new IllegalArgumentException("The consistent hash objects must have the same hash function");
      }
      if (ch1.getNumSegments() != ch2.getNumSegments()) {
         throw new IllegalArgumentException("The consistent hash objects must have the same number of segments");
      }

      List<Address> unionMembers = new ArrayList<Address>(ch1.getMembers());
      for (Address member : ch2.getMembers()) {
         if (!unionMembers.contains(member)) {
            unionMembers.add(member);
         }
      }

      int[] primaryOwners = new int[ch1.getNumSegments()];
      for (int segmentId = 0; segmentId < primaryOwners.length; segmentId++) {
         Address primaryOwner = ch1.locatePrimaryOwnerForSegment(segmentId);
         int primaryOwnerIndex = unionMembers.indexOf(primaryOwner);
         primaryOwners[segmentId] = primaryOwnerIndex;
      }

      return new ReplicatedConsistentHash(ch1.getHashFunction(), unionMembers, primaryOwners);
   }

   public static class Externalizer extends AbstractExternalizer<ReplicatedConsistentHashFactory> {

      @Override
      public void writeObject(ObjectOutput output, ReplicatedConsistentHashFactory chf) {
      }

      @Override
      @SuppressWarnings("unchecked")
      public ReplicatedConsistentHashFactory readObject(ObjectInput unmarshaller) {
         return new ReplicatedConsistentHashFactory();
      }

      @Override
      public Integer getId() {
         return Ids.REPLICATED_CONSISTENT_HASH_FACTORY;
      }

      @Override
      public Set<Class<? extends ReplicatedConsistentHashFactory>> getTypeClasses() {
         return Collections.<Class<? extends ReplicatedConsistentHashFactory>>singleton(ReplicatedConsistentHashFactory.class);
      }
   }
}
