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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import org.infinispan.commons.hash.Hash;
import org.infinispan.remoting.transport.Address;

/**
 * A {@link ConsistentHashFactory} implementation that guarantees caches with the same members
 * have the same consistent hash.
 *
 * It has a drawback compared to {@link DefaultConsistentHashFactory}, though: it can potentially
 * move a lot more segments during a rebalance than strictly necessary.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class SyncConsistentHashFactory implements ConsistentHashFactory<DefaultConsistentHash> {
   @Override
   public DefaultConsistentHash create(Hash hashFunction, int numOwners, int numSegments, List<Address> members) {
      int numMembers = members.size();
      int actualNumOwners = Math.min(numOwners, numMembers);
      List<Address> sortedMembers = sort(members);
      int segmentSize = (int)Math.ceil((double)Integer.MAX_VALUE / numSegments);

      // First find a unique "primary segment" for each virtual member
      SortedMap<Integer, Address> primarySegments = new TreeMap<Integer, Address>();

      // Since the number of segments is potentially much larger than the number of members,
      // we need a concept of "virtual nodes" to help split the segments more evenly.
      // However, we don't have a "numVirtualNodes" setting any more, so we try to guess it
      // based on numSegments. This is not perfect because we may end up with too many virtual nodes,
      // but the only downside in that is a little more shuffling when a node joins/leaves.
      int numVirtualNodes = (int)Math.sqrt(numSegments);
      for (int virtualNode = 0; virtualNode < numVirtualNodes; virtualNode++) {
         for (Address member : sortedMembers) {
            // Use multiplication to compensate for addresses having consecutive hashes (like test addresses do)
            int virtualNodeHash = 31 * member.hashCode() + virtualNode;
            int normalizedHash = hashFunction.hash(virtualNodeHash) & Integer.MAX_VALUE;
            int initSegment = normalizedHash / segmentSize;
            for (int i = 0; i < numSegments; i++) {
               int segment = (initSegment + i) % numSegments;
               if (!primarySegments.containsKey(segment)) {
                  primarySegments.put(segment, member);
                  break;
               }
            }
         }
      }

      List<Address>[] segmentOwners = new List[numSegments];
      if (numSegments >= numMembers) {
         // Each member is present at least once in the primary segments map, so we can use that
         // to populate the owner lists. For each segment assign the owners of the next numOwners
         // "primary segments" as owners.
         for (int i = 0; i < numSegments; i++) {
            ArrayList<Address> owners = new ArrayList<Address>(actualNumOwners);
            for (Address a : primarySegments.tailMap(i).values()) {
               if (owners.size() >= actualNumOwners)
                  break;
               if (!owners.contains(a)) {
                  owners.add(a);
               }
            }
            for (Address a : primarySegments.headMap(i).values()) {
               if (owners.size() >= actualNumOwners)
                  break;
               if (!owners.contains(a)) {
                  owners.add(a);
               }
            }
            segmentOwners[i] = owners;
         }
      } else {
         // Too few segments for each member to have one "primary segment",
         // but we may still have enough segments for each member to be a backup owner.

         // Populate the primary owners first - because numSegments < numMembers we're guaranteed to
         // set the primary owner of each segment
         for (Map.Entry<Integer, Address> e : primarySegments.entrySet()) {
            Integer segment = e.getKey();
            Address primaryOwner = e.getValue();
            segmentOwners[segment] = new ArrayList<Address>(actualNumOwners);
            segmentOwners[segment].add(primaryOwner);
         }

         // Continue with the backup owners. Assign each member as owner to one segment,
         // then repeat until each segment has numOwners owners.
         boolean haveEnoughOwners = false;
         while (!haveEnoughOwners) {
            for (Address member : sortedMembers) {
               // Compute an initial segment and iterate backwards to make it more like the other case
               int normalizedHash = hashFunction.hash(member.hashCode()) & Integer.MAX_VALUE;
               int initSegment = normalizedHash / segmentSize;
               for (int i = 0; i < numSegments; i++) {
                  int segment = (numSegments + initSegment - i) % numSegments;
                  List<Address> owners = segmentOwners[segment];
                  if (owners.size() < actualNumOwners && !owners.contains(member)) {
                     owners.add(member);
                     break;
                  }
               }
            }
            haveEnoughOwners = true;
            for (int i = 0; i < numSegments; i++) {
               if (segmentOwners[i].size() < actualNumOwners) {
                  haveEnoughOwners = false;
                  break;
               }
            }
         }
      }
      return new DefaultConsistentHash(hashFunction, numSegments, numOwners, members, segmentOwners);
   }

   @Override
   public DefaultConsistentHash updateMembers(DefaultConsistentHash baseCH, List<Address> newMembers) {
      // the ConsistentHashFactory contract says we should return the same instance if we're not making changes
      if (newMembers.equals(baseCH.getMembers()))
         return baseCH;

      int numSegments = baseCH.getNumSegments();
      int numOwners = baseCH.getNumOwners();

      // we assume leavers are far fewer than members, so it makes sense to check for leavers
      List<Address> leavers = new ArrayList<Address>(baseCH.getMembers());
      leavers.removeAll(newMembers);

      // create a new "balanced" CH in case we need to allocate new owners for segments with 0 owners
      DefaultConsistentHash rebalancedCH = create(baseCH.getHashFunction(), numOwners, numSegments, newMembers);

      // remove leavers
      List<Address>[] newSegmentOwners = new List[numSegments];
      for (int i = 0; i < numSegments; i++) {
         List<Address> owners = new ArrayList<Address>(baseCH.locateOwnersForSegment(i));
         owners.removeAll(leavers);
         if (!owners.isEmpty()) {
            newSegmentOwners[i] = owners;
         } else {
            // this segment has 0 owners, fix it
            newSegmentOwners[i] = rebalancedCH.locateOwnersForSegment(i);
         }
      }

      return new DefaultConsistentHash(baseCH.getHashFunction(), numSegments, numOwners,
            newMembers, newSegmentOwners);
   }

   @Override
   public DefaultConsistentHash rebalance(DefaultConsistentHash baseCH) {
      DefaultConsistentHash rebalancedCH = create(baseCH.getHashFunction(), baseCH.getNumOwners(), baseCH.getNumSegments(), baseCH.getMembers());

      // the ConsistentHashFactory contract says we should return the same instance if we're not making changes
      if (rebalancedCH.equals(baseCH))
         return baseCH;

      return rebalancedCH;
   }

   @Override
   public DefaultConsistentHash union(DefaultConsistentHash ch1, DefaultConsistentHash ch2) {
      return ch1.union(ch2);
   }


   private List<Address> sort(List<Address> members) {
      ArrayList<Address> result = new ArrayList<Address>(members);
      Collections.sort(result);
      return result;
   }
}
