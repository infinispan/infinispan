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

import java.util.*;

import org.infinispan.commons.hash.Hash;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Creates new instances of {@link DefaultConsistentHash}.
 *
 * @author Dan Berindei
 * @author anistor@redhat.com
 * @since 5.2
 */
public class DefaultConsistentHashFactory implements ConsistentHashFactory<DefaultConsistentHash> {

   private static final Log log = LogFactory.getLog(DefaultConsistentHashFactory.class);

   @Override
   public DefaultConsistentHash create(Hash hashFunction, int numOwners, int numSegments, List<Address> members) {
      if (numOwners <= 0)
         throw new IllegalArgumentException("The number of owners should be greater than 0");

      int actualNumOwners = Math.min(numOwners, members.size());

      // initialize the owner lists with the primary owners
      List<Address>[] segmentOwners = new List[numSegments];
      for (int i = 0; i < numSegments; i++) {
         segmentOwners[i] = new ArrayList<Address>(actualNumOwners);
         segmentOwners[i].add(members.get(i % members.size()));
      }
      DefaultConsistentHash baseCH = new DefaultConsistentHash(hashFunction, numSegments, numOwners, members, segmentOwners);

      // use the CH update algorithm to get an even spread
      // (round robin didn't work properly with numSegments = 8, numOwners = 2, and numNodes = 5)
      return actualNumOwners > 1 ? rebalance(baseCH) : baseCH;
   }

   /**
    * Leavers are removed and segments without owners are assigned new owners. Joiners might get some of the un-owned
    * segments but otherwise they are not taken into account (that should happen during a rebalance).
    *
    * @param baseCH An existing consistent hash instance, should not be {@code null}
    * @param newMembers A list of addresses representing the new cache members.
    * @return
    */
   @Override
   public DefaultConsistentHash updateMembers(DefaultConsistentHash baseCH, List<Address> newMembers) {
      if (newMembers.equals(baseCH.getMembers()))
         return baseCH;

      return removeLeavers(baseCH, newMembers);
   }

   @Override
   public DefaultConsistentHash rebalance(DefaultConsistentHash baseCH) {

      // The goal of this phase is to assign new owners to the segments so that
      // * num_owners(s) == numOwners, for each segment s
      // * floor(numSegments/numNodes) <= num_segments_primary_owned(n) for each node n
      // * floor(numSegments*numOwners/numNodes) <= num_segments_owned(n) for each node n
      // It will not change primary owners or remove old owners, but it will prepare things for the next phase
      // to remove owners so that
      // * num_segments_primary_owned(n) <= ceil(numSegments/numNodes) for each node n
      // * num_segments_owned(n) <= ceil(numSegments*numOwners/numNodes) for each node n
      Hash hashFunction = baseCH.getHashFunction();
      List<Address> nodes = baseCH.getMembers();

      CHStatistics stats = computeStatistics(baseCH, nodes);

      // Copy the owners list out of the old CH
      List<Address>[] ownerLists = extractSegmentOwners(baseCH);
      List<Address>[] intOwnerLists = extractSegmentOwners(baseCH);

      addPrimaryOwners(nodes, baseCH.getNumSegments(), stats, ownerLists, intOwnerLists);
      addBackupOwners(baseCH.getNumOwners(), nodes, baseCH.getNumSegments(), stats, ownerLists, intOwnerLists);

      DefaultConsistentHash ch = new DefaultConsistentHash(hashFunction, baseCH.getNumSegments(), baseCH.getNumOwners(),
            nodes, ownerLists);

      // we should return the base CH if we didn't change anything
      return ch.equals(baseCH) ? baseCH : ch;
   }

   /**
    * Merges two consistent hash objects that have the same number of segments, numOwners and hash function.
    */
   @Override
   public DefaultConsistentHash union(DefaultConsistentHash dch1, DefaultConsistentHash dch2) {
      if (!dch1.getHashFunction().equals(dch2.getHashFunction())) {
         throw new IllegalArgumentException("The consistent hash objects must have the same hash function");
      }
      if (dch1.getNumSegments() != dch2.getNumSegments()) {
         throw new IllegalArgumentException("The consistent hash objects must have the same number of segments");
      }
      if (dch1.getNumOwners() != dch2.getNumOwners()) {
         throw new IllegalArgumentException("The consistent hash objects must have the same number of owners");
      }

      List<Address> members = new ArrayList<Address>(dch1.getMembers());
      List<Address>[] segmentOwners = extractSegmentOwners(dch1);

      mergeLists(members, dch2.getMembers());
      for (int i = 0; i < segmentOwners.length; i++) {
         mergeLists(segmentOwners[i], dch2.locateOwnersForSegment(i));
      }

      return new DefaultConsistentHash(dch1.getHashFunction(), dch1.getNumSegments(), dch1.getNumOwners(), members, segmentOwners);
   }

   private void mergeLists(List<Address> list1, List<Address> list2) {
      for (Address a2 : list2) {
         if (!list1.contains(a2)) {
            list1.add(a2);
         }
      }
   }

   private void addPrimaryOwners(List<Address> nodes, int numSegments, CHStatistics stats, List<Address>[] ownerLists, List<Address>[] intOwnerLists) {
      // Compute how many segments each node has to primary-own.
      // If numSegments is not divisible by numNodes, older nodes will own 1 extra segment.
      Map<Address, Integer> expectedPrimaryOwned = computeExpectedPrimaryOwned(nodes, numSegments);
      List<Address> newPrimaryOwners = computeNewPrimaryOwners(nodes, stats, expectedPrimaryOwned);

      // Iterate over the segments, change ownership if the primary owner has > expectedPrimaryOwned
      // Iterate backwards to make the algorithm more stable
      for (int i = numSegments - 1; i >= 0; i--) {
         Address primaryOwner = ownerLists[i].get(0);
         int primaryOwned = stats.getPrimaryOwned(primaryOwner);
         if (primaryOwned > expectedPrimaryOwned.get(primaryOwner)) {
            // Need to pass primary ownership of this segment to another node.
            // First try to make one of the backup owners primary owner instead.
            Address newPrimaryOwner = removeOneOf(newPrimaryOwners, intOwnerLists[i]);
            if (newPrimaryOwner != null) {
               // Flip the old primary and the new primary nodes in the owners list
               ownerLists[i].remove(newPrimaryOwner);
               ownerLists[i].add(0, newPrimaryOwner);

               stats.decPrimaryOwned(primaryOwner);
               stats.incPrimaryOwned(newPrimaryOwner);
            } else {
               // The existing backup owners primary-own enough segments, add a new backup owner
               newPrimaryOwner = newPrimaryOwners.remove(0);

               // The primary owner stays the same in the intermediary CH
               intOwnerLists[i].add(newPrimaryOwner);
               ownerLists[i].add(0, newPrimaryOwner);

               stats.incOwned(newPrimaryOwner);
               stats.decPrimaryOwned(primaryOwner);
               stats.incPrimaryOwned(newPrimaryOwner);
            }
         }
      }
   }

   private List<Address>[] extractSegmentOwners(DefaultConsistentHash ch) {
      int numSegments = ch.getNumSegments();
      List<Address>[] segmentOwners = new List[numSegments];
      for (int i = 0; i < numSegments; i++) {
         segmentOwners[i] = ch.locateOwnersForSegment(i);
      }
      return segmentOwners;
   }

   private void addBackupOwners(int numOwners, List<Address> nodes, int numSegments, CHStatistics stats,
                                List<Address>[] ownerLists, List<Address>[] intOwnerLists) {
      int actualNumOwners = Math.min(numOwners, nodes.size());
      Map<Address, Integer> expectedOwned = computeExpectedOwned(nodes, actualNumOwners, numSegments);

      // Iterate backwards over the segments
      // If we find a segment with more owners than actualNumOwners and an owner has > expectedOwned segments, remove it
      for (int i = numSegments - 1; i >= 0; i--) {
         for (int j = ownerLists[i].size() - 1; j >= 1; j--) {
            if (ownerLists[i].size() <= actualNumOwners)
               break;

            Address owner = ownerLists[i].get(j);
            int owned = stats.getOwned(owner);
            if (owned > expectedOwned.get(owner)) {
               // We don't actually remove anything in the intermediary CH
               ownerLists[i].remove(j);
               stats.decOwned(owner);
            }
         }
      }

      // Iterate again, in the same order, but this time we can remove owners EITHER
      // because they have too many segments OR because the current segment has too many owners
      // We do this in two iterations in order to minimize the movement of segments.
      for (int i = numSegments - 1; i >= 0; i--) {
         for (int j = ownerLists[i].size() - 1; j >= 1; j--) {
            Address owner = ownerLists[i].get(j);
            int owned = stats.getOwned(owner);
            if (owned > expectedOwned.get(owner) || ownerLists[i].size() > actualNumOwners) {
               // We don't actually remove anything in the intermediary CH
               ownerLists[i].remove(j);
               stats.decOwned(owner);
            }
         }
      }

      // Now we know there are no segments with > numOwners owners, we can start adding back owners
      List<Address> newOwners = computeNewOwners(nodes, stats, expectedOwned);
      for (int i = 0; i < numSegments; i++) {
         for (int j = ownerLists[i].size(); j < actualNumOwners; j++) {
            Address newOwner = removeNotOneOf(newOwners, ownerLists[i]);

            if (newOwner != null) {
               // found a proper owner in the pending owners list
               ownerLists[i].add(newOwner);
               if (!intOwnerLists[i].contains(newOwner)) {
                  intOwnerLists[i].add(newOwner);
               }
               stats.incOwned(newOwner);
            } else {
               // couldn't find a proper new owner, so we're going to have to steal it from another segment
               Address rejectedNewOwner = newOwners.remove(0);
               stealOwner(numSegments, rejectedNewOwner, i, ownerLists, intOwnerLists, stats);
            }
         }
      }

      assert newOwners.isEmpty() : "Can't still have nodes to assign if all the segments have enough owners";
   }

   private void stealOwner(int numSegments, Address replacementOwner, int destSegment,
                           List<Address>[] ownerLists, List<Address>[] intOwnerLists, CHStatistics stats) {
      // try to find a replacement for the new owner in any segment, starting from the back
      for (int i = numSegments - 1; i >= 0; i--) {
         if (ownerLists[i].contains(replacementOwner))
            continue;

         for (int j = ownerLists[i].size() - 1; j >= 1; j--) {
            Address ownerToSteal = ownerLists[i].get(j);
            if (ownerLists[destSegment].contains(ownerToSteal))
               continue;

            // remove from the old segment
            ownerLists[i].remove(j);

            // add to the new segment
            ownerLists[destSegment].add(ownerToSteal);
            if (!intOwnerLists[destSegment].contains(ownerToSteal)) {
               intOwnerLists[destSegment].add(ownerToSteal);
            }

            ownerLists[i].add(replacementOwner);
            if (!intOwnerLists[i].contains(replacementOwner)) {
               intOwnerLists[i].add(replacementOwner);
            }

            stats.incOwned(replacementOwner);
            return;
         }
      }

      assert false : "It should always be possible to find a replacement owner";
   }

   private List<Address> computeNewPrimaryOwners(List<Address> nodes, CHStatistics stats,
                                                 Map<Address, Integer> expectedPrimaryOwned) {
      // Find the nodes that need to own more segments
      // A node can appear multiple times in this list, once for each new segment
      // But in order to make the job of the picker easier, we add nodes to the list in a round-robin fashion
      List<Address> newPrimaryOwners = new LinkedList<Address>();
      int[] toAdd = new int[nodes.size()];
      for (int i = 0; i < nodes.size(); i++) {
         Address node = nodes.get(i);
         toAdd[i] = expectedPrimaryOwned.get(node) - stats.getPrimaryOwned(node);
      }
      boolean changed = true;
      while (changed) {
         changed = false;
         for (int i = 0; i < nodes.size(); i++) {
            if (toAdd[i] > 0) {
               newPrimaryOwners.add(nodes.get(i));
               toAdd[i]--;
               changed = true;
            }
         }
      }
      return newPrimaryOwners;
   }

   private List<Address> computeNewOwners(List<Address> nodes, CHStatistics stats, Map<Address, Integer> expectedOwned) {
      // Find the nodes that need to own more segments
      // A node can appear multiple times in this list, once for each new segment
      // But in order to make the job of the picker easier, we add nodes to the list in a round-robin fashion
      LinkedList<Address> newOwners = new LinkedList<Address>();
      int[] toAdd = new int[nodes.size()];
      for (int i = 0; i < nodes.size(); i++) {
         Address node = nodes.get(i);
         toAdd[i] = expectedOwned.get(node) - stats.getOwned(node);
      }
      boolean changed = true;
      while (changed) {
         changed = false;
         for (int i = 0; i < nodes.size(); i++) {
            if (toAdd[i] > 0) {
               newOwners.addFirst(nodes.get(i));
               toAdd[i]--;
               changed = true;
            }
         }
      }
      return newOwners;
   }

   private Map<Address, Integer> computeExpectedPrimaryOwned(List<Address> nodes, int numSegments) {
      int numNodes = nodes.size();

      // Compute how many segments each node has to primary-own.
      // If numSegments is not divisible by numNodes, older nodes will own 1 extra segment.
      Map<Address, Integer> expectedPrimaryOwned = new HashMap<Address, Integer>(numNodes);
      for (int i = 0; i < numNodes; i++) {
         if (i < numSegments % numNodes) {
            expectedPrimaryOwned.put(nodes.get(i), numSegments / numNodes + 1);
         } else {
            expectedPrimaryOwned.put(nodes.get(i), numSegments / numNodes);
         }
      }
      return expectedPrimaryOwned;
   }

   private Map<Address, Integer> computeExpectedOwned(List<Address> nodes, int numOwners, int numSegments) {
      int numNodes = nodes.size();

      // Compute how many segments each node has to own.
      // If numSegments*numOwners is not divisible by numNodes, older nodes will own 1 extra segment.
      Map<Address, Integer> expectedOwned = new HashMap<Address, Integer>(numNodes);
      for (int i = 0; i < numNodes; i++) {
         if (i < (numSegments * numOwners) % numNodes) {
            expectedOwned.put(nodes.get(i), numSegments * numOwners / numNodes + 1);
         } else {
            expectedOwned.put(nodes.get(i), numSegments * numOwners / numNodes);
         }
      }
      return expectedOwned;
   }

   private Address removeOneOf(List<Address> list, List<Address> searchFor) {
      for (Iterator<Address> it = list.iterator(); it.hasNext(); ) {
         Address element = it.next();
         if (searchFor.contains(element)) {
            it.remove();
            return element;
         }
      }
      return null;
   }

   private Address removeNotOneOf(List<Address> list, List<Address> searchFor) {
      for (Iterator<Address> it = list.iterator(); it.hasNext(); ) {
         Address element = it.next();
         if (!searchFor.contains(element)) {
            it.remove();
            return element;
         }
      }
      return null;
   }

   /**
    * Remove all leavers and if some segments no longer have any owners assign them some owners.
    *
    * @param baseCH
    * @param newMembers
    * @return
    */
   private DefaultConsistentHash removeLeavers(DefaultConsistentHash baseCH, List<Address> newMembers) {
      int numSegments = baseCH.getNumSegments();

      // we assume leavers are far fewer than members, so it makes sense to check for leavers
      List<Address> leavers = new ArrayList<Address>(baseCH.getMembers());
      leavers.removeAll(newMembers);

      // remove leavers
      boolean segmentsWithZeroOwners = false;
      List<Address>[] newSegmentOwners = new List[numSegments];
      for (int i = 0; i < numSegments; i++) {
         List<Address> owners = baseCH.locateOwnersForSegment(i);
         owners.removeAll(leavers);
         segmentsWithZeroOwners |= owners.isEmpty();
         newSegmentOwners[i] = owners;
      }

      // if there are segments with 0 owners, fix them
      if (segmentsWithZeroOwners) {
         assignSegmentsWithZeroOwners(baseCH, newMembers, newSegmentOwners);
      }
      return new DefaultConsistentHash(baseCH.getHashFunction(), numSegments, baseCH.getNumOwners(),
            newMembers, newSegmentOwners);
   }

   private void assignSegmentsWithZeroOwners(DefaultConsistentHash baseDCH, List<Address> newMembers,
                                             List<Address>[] segmentOwners) {
      CHStatistics stats = computeStatistics(baseDCH, newMembers);
      int actualNumOwners = Math.min(baseDCH.getNumOwners(), newMembers.size());
      int numSegments = baseDCH.getNumSegments();
      Map<Address, Integer> expectedPrimaryOwnedSegments = computeExpectedPrimaryOwned(newMembers, numSegments);
      Map<Address, Integer> expectedOwnedSegments = computeExpectedOwned(newMembers, actualNumOwners, numSegments);
      for (int i = 0; i < numSegments; i++) {
         if (segmentOwners[i].isEmpty()) {
            // this segment doesn't have any owners, choose new ones
            List<Address> newOwners = new ArrayList<Address>(actualNumOwners);

            // pick the primary owner first
            // We pick the first node that doesn't "primary own" maxPrimaryOwnedSegments.
            // This algorithm will always pick a primary owner for a segment, because maxPrimaryOwnedSegments
            // is always recomputed so that numNodes * maxPrimaryOwnedSegments >= numSegments
            // It might leave some nodes with < minPrimaryOwnedSegments, but that's ok at this stage
            Address primaryOwner = null;
            for (Address a : newMembers) {
               primaryOwner = a;
               if (stats.getPrimaryOwned(a) < expectedPrimaryOwnedSegments.get(a)) {
                  break;
               }
            }
            newOwners.add(primaryOwner);
            stats.incPrimaryOwned(primaryOwner);
            stats.incOwned(primaryOwner);

            // then the backup owners
            // start again from the beginning so that we don't have to wrap around
            if (actualNumOwners > 1) {
               for (Address a : newMembers) {
                  if (stats.getOwned(a) < expectedOwnedSegments.get(a) && !newOwners.contains(a)) {
                     newOwners.add(a);
                     stats.incOwned(a);
                     if (newOwners.size() == actualNumOwners)
                        break;
                  }
               }
            }
            // we might have < numOwners owners at this point, if the base CH wasn't properly balanced
            // (i.e. some nodes owned > maxOwnedSegment)
            // but as long as we have at least one owner we're going to be fine
            segmentOwners[i] = newOwners;
         }
      }
   }

   private CHStatistics computeStatistics(DefaultConsistentHash ch, List<Address> nodes) {
      CHStatistics stats = new CHStatistics(nodes);
      for (int i = 0; i < ch.getNumSegments(); i++) {
         List<Address> owners = ch.locateOwnersForSegment(i);
         for (int j = 0; j < owners.size(); j++) {
            Address address = owners.get(j);
            if (nodes.contains(address)) {
               if (j == 0) {
                  stats.incPrimaryOwned(address);
               }
               stats.incOwned(address);
            }
         }
      }
      return stats;
   }

}
