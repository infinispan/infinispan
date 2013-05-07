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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

import org.infinispan.commons.hash.Hash;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;

/**
 * Default implementation of {@link ConsistentHashFactory}.
 *
 * All methods except {@link #union(DefaultConsistentHash, DefaultConsistentHash)} return a consistent hash
 * with floor(numOwners*numSegments/numNodes) <= segments per owner <= ceil(numOwners*numSegments/numNodes).
 *
 * @author Dan Berindei
 * @author anistor@redhat.com
 * @since 5.2
 */
public class DefaultConsistentHashFactory implements ConsistentHashFactory<DefaultConsistentHash> {

   @Override
   public DefaultConsistentHash create(Hash hashFunction, int numOwners, int numSegments, List<Address> members) {
      if (numOwners <= 0)
         throw new IllegalArgumentException("The number of owners should be greater than 0");

      // Use the CH rebalance algorithm to get an even spread
      // Round robin doesn't work properly because a segment's owner must be unique,
      Builder builder = new Builder(hashFunction, numOwners, numSegments, members);
      rebalanceBuilder(builder);

      return builder.build();
   }

   /**
    * Leavers are removed and segments without owners are assigned new owners. Joiners might get some of the un-owned
    * segments but otherwise they are not taken into account (that should happen during a rebalance).
    *
    * @param baseCH An existing consistent hash instance, should not be {@code null}
    * @param actualMembers A list of addresses representing the new cache members.
    * @return
    */
   @Override
   public DefaultConsistentHash updateMembers(DefaultConsistentHash baseCH, List<Address> actualMembers) {
      if (actualMembers.equals(baseCH.getMembers()))
         return baseCH;

      // The builder constructor automatically removes leavers
      Builder builder = new Builder(baseCH, actualMembers);

      // If there are segments with 0 owners, fix them
      // Try to assign the same owners for those segments as a future rebalance call would.
      Builder balancedBuilder = null;
      for (int segment = 0; segment < baseCH.getNumSegments(); segment++) {
         if (builder.getOwners(segment).isEmpty()) {
            if (balancedBuilder == null) {
               balancedBuilder = new Builder(builder);
               rebalanceBuilder(balancedBuilder);
            }
            builder.addOwners(segment, balancedBuilder.getOwners(segment));
         }
      }
      return builder.build();
   }

   @Override
   public DefaultConsistentHash rebalance(DefaultConsistentHash baseCH) {

      // This method assign new owners to the segments so that
      // * num_owners(s) == numOwners, for each segment s
      // * floor(numSegments/numNodes) <= num_segments_primary_owned(n) for each node n
      // * num_segments_primary_owned(n) <= ceil(numSegments/numNodes) for each node n
      // * floor(numSegments*numOwners/numNodes) <= num_segments_owned(n) for each node n
      // * num_segments_owned(n) <= ceil(numSegments*numOwners/numNodes) for each node n
      Builder builder = new Builder(baseCH);
      rebalanceBuilder(builder);

      DefaultConsistentHash balancedCH = builder.build();

      // we should return the base CH if we didn't change anything
      return balancedCH.equals(baseCH) ? baseCH : balancedCH;
   }

   /**
    * Merges two consistent hash objects that have the same number of segments, numOwners and hash function.
    * For each segment, the primary owner of the first CH has priority, the other primary owners become backups.
    */
   @Override
   public DefaultConsistentHash union(DefaultConsistentHash dch1, DefaultConsistentHash dch2) {
      return dch1.union(dch2);
   }

   protected void rebalanceBuilder(Builder builder) {
      addPrimaryOwners(builder);
      addBackupOwners(builder);
      // balancePrimaryOwners(builder);
   }

   protected void addPrimaryOwners(Builder builder) {
      int minPrimarySegments = builder.getNumSegments() / builder.getNumNodes();

      // 1. Try to replace primary owners with too many segments with the backups in those segments.
      swapPrimaryOwnersWithBackups(builder, minPrimarySegments + 1);
      swapPrimaryOwnersWithBackups(builder, minPrimarySegments);
      swapPrimaryOwnersWithBackups(builder, minPrimarySegments + 1);

      // 2. If existing backup owners weren't enough, try to add new backup owners and then to the swap.
      // 2.1. In the first phase, try to keep the number of owners below actualNumOwners
      int actualNumOwners = builder.getActualNumOwners();
      doAddPrimaryOwners(builder, minPrimarySegments + 1, actualNumOwners);
      doAddPrimaryOwners(builder, minPrimarySegments, actualNumOwners);
      doAddPrimaryOwners(builder, minPrimarySegments + 1, actualNumOwners);

      // 2.2. In the second phase, allow numOwners + 1 owners for each segment.
      // Since a segment only has 1 primary owner, this will be enough to give us a "proper" primary owner
      // for each segment.
      doAddPrimaryOwners(builder, minPrimarySegments + 1, actualNumOwners + 1);
      doAddPrimaryOwners(builder, minPrimarySegments, actualNumOwners + 1);
      doAddPrimaryOwners(builder, minPrimarySegments + 1, actualNumOwners + 1);
   }

   protected void doAddPrimaryOwners(Builder builder, int maxSegments, int maxOwners) {
      // If a segment has primaryOwned(primaryOwner(segment)) > maxSegments,
      // and owners(segment) < maxOwners, add a new primary owner from the members list.
      // The new primary owner must primary-own < minSegments segments.
      for (int segment = builder.getNumSegments() - 1; segment >= 0; segment--) {
         if (builder.getOwners(segment).size() >= maxOwners)
            continue;

         // Must be able to deal with segments that don't have any owners
         // Either when creating a new CH or when all the owners of a segment left
         boolean zeroOwners = builder.getOwners(segment).isEmpty();
         if (!zeroOwners && builder.getPrimaryOwned(builder.getPrimaryOwner(segment)) <= maxSegments)
            continue;

         Address newPrimary = findNewPrimaryOwner(builder, builder.getMembers(), maxSegments);
         if (newPrimary != null) {
            builder.replacePrimaryOwner(segment, newPrimary);
         }
      }
   }

   protected void swapPrimaryOwnersWithBackups(Builder builder, int maxSegments) {
      // If a segment has primaryOwned(primaryOwner(segment)) > maxPrimarySegments,
      // try to swap the primary owner with one of the backup owners.
      // The new primary owner must primary-own < minPrimarySegments segments.
      // Iterate in reverse order so the CH looks more stable in the logs as we add nodes
      for (int segment = builder.getNumSegments() - 1; segment >= 0; segment--) {
         if (builder.getOwners(segment).isEmpty())
            continue;

         if (builder.getPrimaryOwned(builder.getPrimaryOwner(segment)) > maxSegments) {
            Address newPrimary = findNewPrimaryOwner(builder, builder.getBackupOwners(segment), maxSegments);
            if (newPrimary != null) {
               // actually replaces the primary owner
               builder.replacePrimaryOwner(segment, newPrimary);
            }
         }
      }
   }

   protected void addBackupOwners(Builder builder) {
      int minSegments = builder.getActualNumOwners() * builder.getNumSegments() / builder.getNumNodes();

      // 1. Remove extra owners (could be leftovers from addPrimaryOwners).
      removeExtraBackupOwners(builder, minSegments);

      // 2. If owners(segment) < numOwners, add new owners.
      // In the first phase, the new owners must own < minSegments segments.
      // It may not be possible to fill all the segments with numOwners owners this way,
      // so we repeat this in a loop, each iteration with a higher limit of owned segments
      boolean insufficientOwners = true;
      int maxSegments = minSegments;
      while(insufficientOwners) {
         insufficientOwners = doAddBackupOwners(builder, maxSegments);
         maxSegments++;
      }

      // 3. Now owners(segment) == numOwners for every segment because of steps 1 and 2.
      // 3.1. If there is an owner with owned(owner) > minSegments, find another node
      // with owned(node) < minSegments and replace that owner with it.
      // Do it iteratively in order to spread the new owners as much as possible.
      for (maxSegments = builder.getNumSegments() - 1; maxSegments >= minSegments; maxSegments--) {
         replaceBackupOwners(builder, maxSegments);
      }

      // 3.2. Same as step 3.1, but allow replacing nodes that have owned(node) > minSegments + 1segments
      // with nodes that already have owned(node) = minSegments.
      // Necessary when numOwners*numSegments doesn't divide evenly with numNodes,
      // because all nodes could own minSegments segments and yet one node could own
      // minSegments + (numOwners*numSegments % numNodes) segments.
      replaceBackupOwners(builder, minSegments + 1);

      // At this point each node should have minSegments <= owned(node) <= minSegments + 1
   }

   protected void removeExtraBackupOwners(Builder builder, int minSegments) {
      boolean tooManyOwners = true;
      int maxSegments = minSegments + 1;
      while(tooManyOwners) {
         tooManyOwners = doRemoveExtraBackupOwners(builder, maxSegments);
         maxSegments--;
      }
   }

   protected boolean doRemoveExtraBackupOwners(Builder builder, int maxSegments) {
      boolean tooManyOwners = false;
      for (int segment = 0; segment < builder.getNumSegments(); segment++) {
         List<Address> owners = builder.getOwners(segment);
         for (int ownerIdx = owners.size() - 1; ownerIdx >= 1; ownerIdx--) {
            if (owners.size() <= builder.getActualNumOwners())
               break;

            Address owner = owners.get(ownerIdx);
            if (builder.getOwned(owner) > maxSegments) {
               // Owner has too many segments. Remove it.
               builder.removeOwner(segment, owner);
            }
         }
         tooManyOwners |= builder.getOwners(segment).size() > builder.getActualNumOwners();
      }
      return tooManyOwners;
   }

   protected boolean doAddBackupOwners(Builder builder, int maxSegments) {
      boolean insufficientOwners = false;
      for (int segment = 0; segment < builder.getNumSegments(); segment++) {
         List<Address> owners = builder.getOwners(segment);

         while (owners.size() < builder.getActualNumOwners()) {
            Address newOwner = findNewBackupOwner(builder, owners, maxSegments);
            // If we haven't found an owner, we'll only find it with an increased maxSegments
            if (newOwner == null) {
               insufficientOwners = true;
               break;
            }

            builder.addOwner(segment, newOwner);
         }
      }
      return insufficientOwners;
   }

   protected void replaceBackupOwners(Builder builder, int maxSegments) {
      // Iterate over the owners in the outer loop so that we minimize the number of owner changes
      // for the same segment.
      for (int ownerIdx = builder.getActualNumOwners() - 1; ownerIdx >= 0; ownerIdx--) {
         // Iterate in reverse order so the CH looks more stable in the logs as we add nodes
         for (int segment = builder.getNumSegments() - 1; segment >= 0; segment--) {
            List<Address> owners = builder.getOwners(segment);
            Address owner = owners.get(ownerIdx);
            if (builder.getOwned(owner) > maxSegments) {
               // This owner has too many segments. Find another node to replace it with.
               Address replacement = findNewBackupOwner(builder, owners, maxSegments);
               if (replacement != null) {
                  builder.removeOwner(segment, owner);
                  builder.addOwner(segment, replacement);
               }
            }
         }
      }
   }

   /**
    * @return The member with the least owned segments that is also not in the excludes list.
    */
   protected Address findNewBackupOwner(Builder builder, Collection<Address> excludes, int maxSegments) {
      // find the member with the least owned segments
      Address best = null;
      int foundOwned = maxSegments;
      for (Address candidate : builder.getMembers()) {
         if (builder.getOwned(candidate) >= foundOwned)
            continue;

         if (excludes == null || !excludes.contains(candidate)) {
            best = candidate;
            foundOwned = builder.getOwned(candidate);
         }
      }
      return best;
   }

   /**
    * @return The candidate with the least primary-owned segments that is also not in the excludes list.
    */
   protected Address findNewPrimaryOwner(Builder builder, Collection<Address> candidates, int maxSegments) {
      // find the member with the least owned segments
      Address best = null;
      int foundOwned = maxSegments;
      for (Address candidate : candidates) {
         if (builder.getPrimaryOwned(candidate) < foundOwned) {
            best = candidate;
            foundOwned = builder.getPrimaryOwned(candidate);
         }
      }
      return best;
   }


   protected static class Builder {
      private final Hash hashFunction;
      private final int initialNumOwners;
      private final int actualNumOwners;
      private final List<Address>[] segmentOwners;
      private final OwnershipStatistics stats;
      private final List<Address> members;

      public Builder(Hash hashFunction, int numOwners, int numSegments, List<Address> members) {
         this.hashFunction = hashFunction;
         this.initialNumOwners = numOwners;
         this.actualNumOwners = Math.min(numOwners, members.size());
         this.members = members;
         this.segmentOwners = new List[numSegments];
         for (int segment = 0; segment < numSegments; segment++) {
            segmentOwners[segment] = new ArrayList<Address>(actualNumOwners);
         }
         this.stats = new OwnershipStatistics(members);
      }

      public Builder(DefaultConsistentHash baseCH, List<Address> actualMembers) {
         int numSegments = baseCH.getNumSegments();
         Set<Address> actualMembersSet = new HashSet<Address>(actualMembers);
         List[] owners = new List[numSegments];
         for (int segment = 0; segment < numSegments; segment++) {
            owners[segment] = new ArrayList<Address>(baseCH.locateOwnersForSegment(segment));
            owners[segment].retainAll(actualMembersSet);
         }
         this.hashFunction = baseCH.getHashFunction();
         this.initialNumOwners = baseCH.getNumOwners();
         this.actualNumOwners = Math.min(initialNumOwners, actualMembers.size());
         this.members = actualMembers;
         this.segmentOwners = owners;
         this.stats = new OwnershipStatistics(baseCH, actualMembers);
      }

      public Builder(DefaultConsistentHash baseCH) {
         this(baseCH, baseCH.getMembers());
      }

      public Builder(Builder other) {
         int numSegments = other.getNumSegments();
         List[] owners = new List[numSegments];
         for (int segment = 0; segment < numSegments; segment++) {
            owners[segment] = new ArrayList<Address>(other.segmentOwners[segment]);
         }
         this.hashFunction = other.hashFunction;
         this.initialNumOwners = other.initialNumOwners;
         this.actualNumOwners = other.actualNumOwners;
         this.members = other.members;
         this.segmentOwners = owners;
         this.stats = new OwnershipStatistics(other.stats);
      }

      public int getActualNumOwners() {
         return actualNumOwners;
      }

      public int getNumSegments() {
         return segmentOwners.length;
      }

      public List<Address> getMembers() {
         return members;
      }

      public int getNumNodes() {
         return getMembers().size();
      }

      public List<Address> getOwners(int segment) {
         return segmentOwners[segment];
      }

      public Address getPrimaryOwner(int segment) {
         return segmentOwners[segment].get(0);
      }

      public List<Address> getBackupOwners(int segment) {
         return segmentOwners[segment].subList(1, segmentOwners[segment].size());
      }

      public boolean addOwner(int segment, Address owner) {
         List<Address> thisSegmentOwners = segmentOwners[segment];
         if (thisSegmentOwners.contains(owner))
            return false;

         thisSegmentOwners.add(owner);
         stats.incOwned(owner);
         if (thisSegmentOwners.size() == 1) {
            // the first owner
            stats.incPrimaryOwned(owner);
         }
         return true;
      }

      public boolean addOwners(int segment, Collection<Address> newOwners) {
         boolean modified = false;
         for (Address owner : newOwners) {
            modified |= addOwner(segment, owner);
         }
         return modified;
      }

      public boolean removeOwner(int segment, Address owner) {
         List<Address> segmentOwners = this.segmentOwners[segment];
         if (segmentOwners.get(0).equals(owner)) {
            stats.decPrimaryOwned(owner);
         }
         boolean modified = segmentOwners.remove(owner);
         if (modified) {
            stats.decOwned(owner);
         }
         return modified;
      }

      public void replacePrimaryOwner(int segment, Address newPrimaryOwner) {
         List<Address> segmentOwners = this.segmentOwners[segment];
         int ownerIndex = segmentOwners.indexOf(newPrimaryOwner);
         if (ownerIndex == 0) {
            throw new IllegalStateException("Can't replace a primary owner with itself");
         }

         if (segmentOwners.isEmpty()) {
            segmentOwners.add(newPrimaryOwner);
            stats.incOwned(newPrimaryOwner);
            stats.incPrimaryOwned(newPrimaryOwner);
            return;
         }

         Address oldPrimaryOwner = segmentOwners.get(0);
         if (ownerIndex == -1) {
            stats.incOwned(newPrimaryOwner);
         } else {
            // The new primary owner was already a backup owner, first remove it from the list
            segmentOwners.remove(ownerIndex);
         }

         segmentOwners.add(0, newPrimaryOwner);
         stats.decPrimaryOwned(oldPrimaryOwner);
         stats.incPrimaryOwned(newPrimaryOwner);
      }

      public DefaultConsistentHash build() {
         return new DefaultConsistentHash(hashFunction, initialNumOwners, segmentOwners.length, members, segmentOwners);
      }

      private int getPrimaryOwned(Address node) {
         return stats.getPrimaryOwned(node);
      }

      public int getOwned(Address node) {
         return stats.getOwned(node);
      }
   }

   public static class Externalizer extends AbstractExternalizer<DefaultConsistentHashFactory> {

      @Override
      public void writeObject(ObjectOutput output, DefaultConsistentHashFactory chf) throws IOException {
      }

      @Override
      @SuppressWarnings("unchecked")
      public DefaultConsistentHashFactory readObject(ObjectInput unmarshaller) throws IOException, ClassNotFoundException {
         return new DefaultConsistentHashFactory();
      }

      @Override
      public Integer getId() {
         return Ids.DEFAULT_CONSISTENT_HASH_FACTORY;
      }

      @Override
      public Set<Class<? extends DefaultConsistentHashFactory>> getTypeClasses() {
         return Collections.<Class<? extends DefaultConsistentHashFactory>>singleton(DefaultConsistentHashFactory.class);
      }
   }
}
