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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.TopologyAwareAddress;

/**
 * Default topology-aware consistent hash factory implementation.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class TopologyAwareConsistentHashFactory extends DefaultConsistentHashFactory {
   private enum Level { SITE, RACK, MACHINE, NONE }

   @Override
   protected void addBackupOwners(Builder builder) {
      int minSegments = builder.getActualNumOwners() * builder.getNumSegments() / builder.getNumNodes();

      // 1. Remove extra owners (could be leftovers from addPrimaryOwners).
      // Don't worry about location information yet.
      removeExtraBackupOwners(builder, minSegments);

      // 2. If owners(segment) < numOwners, add new owners.
      // Unlike the parent class, we allow many more segments for one node just in order to get
      // as many different sites, racks and machines in the same owner list.
      addBackupOwnersForLevel(builder, minSegments, Level.SITE);
      addBackupOwnersForLevel(builder, minSegments, Level.RACK);
      addBackupOwnersForLevel(builder, minSegments, Level.MACHINE);

      addBackupOwnersForLevel(builder, minSegments, Level.NONE);

      // 3. Now owners(segment) == numOwners for every segment because of steps 1 and 2.
      replaceBackupOwnersForLevel(builder, Level.SITE);
      replaceBackupOwnersForLevel(builder, Level.RACK);
      replaceBackupOwnersForLevel(builder, Level.MACHINE);

      // Replace owners that have too many segments with owners that have too few.
      replaceBackupOwnerForMachineLevel(builder, minSegments);
   }

   private void addBackupOwnersForLevel(Builder builder, int minSegments, Level level) {
      // In the first phase, the new owners must own < minSegments segments.
      // It may not be possible to fill all the segments with numOwners owners this way,
      // so we repeat this in a loop, each iteration with a higher limit of owned segments
      int currentMax = minSegments;
      while (doAddBackupOwnersForLevel(builder, currentMax, level)) {
         currentMax++;
      }
   }

   private boolean doAddBackupOwnersForLevel(Builder builder, int maxSegments, Level level) {
      // Mostly copied from DefaultConsistentHashFactory.doAddBackupOwners, but with an extra location check
      boolean sufficientOwners = true;
      boolean modified = false;
      for (int segment = 0; segment < builder.getNumSegments(); segment++) {
         List<Address> owners = builder.getOwners(segment);
         for (Address candidate : builder.getMembers()) {
            if (owners.size() >= builder.getActualNumOwners())
               break;

            if (builder.getOwned(candidate) < maxSegments) {
               if (!owners.contains(candidate) && !locationIsDuplicate(candidate, owners, level)) {
                  builder.addOwner(segment, candidate);
                  modified = true;
               }
            }
         }
         sufficientOwners &= owners.size() >= builder.getActualNumOwners();
      }

      // If we didn't add any owners this time, we won't add any owners with a higher maxSegments either
      return !sufficientOwners && modified;
   }

   protected void replaceBackupOwnersForLevel(Builder builder, Level level) {
      // At this point each segment already has actualNumOwners owners.
      for (int segment = 0; segment < builder.getNumSegments(); segment++) {
         List<Address> owners = builder.getOwners(segment);
         List<Address> backupOwners = builder.getBackupOwners(segment);
         for (int i = backupOwners.size() - 1; i >= 0; i--) {
            Address owner = backupOwners.get(i);
            if (locationIsDuplicate(owner, owners, level)) {
               // Got a duplicate site/rack/machine, we might have an alternative for it.
               for (Address candidate : builder.getMembers()) {
                  if (!owners.contains(candidate) && !locationIsDuplicate(candidate, owners, level)) {
                     builder.addOwner(segment, candidate);
                     builder.removeOwner(segment, owner);
                     // Update the owners list, needed for the locationIsDuplicate check.
                     owners = builder.getOwners(segment);
                     backupOwners = builder.getBackupOwners(segment);
                     break;
                  }
               }
            }
         }
      }
   }

   protected void replaceBackupOwnerForMachineLevel(Builder builder, int minSegments) {
      // 3.1. If there is an owner with owned(owner) > minSegments + 1, find another node
      // with owned(node) < minSegments and replace that owner with it.
      doReplaceBackupOwnersSameMachine(builder, minSegments, minSegments + 1);
      // 3.2. Same as step 3.1, but also replace owners that own minSegments + 1 segments.
      // Doing this in a separate iteration minimizes the number of moves from nodes with
      // owned(node) == minSegments + 1, when numOwners*numSegments doesn't divide evenly with numNodes.
      doReplaceBackupOwnersSameMachine(builder, minSegments, minSegments);
      // 3.3. Same as step 3.1, but allow replacing with nodes that already have owned(node) = minSegments.
      // Necessary when numOwners*numSegments doesn't divide evenly with numNodes,
      // because all nodes could own minSegments segments and yet one node could own
      // minSegments + (numOwners*numSegments % numNodes) segments.
      doReplaceBackupOwnersSameMachine(builder, minSegments + 1, minSegments + 1);
   }

   private void doReplaceBackupOwnersSameMachine(Builder builder, int minSegments, int maxSegments) {
      // Iterate over the owners in the outer loop so that we minimize the number of owner changes
      // for the same segment. At this point each segment already has actualNumOwners owners.
      for (int ownerIdx = builder.getActualNumOwners() - 1; ownerIdx >= 1; ownerIdx--) {
         for (int segment = 0; segment < builder.getNumSegments(); segment++) {
            List<Address> owners = builder.getOwners(segment);
            Address owner = owners.get(ownerIdx);
            if (builder.getOwned(owner) > maxSegments) {
               // Owner has too many segments. Find another node to replace it with.
               for (Address candidate : builder.getMembers()) {
                  if (builder.getOwned(candidate) < minSegments) {
                     if (!owners.contains(candidate) && maintainsMachines(owners, candidate, owner)) {
                        builder.addOwner(segment, candidate);
                        builder.removeOwner(segment, owner);
                        break;
                     }
                  }
               }
            }
         }
      }
   }

   private Object getLocationId(Address address, Level level) {
      TopologyAwareAddress taa = (TopologyAwareAddress) address;
      Object locationId;
      switch (level) {
         case SITE:
            locationId = "" + taa.getSiteId();
            break;
         case RACK:
            locationId = taa.getSiteId() + "|" + taa.getRackId();
            break;
         case MACHINE:
            locationId = taa.getSiteId() + "|" + taa.getRackId() + "|" + taa.getMachineId();
            break;
         case NONE:
            locationId = address;
            break;
         default:
            throw new IllegalStateException("Unknown level: " + level);
      }
      return locationId;
   }

   private boolean locationIsDuplicate(Address target, List<Address> addresses, Level level) {
      for (Address address : addresses) {
         if (address != target && getLocationId(address, level).equals(getLocationId(target, level)))
            return true;
      }
      return false;
   }

   private boolean maintainsMachines(List<Address> owners, Address candidate, Address replaced) {
      Set<Object> newMachines = new HashSet<Object>(owners.size());
      newMachines.add(getLocationId(candidate, Level.MACHINE));

      for (Address node : owners) {
         if (!node.equals(replaced)) {
            newMachines.add(getLocationId(node, Level.MACHINE));
         }
      }

      return newMachines.contains(getLocationId(replaced, Level.MACHINE));
   }
}
