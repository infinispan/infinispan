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
      // Replace owners that have too many segments with owners that have too few.
      replaceBackupOwnersForLevel(builder, minSegments, Level.MACHINE);
      replaceBackupOwnersForLevel(builder, minSegments, Level.RACK);
      replaceBackupOwnersForLevel(builder, minSegments, Level.SITE);
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
               if (!locationAlreadyAdded(owners, candidate, null, level)) {
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

   protected void replaceBackupOwnersForLevel(Builder builder, int minSegments, Level level) {
      // 3.1. If there is an owner with owned(owner) > minSegments + 1, find another node
      // with owned(node) < minSegments and replace that owner with it.
      doReplaceBackupOwnersForLevel(builder, minSegments, minSegments + 1, level);
      // 3.2. Same as step 3.1, but also replace owners that own minSegments + 1 segments.
      // Doing this in a separate iteration minimizes the number of moves from nodes with
      // owned(node) == minSegments + 1, when numOwners*numSegments doesn't divide evenly with numNodes.
      doReplaceBackupOwnersForLevel(builder, minSegments, minSegments, level);
      // 3.3. Same as step 3.1, but allow replacing with nodes that already have owned(node) = minSegments.
      // Necessary when numOwners*numSegments doesn't divide evenly with numNodes,
      // because all nodes could own minSegments segments and yet one node could own
      // minSegments + (numOwners*numSegments % numNodes) segments.
      doReplaceBackupOwnersForLevel(builder, minSegments + 1, minSegments + 1, level);
   }

   private void doReplaceBackupOwnersForLevel(Builder builder, int minSegments, int maxSegments, Level level) {
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
                     if (!owners.contains(candidate) && maintainsLocations(owners, candidate, owner)) {
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

   private boolean maintainsLocations(List<Address> owners, Address candidate, Address replaced) {
      TopologyAwareAddress topologyAwareCandidate = (TopologyAwareAddress) candidate;
      TopologyAwareAddress topologyAwareReplaced = (TopologyAwareAddress) replaced;

      Set<String> newSites = new HashSet<String>();
      Set<String> newRacks = new HashSet<String>();
      Set<String> newMachines = new HashSet<String>();
      for (Address node : owners) {
         if (node.equals(replaced))
            continue;

         TopologyAwareAddress topologyAwareNode = (TopologyAwareAddress) node;
         newSites.add(topologyAwareNode.getSiteId());
         newRacks.add(topologyAwareNode.getSiteId() + "|" + topologyAwareNode.getRackId());
         newMachines.add(topologyAwareNode.getSiteId() + "|" + topologyAwareNode.getRackId()
               + "|" + topologyAwareNode.getMachineId());
      }

      newSites.add(topologyAwareCandidate.getSiteId());
      newRacks.add(topologyAwareCandidate.getSiteId() + "|" + topologyAwareCandidate.getRackId());
      newMachines.add(topologyAwareCandidate.getSiteId() + "|" + topologyAwareCandidate.getRackId()
            + "|" + topologyAwareCandidate.getMachineId());

      if (!newSites.contains(topologyAwareReplaced.getSiteId()))
         return false;
      if (!newRacks.contains(topologyAwareReplaced.getRackId()))
         return false;
      if (!newMachines.contains(topologyAwareReplaced.getMachineId()))
         return false;
      return true;
   }

   private boolean locationAlreadyAdded(List<Address> owners, Address candidate, Address replaced, Level level) {
      TopologyAwareAddress topologyAwareCandidate = (TopologyAwareAddress) candidate;
      boolean locationAlreadyAdded = false;
      for (Address owner : owners) {
         if (owner.equals(replaced))
            continue;

         TopologyAwareAddress topologyAwareOwner = (TopologyAwareAddress) owner;
         switch (level) {
            case SITE:
               locationAlreadyAdded = topologyAwareCandidate.isSameSite(topologyAwareOwner);
               break;
            case RACK:
               locationAlreadyAdded = topologyAwareCandidate.isSameRack(topologyAwareOwner);
               break;
            case MACHINE:
               locationAlreadyAdded = topologyAwareCandidate.isSameMachine(topologyAwareOwner);
               break;
            case NONE:
               locationAlreadyAdded = owner.equals(candidate);
         }
         if (locationAlreadyAdded)
            break;
      }
      return locationAlreadyAdded;
   }
}
