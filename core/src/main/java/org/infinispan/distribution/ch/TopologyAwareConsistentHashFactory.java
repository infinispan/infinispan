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

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.infinispan.distribution.topologyaware.TopologyInfo;
import org.infinispan.distribution.topologyaware.TopologyLevel;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.TopologyAwareAddress;

/**
 * Default topology-aware consistent hash factory implementation.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class TopologyAwareConsistentHashFactory extends DefaultConsistentHashFactory {

   @Override
   protected void addBackupOwners(Builder builder) {
      TopologyInfo topologyInfo = new TopologyInfo(builder.getMembers());
      int minSegments = builder.getActualNumOwners() * builder.getNumSegments() / builder.getNumNodes();

      // 1. Remove extra owners (could be leftovers from addPrimaryOwners).
      // Don't worry about location information yet.
      removeExtraBackupOwners(builder, minSegments);

      // 2. If owners(segment) < numOwners, add new owners.
      // Unlike the parent class, we allow many more segments for one node just in order to get
      // as many different sites, racks and machines in the same owner list.
      addBackupOwnersForLevel(builder, topologyInfo, TopologyLevel.SITE);
      addBackupOwnersForLevel(builder, topologyInfo, TopologyLevel.RACK);
      addBackupOwnersForLevel(builder, topologyInfo, TopologyLevel.MACHINE);

      addBackupOwnersForLevel(builder, topologyInfo, TopologyLevel.NODE);

      // 3. Now owners(segment) == numOwners for every segment because of steps 1 and 2.
      replaceBackupOwnersForLevel(builder, topologyInfo, TopologyLevel.SITE);
      replaceBackupOwnersForLevel(builder, topologyInfo, TopologyLevel.RACK);
      replaceBackupOwnersForLevel(builder, topologyInfo, TopologyLevel.MACHINE);

      // Replace owners that have too many segments with owners that have too few.
      replaceBackupOwnerNoLevel(builder, topologyInfo);
   }

   private void addBackupOwnersForLevel(Builder builder, TopologyInfo topologyInfo, TopologyLevel level) {
      // In the first phase, the new owners must own < minSegments segments.
      // It may not be possible to fill all the segments with numOwners owners this way,
      // so we repeat this in a loop, each iteration with a higher limit of owned segments
      int extraSegments = 0;
      while (doAddBackupOwnersForLevel(builder, topologyInfo, level, extraSegments)) {
         extraSegments++;
      }
   }

   private boolean doAddBackupOwnersForLevel(Builder builder, TopologyInfo topologyInfo, TopologyLevel level, int extraSegments) {
      boolean sufficientOwners = true;
      for (int segment = 0; segment < builder.getNumSegments(); segment++) {
         List<Address> owners = builder.getOwners(segment);
         if (owners.size() >= builder.getActualNumOwners())
            continue;

         int maxDistinctLocations = topologyInfo.getDistinctLocationsCount(level, builder.getActualNumOwners());
         int distinctLocations = new TopologyInfo(owners).getDistinctLocationsCount(level, builder.getActualNumOwners());
         if (distinctLocations == maxDistinctLocations)
            continue;

         for (Address candidate : builder.getMembers()) {
            int maxSegments = topologyInfo.computeMaxSegments(builder.getNumSegments(),
                  builder.getActualNumOwners(), candidate) + extraSegments;
            if (builder.getOwned(candidate) < maxSegments) {
               if (!owners.contains(candidate) && !locationIsDuplicate(owners, candidate, level)) {
                  builder.addOwner(segment, candidate);
                  distinctLocations++;
                  // The owners list is live, no need to query it again
                  if (owners.size() >= builder.getActualNumOwners())
                     break;
               }
            }
         }

         if (distinctLocations < maxDistinctLocations && owners.size() < builder.getActualNumOwners()) {
            sufficientOwners = false;
         }
      }

      return !sufficientOwners;
   }

   private void replaceBackupOwnersForLevel(Builder builder, TopologyInfo topologyInfo, TopologyLevel level) {
      int extraSegments = 0;
      while (doReplaceBackupOwnersForLevel(builder, topologyInfo, level, extraSegments)) {
         extraSegments++;
      }
   }

   private boolean doReplaceBackupOwnersForLevel(Builder builder, TopologyInfo topologyInfo,
                                                   TopologyLevel level, int extraSegments) {
      boolean sufficientLocations = true;
      // At this point each segment already has actualNumOwners owners.
      for (int segment = 0; segment < builder.getNumSegments(); segment++) {
         List<Address> owners = builder.getOwners(segment);
         int maxDistinctLocations = topologyInfo.getDistinctLocationsCount(level, builder.getActualNumOwners());
         int distinctLocations = new TopologyInfo(owners).getDistinctLocationsCount(level, builder.getActualNumOwners());
         if (distinctLocations == maxDistinctLocations)
            continue;

         for (int i = owners.size() - 1; i >= 1; i--) {
            Address owner = owners.get(i);
            if (locationIsDuplicate(owners, owner, level)) {
               // Got a duplicate site/rack/machine, we might have an alternative for it.
               for (Address candidate : builder.getMembers()) {
                  int maxSegments = topologyInfo.computeMaxSegments(builder.getNumSegments(),
                        builder.getActualNumOwners(), candidate);
                  if (builder.getOwned(candidate) < maxSegments + extraSegments) {
                     if (!owners.contains(candidate) && !locationIsDuplicate(owners, candidate, level)) {
                        builder.addOwner(segment, candidate);
                        builder.removeOwner(segment, owner);
                        distinctLocations++;
                        // The owners list is live, no need to query it again
                        break;
                     }
                  }
               }
            }
         }

         if (distinctLocations < maxDistinctLocations) {
            sufficientLocations = false;
         }
      }
      return !sufficientLocations;
   }

   private void replaceBackupOwnerNoLevel(Builder builder, TopologyInfo topologyInfo) {
      // 3.1. If there is an owner with owned(owner) > maxSegments, find another node
      // with owned(node) < maxSegments and replace that owner with it.
      doReplaceBackupOwnersNoLevel(builder, topologyInfo, -1, 0);
      // 3.2. Same as step 3.1, but also replace owners that own maxSegments segments.
      // Doing this in a separate iteration minimizes the number of moves from nodes with
      // owned(node) == maxSegments, when numOwners*numSegments doesn't divide evenly with numNodes.
      doReplaceBackupOwnersNoLevel(builder, topologyInfo, -1, -1);
      // 3.3. Same as step 3.1, but allow replacing with nodes that already have owned(node) = maxSegments - 1.
      // Necessary when numOwners*numSegments doesn't divide evenly with numNodes,
      // because all nodes could own maxSegments - 1 segments and yet one node could own
      // maxSegments + (numOwners*numSegments % numNodes) segments.
      doReplaceBackupOwnersNoLevel(builder, topologyInfo, 0, 0);
   }

   private void doReplaceBackupOwnersNoLevel(Builder builder, TopologyInfo topologyInfo,
                                             int minSegmentsDiff, int maxSegmentsDiff) {
      // Iterate over the owners in the outer loop so that we minimize the number of owner changes
      // for the same segment. At this point each segment already has actualNumOwners owners.
      for (int ownerIdx = builder.getActualNumOwners() - 1; ownerIdx >= 1; ownerIdx--) {
         for (int segment = 0; segment < builder.getNumSegments(); segment++) {
            List<Address> owners = builder.getOwners(segment);
            Address owner = owners.get(ownerIdx);
            int maxSegments = topologyInfo.computeMaxSegments(builder.getNumSegments(),
                  builder.getActualNumOwners(), owner) + maxSegmentsDiff;
            if (builder.getOwned(owner) > maxSegments) {
               // Owner has too many segments. Find another node to replace it with.
               for (Address candidate : builder.getMembers()) {
                  int minSegments = topologyInfo.computeMaxSegments(builder.getNumSegments(),
                        builder.getActualNumOwners(), candidate) + minSegmentsDiff;
                  if (builder.getOwned(candidate) < minSegments) {
                     if (!owners.contains(candidate) && maintainsDiversity(owners, candidate, owner)) {
                        builder.addOwner(segment, candidate);
                        builder.removeOwner(segment, owner);
                        // The owners list is live, no need to query it again
                        break;
                     }
                  }
               }
            }
         }
      }
   }

   private Object getLocationId(Address address, TopologyLevel level) {
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
         case NODE:
            locationId = address;
            break;
         default:
            throw new IllegalStateException("Unknown level: " + level);
      }
      return locationId;
   }

   private boolean locationIsDuplicate(List<Address> addresses, Address target, TopologyLevel level) {
      Object targetLocationId = getLocationId(target, level);
      for (Address address : addresses) {
         if (address != target && getLocationId(address, level).equals(targetLocationId))
            return true;
      }
      return false;
   }

   private boolean maintainsDiversity(List<Address> owners, Address candidate, Address replaced) {
      return maintainsDiversity(owners, candidate, replaced, TopologyLevel.SITE)
            && maintainsDiversity(owners, candidate, replaced, TopologyLevel.RACK)
            && maintainsDiversity(owners, candidate, replaced, TopologyLevel.MACHINE);
   }

   private boolean maintainsDiversity(List<Address> owners, Address candidate, Address replaced, TopologyLevel machine) {
      Set<Object> oldMachines = new HashSet<Object>(owners.size());
      Set<Object> newMachines = new HashSet<Object>(owners.size());
      newMachines.add(getLocationId(candidate, machine));

      for (Address node : owners) {
         oldMachines.add(getLocationId(node, machine));
         if (!node.equals(replaced)) {
            newMachines.add(getLocationId(node, machine));
         }
      }

      return newMachines.size() >= oldMachines.size();
   }

   public static class Externalizer extends AbstractExternalizer<TopologyAwareConsistentHashFactory> {

      @Override
      public void writeObject(ObjectOutput output, TopologyAwareConsistentHashFactory chf) {
      }

      @Override
      @SuppressWarnings("unchecked")
      public TopologyAwareConsistentHashFactory readObject(ObjectInput unmarshaller) {
         return new TopologyAwareConsistentHashFactory();
      }

      @Override
      public Integer getId() {
         return Ids.TOPOLOGY_AWARE_CONSISTENT_HASH_FACTORY;
      }

      @Override
      public Set<Class<? extends TopologyAwareConsistentHashFactory>> getTypeClasses() {
         return Collections.<Class<? extends TopologyAwareConsistentHashFactory>>singleton(TopologyAwareConsistentHashFactory.class);
      }
   }
}
