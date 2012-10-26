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

import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.TopologyAwareAddress;

/**
 * A {@link org.infinispan.distribution.ch.ConsistentHashFactory} implementation that guarantees caches
 * with the same members have the same consistent hash and also tries to distribute segments based on the
 * topology information in {@link org.infinispan.configuration.global.TransportConfiguration}.
 * <p/>
 * It has a drawback compared to {@link org.infinispan.distribution.ch.DefaultConsistentHashFactory}:
 * it can potentially move a lot more segments during a rebalance than strictly necessary.
 * <p/>
 * It is not recommended using the {@code TopologyAwareSyncConsistentHashFactory} with a very small number
 * of segments. The distribution of segments to owners gets better with a higher number of segments, and is
 * especially bad when {@code numSegments &lt; numNodes}
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class TopologyAwareSyncConsistentHashFactory extends SyncConsistentHashFactory {

   private enum Level { SITE, RACK, MACHINE, NONE }

   @Override
   protected void populateOwnersFewSegments(Builder builder, SortedMap<Integer, Address> primarySegments) {
      // Too few segments for each member to have one "primary segment",
      // but we may still have enough segments for each member to be a backup owner.

      // Populate the primary owners first - because numSegments < numMembers we're guaranteed to
      // set the primary owner of each segment
      for (Map.Entry<Integer, Address> e : primarySegments.entrySet()) {
         Integer segment = e.getKey();
         Address primaryOwner = e.getValue();
         builder.getOwners(segment).add(primaryOwner);
      }

      // Continue with the backup owners. Assign each member as owner to one segment,
      // then repeat until each segment has numOwners owners.
      populateBackupOwners(builder, Level.SITE);
      populateBackupOwners(builder, Level.RACK);
      populateBackupOwners(builder, Level.MACHINE);
      populateBackupOwners(builder, Level.NONE);
   }

   private boolean populateBackupOwners(Builder builder, Level level) {
      boolean modified = false;
      // Try to add each node as an owner to one segment
      for (Address member : builder.getSortedMembers()) {
         // Compute an initial segment and iterate backwards to make it more like the other case
         int initSegment = normalizedHash(builder.getHashFunction(), member.hashCode()) / builder.getSegmentSize();
         for (int i = 0; i < builder.getNumSegments(); i++) {
            int segment = (builder.getNumSegments() + initSegment - i) % builder.getNumSegments();
            List<Address> owners = builder.getOwners(segment);
            if (owners.size() < builder.getActualNumOwners() && locationAlreadyAdded(member, owners, level)) {
               owners.add(member);
               modified = true;
               break;
            }
         }
      }
      return modified;
   }

   @Override
   protected void populateOwnersManySegments(Builder builder, SortedMap<Integer, Address> primarySegments) {
      // Each member is present at least once in the primary segments map, so we can use that
      // to populate the owner lists. For each segment assign the owners of the next numOwners
      // "primary segments" as owners.
      for (int segment = 0; segment < builder.getNumSegments(); segment++) {
         populateSegmentOwners(builder, primarySegments, segment, Level.SITE);
         populateSegmentOwners(builder, primarySegments, segment, Level.RACK);
         populateSegmentOwners(builder, primarySegments, segment, Level.MACHINE);
         populateSegmentOwners(builder, primarySegments, segment, Level.NONE);
      }
   }

   private void populateSegmentOwners(Builder builder, SortedMap<Integer, Address> primarySegments,
                                      int segment, Level level) {
      List<Address> owners = builder.getOwners(segment);
      if (owners.size() >= builder.getActualNumOwners())
         return;

      for (Address a : primarySegments.tailMap(segment).values()) {
         if (owners.size() >= builder.getActualNumOwners())
            return;
         if (!locationAlreadyAdded(a, owners, level)) {
            owners.add(a);
         }
      }
      for (Address a : primarySegments.headMap(segment).values()) {
         if (owners.size() >= builder.getActualNumOwners())
            return;
         if (!locationAlreadyAdded(a, owners, level)) {
            owners.add(a);
         }
      }
   }

   private boolean locationAlreadyAdded(Address candidate, List<Address> owners, Level level) {
      TopologyAwareAddress topologyAwareCandidate = (TopologyAwareAddress) candidate;
      boolean locationAlreadyAdded = false;
      for (Address owner : owners) {
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
