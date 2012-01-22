/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.distribution.ch;

import org.infinispan.commons.hash.Hash;
import org.infinispan.marshall.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.TopologyAwareAddress;
import org.infinispan.util.Util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Consistent hash that is aware of cluster topology. Design described here: http://community.jboss.org/wiki/DesigningServerHinting.
 * <p/>
 * <pre>
 * Algorithm:
 * - place nodes on the hash wheel based address's hash code
 * - For selecting owner nodes:
 *       - pick the first one based on key's hash code
 *       - for subsequent nodes, walk clockwise and pick nodes that have a different site id
 *       - if not enough nodes found repeat walk again and pick nodes that have different site id and rack id
 *       - if not enough nodes found repeat walk again and pick nodes that have different site id, rack id and machine
 * id
 *       - Ultimately cycle back to the first node selected, don't discard any nodes, regardless of machine id/rack
 * id/site id match.
 * </pre>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
public class TopologyAwareConsistentHash extends AbstractWheelConsistentHash {
   private enum Level { SITE, RACK, MACHINE, NONE }

   private SortedSet<Integer> siteIdChangeIndexes = new TreeSet<Integer>();
   private SortedSet<Integer> rackIdChangeIndexes = new TreeSet<Integer>();
   private SortedSet<Integer> machineIdChangeIndexes = new TreeSet<Integer>();

   public TopologyAwareConsistentHash() {
   }

   public TopologyAwareConsistentHash(Hash hash) {
      setHashFunction(hash);
   }

   @Override
   public void setCaches(Set<Address> newCaches) {
      super.setCaches(newCaches);

      siteIdChangeIndexes.clear();
      rackIdChangeIndexes.clear();
      machineIdChangeIndexes.clear();
      TopologyAwareAddress lastSiteAddr = (TopologyAwareAddress) positionValues[positionValues.length - 1];
      TopologyAwareAddress lastRackAddr = (TopologyAwareAddress) positionValues[positionValues.length - 1];
      TopologyAwareAddress lastMachineAddr = (TopologyAwareAddress) positionValues[positionValues.length - 1];
      for (int i = 0; i < positionKeys.length; i++) {
         TopologyAwareAddress a = (TopologyAwareAddress) positionValues[i];
         if (!lastSiteAddr.isSameSite(a)) {
            siteIdChangeIndexes.add(i);
            lastSiteAddr = a;
         }
         if (!lastRackAddr.isSameRack(a)) {
            rackIdChangeIndexes.add(i);
            lastRackAddr = a;
         }
         if (!lastMachineAddr.isSameMachine(a)) {
            machineIdChangeIndexes.add(i);
            lastMachineAddr = a;
         }
      }
   }

   @Override
   public List<Address> locate(Object key, int replCount) {
      return locateInternal(key, replCount, null);
   }

   @Override
   public boolean isKeyLocalToAddress(Address target, Object key, int replCount) {
      return locateInternal(key, replCount, target).contains(target);
   }

   /**
    * Locate <code>replCount</code> owners for key <code>key</code> and return the list.
    * If one of the owners is identical to <code>target</code>, return after adding <code>target</code> to the list.
    */
   private List<Address> locateInternal(Object key, int replCount, Address target) {
      int actualReplCount = Math.min(replCount, caches.size());
      int keyNormalizedHash = getNormalizedHash(getGrouping(key));
      int firstOwnerIndex = getPositionIndex(keyNormalizedHash);
      Address firstOwner = positionValues[firstOwnerIndex];

      List<Address> owners = new ArrayList<Address>(actualReplCount);
      owners.add(firstOwner);
      if (owners.size() >= actualReplCount)
         return owners;

      // try to find owners with different site ids
      if (locateOwnersForLevel(firstOwnerIndex, actualReplCount, Level.SITE, siteIdChangeIndexes, target, owners))
         return owners;
      // try to find owners with different site ids and rack ids
      if (locateOwnersForLevel(firstOwnerIndex, actualReplCount, Level.RACK, rackIdChangeIndexes, target, owners))
         return owners;
      // try to find owners with different site ids, rack ids and machine ids
      if (locateOwnersForLevel(firstOwnerIndex, actualReplCount, Level.MACHINE, machineIdChangeIndexes, target, owners))
         return owners;

      // we have exhausted all the levels, now check for duplicate nodes on the same machines
      for (Iterator<Address> it = getPositionsIterator(keyNormalizedHash); it.hasNext();) {
         TopologyAwareAddress address = (TopologyAwareAddress) it.next();
         if (addOwner(owners, address, replCount, target, Level.NONE))
            return owners;
      }

      // might return < replCount owners if there aren't enough nodes in the list
      return owners;
   }

   /**
    * Locate owners for <code>keyNormalizedHash</code>, but consider only the addresses in <code>levelIdChangeIndexes</code>.
    * Return <code>false</code> when the list is exhausted, and <code>true</code> when we have found <code>replCount</code>
    * owners or <code>target</code> is one of the owners.
    */
   private boolean locateOwnersForLevel(int firstOwnerIndex, int replCount, Level level, SortedSet<Integer> levelIdChangeIndexes, Address target, List<Address> owners) {
      // start with the nodes after firstOwnerIndex in the wheel
      for (Integer addrIndex : levelIdChangeIndexes.tailSet(firstOwnerIndex)) {
         TopologyAwareAddress address = (TopologyAwareAddress) positionValues[addrIndex];
         if (addOwner(owners, address, replCount, target, level))
            return true;
      }
      // continue with the nodes from the beginning to firstOwnerIndex
      for (Integer addrIndex : levelIdChangeIndexes.headSet(firstOwnerIndex)) {
         TopologyAwareAddress address = (TopologyAwareAddress) positionValues[addrIndex];
         if (addOwner(owners, address, replCount, target, level))
            return true;
      }
      return false;
   }

   private boolean addOwner(List<Address> owners, TopologyAwareAddress address, int replCount, Address target, Level level) {
      boolean alreadyAdded = false;
      for (Address owner : owners) {
         switch (level) {
            case SITE:
               alreadyAdded = ((TopologyAwareAddress)owner).isSameSite(address);
               break;
            case RACK:
               alreadyAdded = ((TopologyAwareAddress)owner).isSameRack(address);
               break;
            case MACHINE:
               alreadyAdded = ((TopologyAwareAddress)owner).isSameMachine(address);
               break;
            case NONE:
               alreadyAdded = owner == address;
         }
         if (alreadyAdded)
            break;
      }
      if (!alreadyAdded) {
         owners.add(address);

         if (owners.size() >= replCount || address == target)
            return true;
      }
      return false;
   }

   public static class Externalizer extends AbstractWheelConsistentHash.Externalizer<TopologyAwareConsistentHash> {
      @Override
      protected TopologyAwareConsistentHash instance() {
         return new TopologyAwareConsistentHash();
      }

      @Override
      public Integer getId() {
         return Ids.TOPOLOGY_AWARE_CH;
      }

      @Override
      public Set<Class<? extends TopologyAwareConsistentHash>> getTypeClasses() {
         return Util.<Class<? extends TopologyAwareConsistentHash>>asSet(TopologyAwareConsistentHash.class);
      }
   }

}
