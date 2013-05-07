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

package org.infinispan.distribution.group;

import org.infinispan.commons.hash.Hash;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * {@link ConsistentHash} wrapper that groups keys to the same node based on their @{@link Group}
 * annotation.
 * <p/>
 * It uses a {@link GroupManager} to determine the group key from annotations.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class GroupingConsistentHash implements ConsistentHash {
   private final ConsistentHash ch;
   private final GroupManager groupManager;

   public GroupingConsistentHash(ConsistentHash ch, GroupManager groupManager) {
      this.ch = ch;
      this.groupManager = groupManager;
   }

   @Override
   public int getNumSegments() {
      return ch.getNumSegments();
   }

   @Override
   public int getNumOwners() {
      return ch.getNumOwners();
   }

   @Override
   public Hash getHashFunction() {
      return null;
   }

   @Override
   public List<Address> getMembers() {
      return ch.getMembers();
   }

   @Override
   public int getSegment(Object key) {
      return ch.getSegment(getGroupKey(key));
   }

   @Override
   public List<Address> locateOwnersForSegment(int segmentId) {
      return ch.locateOwnersForSegment(segmentId);
   }

   @Override
   public Address locatePrimaryOwnerForSegment(int segmentId) {
      return ch.locatePrimaryOwnerForSegment(segmentId);
   }

   @Override
   public Set<Integer> getSegmentsForOwner(Address owner) {
      return ch.getSegmentsForOwner(owner);
   }

   @Override
   public String getRoutingTableAsString() {
      return ch.getRoutingTableAsString();
   }

   @Override
   public Address locatePrimaryOwner(Object key) {
      return ch.locatePrimaryOwner(getGroupKey(key));
   }

   @Override
   public List<Address> locateOwners(Object key) {
      return ch.locateOwners(getGroupKey(key));
   }

   @Override
   public Set<Address> locateAllOwners(Collection<Object> keys) {
      // We have to duplicate the work in DefaultConsistentHash.locateAllOwners
      // because there's no way to call back from DCH to our getSegment(key) method.
      HashSet<Integer> segments = new HashSet<Integer>();
      for (Object key : keys) {
         segments.add(getSegment(key));
      }
      HashSet<Address> owners = new HashSet<Address>();
      for (Integer segment : segments) {
         owners.addAll(locateOwnersForSegment(segment));
      }
      return owners;
   }

   @Override
   public boolean isKeyLocalToNode(Address nodeAddress, Object key) {
      return ch.isKeyLocalToNode(nodeAddress, getGroupKey(key));
   }

   private Object getGroupKey(Object key) {
      Object finalKey = key;
      String groupKey = groupManager.getGroup(key);
      if (groupKey != null) {
         finalKey = groupKey;
      }
      return finalKey;
   }

   @Override
   public String toString() {
      return "GroupingConsistentHash:" + ch;
   }
}
