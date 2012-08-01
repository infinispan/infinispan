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

import org.infinispan.distribution.ch.AdvancedConsistentHash;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * {@link AdvancedConsistentHash} wrapper that groups keys to the same node based on their @{@link Group}
 * annotation.
 * <p/>
 * It uses a {@link GroupManager} to determine the group key from annotations.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class GroupingAdvancedConsistentHash implements AdvancedConsistentHash {
   private AdvancedConsistentHash ch;
   private GroupManager groupManager;

   public GroupingAdvancedConsistentHash(AdvancedConsistentHash ch, GroupManager groupManager) {
      this.ch = ch;
      this.groupManager = groupManager;
   }

   @Override
   public int getNumSegments() {
      return ch.getNumSegments();
   }

   @Override
   public int getSegment(Object key) {
      String groupKey = groupManager.getGroup(key);
      return ch.getSegment(groupKey);
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
   public int getNumOwners() {
      return ch.getNumOwners();
   }

   @Override
   public List<Address> getMembers() {
      return ch.getMembers();
   }

   @Override
   public Address locatePrimaryOwner(Object key) {
      return ch.locatePrimaryOwner(key);
   }

   @Override
   public List<Address> locateOwners(Object key) {
      return ch.locateOwners(key);
   }

   @Override
   public Set<Address> locateAllOwners(Collection<Object> keys) {
      return ch.locateAllOwners(keys);
   }

   @Override
   public boolean isKeyLocalToNode(Address nodeAddress, Object key) {
      return ch.isKeyLocalToNode(nodeAddress, key);
   }
}
