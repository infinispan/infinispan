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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.infinispan.remoting.transport.Address;

/**
 * Special implementation of {@link AdvancedConsistentHash} for replicated caches.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class ReplicatedConsistentHash implements AdvancedConsistentHash {
   private List<Address> members;

   public ReplicatedConsistentHash(List<Address> members) {
      this.members = new ArrayList<Address>(members);
   }

   @Override
   public int getNumSegments() {
      return 1;
   }

   @Override
   public int getSegment(Object key) {
      return 0;
   }

   @Override
   public List<Address> locateOwnersForSegment(int segmentId) {
      return members;
   }

   @Override
   public Address locatePrimaryOwnerForSegment(int segmentId) {
      return members.get(0);
   }

   @Override
   public int getNumOwners() {
      return Integer.MAX_VALUE;
   }

   @Override
   public List<Address> getMembers() {
      return members;
   }

   @Override
   public Address locatePrimaryOwner(Object key) {
      return members.get(0);
   }

   @Override
   public List<Address> locateOwners(Object key) {
      return members;
   }

   @Override
   public Set<Address> locateAllOwners(Collection<Object> keys) {
      return new HashSet<Address>(members);
   }

   @Override
   public boolean isKeyLocalToNode(Address nodeAddress, Object key) {
      return true;
   }
}
