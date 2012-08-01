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

import org.infinispan.commons.hash.Hash;
import org.infinispan.remoting.transport.Address;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Factory for ReplicatedConsistentHash.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class ReplicatedConsistentHashFactory implements ConsistentHashFactory<ReplicatedConsistentHash> {
   @Override
   public ReplicatedConsistentHash create(Hash hashFunction, int numOwners, int numSegments, List<Address> members) {
      return new ReplicatedConsistentHash(members);
   }

   @Override
   public ReplicatedConsistentHash updateMembers(ReplicatedConsistentHash baseCH, List<Address> newMembers) {
      if (newMembers.equals(baseCH.getMembers()))
         return baseCH;

      return new ReplicatedConsistentHash(newMembers);
   }

   @Override
   public ReplicatedConsistentHash rebalance(ReplicatedConsistentHash baseCH) {
      return baseCH;
   }

   @Override
   public ReplicatedConsistentHash union(ReplicatedConsistentHash ch1, ReplicatedConsistentHash ch2) {
      Set<Address> membersUnion = new HashSet<Address>(ch1.getMembers().size() + ch2.getMembers().size());
      membersUnion.addAll(ch1.getMembers());
      membersUnion.addAll(ch2.getMembers());
      return new ReplicatedConsistentHash(new ArrayList<Address>(membersUnion));
   }
}
