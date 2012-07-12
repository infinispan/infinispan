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

package org.infinispan.topology;

import java.util.List;

import org.infinispan.distribution.newch.ConsistentHash;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;

/**
 * Processes cache membership changes and decides when to rebalance state between members.
 * It is used both in distributed and replicated mode.
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Scope(Scopes.GLOBAL)
interface RebalancePolicy {
   /**
    * Initialize the policy for a cache, with an existing list of members.
    */
   void initCache(String cacheName, List<Address> memberList, CacheJoinInfo joinInfo);

   /**
    * Initialize the policy for an existing cache, after this node became the coordinator.
    * @param cacheName
    * @param existingCHs
    */
   void initCache(String cacheName, ConsistentHash... existingCHs);

   void updateMembersList(String cacheName, List<Address> membersList);
}
