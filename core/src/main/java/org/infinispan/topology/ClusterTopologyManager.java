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

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;

/**
 * Maintains the list of members and performs rebalance operations.
 * The {@link RebalancePolicy} actually decides when to perform the rebalance or how to update the
 * consistent hash.
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Scope(Scopes.GLOBAL)
public interface ClusterTopologyManager {
   /**
    * Used by {@link RebalancePolicy} to update the consistent hash on all the members,
    * without triggering a state transfer.
    */
   void updateConsistentHash(String cacheName, CacheTopology cacheTopology) throws Exception;

   /**
    * Used by {@link RebalancePolicy} to start a state transfer.
    */
   void rebalance(String cacheName, CacheTopology cacheTopology) throws Exception;


   /**
    * Updates the members list and notifies the {@link RebalancePolicy}.
    * @return The current consistent hash.
    */
   CacheTopology handleJoin(String cacheName, Address joiner, CacheJoinInfo joinInfo, int viewId) throws Exception;

   /**
    * Updates the members list and notifies the {@link RebalancePolicy}
    */
   void handleLeave(String cacheName, Address leaver, int viewId) throws Exception;

   /**
    * Marks the rebalance as complete on the sender.
    */
   void handleRebalanceCompleted(String cacheName, Address node, int topologyId, Throwable throwable, int viewId) throws Exception;
}
