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

import java.util.Map;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Runs on every node and handles the communication with the {@link ClusterTopologyManager}.
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Scope(Scopes.GLOBAL)
public interface LocalTopologyManager {
   /**
    * Forwards the join request to the coordinator.
    * @return The current consistent hash.
    */
   CacheTopology join(String cacheName, CacheJoinInfo joinInfo, CacheTopologyHandler stm) throws Exception;

   /**
    * Forwards the leave request to the coordinator.
    */
   void leave(String cacheName);

   /**
    * Confirm that the local cache {@code cacheName} has finished receiving the new data for topology
    * {@code topologyId}.
    *
    * <p>The coordinator can change during the state transfer, so we make the rebalance RPC async
    * and we send the response as a different command.
    *
    * @param cacheName the name of the cache
    * @param topologyId the current topology id of the node at the time the rebalance is completed. This must be >= than the one when rebalance starts.
    * @param throwable {@code null} unless local rebalance ended because of an error.
    */
   void confirmRebalance(String cacheName, int topologyId, Throwable throwable);

   /**
    * Recovers the current topology information for all running caches and returns it to the coordinator.
    * @param viewId
    */
   // TODO Add a new class to hold the CacheJoinInfo and the CacheTopology
   Map<String, Object[]> handleStatusRequest(int viewId);

   /**
    * Updates the current and/or pending consistent hash, without transferring any state.
    */
   void handleConsistentHashUpdate(String cacheName, CacheTopology cacheTopology, int viewId) throws InterruptedException;

   /**
    * Performs the state transfer.
    */
   void handleRebalance(String cacheName, CacheTopology cacheTopology, int viewId) throws InterruptedException;

   /**
    * @return the current topology for a cache.
    */
   CacheTopology getCacheTopology(String cacheName);
}
