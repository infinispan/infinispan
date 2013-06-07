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

/**
 * Processes cache membership changes or any other events and decides when to
 * rebalance state between members.
 *
 * It is used both in distributed and replicated mode.
 *
 * Implementations can trigger a rebalance using {@link ClusterTopologyManager#triggerRebalance(String)}.
 * They don't control the resulting consistent hash directly, but they can use the {@link ClusterCacheStatus}
 * to access the cache's custom {@link org.infinispan.distribution.ch.ConsistentHashFactory} implementation
 * and influence the generated consistent hash indirectly.
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Scope(Scopes.GLOBAL)
public interface RebalancePolicy {
   /**
    * Initialize the policy for a cache, without a list of members.
    * It won't have any effect if the cache is already initialized.
    */
   void initCache(String cacheName, ClusterCacheStatus cacheStatus) throws Exception;

   /**
    * Called when the status of a cache changes. It could be a node joining or leaving, or a merge, or a
    */
   void updateCacheStatus(String cacheName, ClusterCacheStatus cacheStatus) throws Exception;

   /**
    * @return {@code true} if rebalancing is allowed, {@code false} if rebalancing is suspended.
    * @since 5.3
    */
   boolean isRebalancingEnabled();

   /**
    * @param enabled {@code true} to start rebalancing (immediately starting the rebalance if necessary),
    *                {@code false} to suspend it.
    * @since 5.3
    */
   void setRebalancingEnabled(boolean enabled);
}
