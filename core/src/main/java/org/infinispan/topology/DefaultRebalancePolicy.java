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

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Default implementation of {@link RebalancePolicy}
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class DefaultRebalancePolicy implements RebalancePolicy {
   private static Log log = LogFactory.getLog(DefaultRebalancePolicy.class);

   private ClusterTopologyManager clusterTopologyManager;
   private Set<String> cachesPendingRebalance = null;
   private final Object lock = new Object();

   @Inject
   public void inject(ClusterTopologyManager clusterTopologyManager) {
      this.clusterTopologyManager = clusterTopologyManager;
   }

   @Override
   public void initCache(String cacheName, ClusterCacheStatus cacheStatus) throws Exception {
      log.tracef("Initializing rebalance policy for cache %s", cacheName);
   }

   @Override
   public void updateCacheStatus(String cacheName, ClusterCacheStatus cacheStatus) throws Exception {
      log.tracef("Cache %s status changed: joiners=%s, topology=%s", cacheName, cacheStatus.getJoiners(),
            cacheStatus.getCacheTopology());
      if (!cacheStatus.hasMembers()) {
         log.tracef("Not triggering rebalance for zero-members cache %s", cacheName);
         return;
      }

      if (!cacheStatus.hasJoiners() && isBalanced(cacheStatus.getCacheTopology().getCurrentCH())) {
         log.tracef("Not triggering rebalance for cache %s, no joiners and the current consistent hash is already balanced",
               cacheName);
         return;
      }

      if (cacheStatus.isRebalanceInProgress()) {
         log.tracef("Not triggering rebalance for cache %s, a rebalance is already in progress", cacheName);
         return;
      }

      synchronized (lock) {
         if (!isRebalancingEnabled()) {
            log.tracef("Rebalancing is disabled, queueing rebalance for cache %s", cacheName);
            cachesPendingRebalance.add(cacheName);
            return;
         }
      }

      log.tracef("Triggering rebalance for cache %s", cacheName);
      clusterTopologyManager.triggerRebalance(cacheName);
   }

   public boolean isBalanced(ConsistentHash ch) {
      int numSegments = ch.getNumSegments();
      int actualNumOwners = Math.min(ch.getMembers().size(), ch.getNumOwners());
      for (int i = 0; i < numSegments; i++) {
         if (ch.locateOwnersForSegment(i).size() != actualNumOwners) {
            return false;
         }
      }
      return true;
   }

   @ManagedAttribute(description = "Rebalancing enabled", displayName = "Rebalancing enabled",
         dataType = DataType.TRAIT, writable = true)
   @Override
   public boolean isRebalancingEnabled() {
      synchronized (lock) {
         return cachesPendingRebalance == null;
      }
   }

   @Override
   public void setRebalancingEnabled(boolean enabled) {
      Set<String> caches;
      synchronized (lock) {
         caches = cachesPendingRebalance;
         if (enabled) {
            cachesPendingRebalance = null;
         } else {
            cachesPendingRebalance = new HashSet<String>();
         }
      }

      if (enabled && caches != null) {
         log.debugf("Rebalancing re-enabled, triggering rebalancing for caches %s", caches);
         for (String cacheName : caches) {
            try {
               clusterTopologyManager.triggerRebalance(cacheName);
            } catch (Exception e) {
               log.rebalanceStartError(cacheName, e);
            }
         }
      } else {
         log.debugf("Rebalancing suspended");
      }
   }
}
