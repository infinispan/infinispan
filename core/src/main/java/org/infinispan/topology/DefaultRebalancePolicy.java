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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.distribution.newch.ConsistentHash;
import org.infinispan.distribution.newch.ConsistentHashFactory;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.concurrent.ConcurrentMapFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;

/**
 * Default implementation of {@code RebalancePolicy}
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class DefaultRebalancePolicy implements RebalancePolicy {
   private static Log log = LogFactory.getLog(DefaultRebalancePolicy.class);

   private Transport transport;
   private ClusterTopologyManager clusterTopologyManager;
   private ExecutorService asyncTransportExecutor;
   private GlobalConfiguration globalConfiguration;

   private volatile List<Address> clusterMembers;
   private final ConcurrentMap<String, CacheStatus> cacheStatusMap = ConcurrentMapFactory.makeConcurrentMap();

   public void inject(Transport transport, ClusterTopologyManager clusterTopologyManager,
                      @ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService asyncTransportExecutor,
                      GlobalConfiguration globalConfiguration) {
      this.transport = transport;
      this.clusterTopologyManager = clusterTopologyManager;
      this.asyncTransportExecutor = asyncTransportExecutor;
      this.globalConfiguration = globalConfiguration;
   }

   // must start before ClusterTopologyManager
   @Start(priority = 99)
   public void start() {
      this.clusterMembers = transport.getMembers();
   }

   @Override
   public void initCache(String cacheName, CacheJoinInfo joinInfo) throws Exception {
      cacheStatusMap.putIfAbsent(cacheName, new CacheStatus(joinInfo, globalConfiguration));
   }

   @Override
   public void initCache(String cacheName, List<CacheTopology> partitionTopologies) {
      throw new IllegalStateException("Not implemented");
   }

   @Override
   public void updateMembersList(List<Address> clusterMembers) throws Exception {
      this.clusterMembers = clusterMembers;

      for (Map.Entry<String, CacheStatus> e : cacheStatusMap.entrySet()) {
         String cacheName = e.getKey();
         CacheStatus cacheStatus = e.getValue();
         synchronized (cacheStatus) {
            cacheStatus.joiners.retainAll(clusterMembers);
            ConsistentHash currentCH = cacheStatus.cacheTopology.getCurrentCH();
            // the consistent hash may not be initialized yet
            if (currentCH == null)
               continue;
            ConsistentHash pendingCH = cacheStatus.cacheTopology.getPendingCH();
            boolean currentMembersValid = clusterMembers.containsAll(currentCH.getMembers());
            boolean pendingMembersValid = pendingCH == null || clusterMembers.containsAll(pendingCH.getMembers());
            if (!currentMembersValid || !pendingMembersValid) {
               int topologyId = cacheStatus.cacheTopology.getTopologyId();
               List<Address> newMembers1 = new ArrayList<Address>(currentCH.getMembers());
               newMembers1.retainAll(clusterMembers);
               ConsistentHash newCurrentCH = cacheStatus.chFactory.updateMembers(currentCH, newMembers1);
               List<Address> newMembers = new ArrayList<Address>(pendingCH.getMembers());
               newMembers.retainAll(clusterMembers);
               ConsistentHash newPendingCH = cacheStatus.chFactory.updateMembers(pendingCH, newMembers);
               cacheStatus.cacheTopology = new CacheTopology(topologyId, newCurrentCH, newPendingCH);
               clusterTopologyManager.updateConsistentHash(cacheName, topologyId, newCurrentCH, newPendingCH);
            }
         }
      }
   }

   @Override
   public void updateMembersList(String cacheName, List<Address> joiners, List<Address> leavers) throws Exception {
      // TODO Separate into two methods, join() and leave()
      CacheStatus cacheStatus = cacheStatusMap.get(cacheName);
      if (!leavers.isEmpty()) {
         synchronized (cacheStatus) {
            int topologyId = cacheStatus.cacheTopology.getTopologyId();
            ConsistentHash currentCH = cacheStatus.cacheTopology.getCurrentCH();
            ConsistentHash pendingCH = cacheStatus.cacheTopology.getPendingCH();

            // The list of "current" members will always be included in the set of "pending" members,
            // because leaves are reflected at the same time in both collections
            List<Address> newMembers = new ArrayList<Address>(clusterMembers);
            newMembers.retainAll(pendingCH.getMembers());
            ConsistentHash newPendingCH = cacheStatus.chFactory.updateMembers(pendingCH, newMembers);

            newMembers.retainAll(currentCH.getMembers());
            ConsistentHash newCurrentCH = cacheStatus.chFactory.updateMembers(currentCH, newMembers);

            cacheStatus.cacheTopology = new CacheTopology(topologyId, newCurrentCH, newPendingCH);
            clusterTopologyManager.updateConsistentHash(cacheName, topologyId, newCurrentCH, newPendingCH);
         }
      }
      if (!joiners.isEmpty()) {
         synchronized (cacheStatus) {
            cacheStatus.joiners.addAll(joiners);

            int topologyId = cacheStatus.cacheTopology.getTopologyId();
            ConsistentHash currentCH = cacheStatus.cacheTopology.getCurrentCH();
            ConsistentHash pendingCH = cacheStatus.cacheTopology.getPendingCH();
            if (pendingCH != null) {
               // there is already a rebalance in progress
               log.debugf("Received join request from %s, but there is already a rebalance operation " +
                     "in progress. The node will join once the current rebalance has ended.", joiners);
               return;
            }

            int newTopologyId = topologyId + 1;
            List<Address> newMembers = cacheStatus.joiners;
            cacheStatus.joiners = new ArrayList<Address>();
            newMembers.addAll(cacheStatus.cacheTopology.getMembers());
            newMembers.retainAll(clusterMembers);

            // TODO Keep track in cacheStatus of the "balanced" CH instead of calling rebalance again
            ConsistentHash balancedCH = cacheStatus.chFactory.rebalance(pendingCH);
            cacheStatus.balancedCH = balancedCH;
            ConsistentHash newPendingCH = cacheStatus.chFactory.union(currentCH, balancedCH);
            cacheStatus.cacheTopology = new CacheTopology(newTopologyId, currentCH, newPendingCH);
            clusterTopologyManager.updateConsistentHash(cacheName, newTopologyId, currentCH, newPendingCH);
         }
      }
   }

   @Override
   public void onRebalanceCompleted(String cacheName, int topologyId) {
      CacheStatus cacheStatus = cacheStatusMap.get(cacheName);
      synchronized (cacheStatus) {
         assert topologyId == cacheStatus.cacheTopology.getTopologyId();
         int newTopologyId = topologyId + 1;
         ConsistentHash currentCH = cacheStatus.cacheTopology.getCurrentCH();
         ConsistentHash newCurrentCH = cacheStatus.chFactory.rebalance(currentCH);

         cacheStatus.cacheTopology = new CacheTopology(newTopologyId, newCurrentCH, null);
         cacheStatus.balancedCH = null;
      }
   }

   @Override
   public CacheTopology getTopology(String cacheName) {
      return cacheStatusMap.get(cacheName).cacheTopology;
   }


   private static class CacheStatus {
      private ConsistentHashFactory chFactory;
      private int numSegments;
      private int numOwners;

      private CacheTopology cacheTopology;
      private List<Address> joiners;
      public ConsistentHash balancedCH;

      public CacheStatus(CacheJoinInfo joinInfo, GlobalConfiguration globalConfiguration) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
         Class<?> chfClass = globalConfiguration.classLoader().loadClass(joinInfo.getConsistentHashFactoryClass());
         this.chFactory = (ConsistentHashFactory) chfClass.newInstance();
         this.numSegments = joinInfo.getNumSegments();
         this.numOwners = joinInfo.getNumOwners();
         this.joiners = new ArrayList<Address>();
      }
   }
}
