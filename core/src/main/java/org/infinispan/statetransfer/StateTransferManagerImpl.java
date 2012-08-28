/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.distribution.ch.*;
import org.infinispan.distribution.group.GroupManager;
import org.infinispan.distribution.group.GroupingConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.topology.CacheJoinInfo;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.CacheTopologyHandler;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author anistor@redhat.com
 * @since 5.2
 */
public class StateTransferManagerImpl implements StateTransferManager {

   private static final Log log = LogFactory.getLog(StateTransferManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   private StateConsumer stateConsumer;
   private StateProvider stateProvider;
   private String cacheName;
   private CacheNotifier cacheNotifier;
   private Configuration configuration;
   private GlobalConfiguration globalConfiguration;
   private RpcManager rpcManager;
   private GroupManager groupManager;   // optional
   private LocalTopologyManager localTopologyManager;

   public StateTransferManagerImpl() {
   }

   @Inject
   public void init(StateConsumer stateConsumer,
                    StateProvider stateProvider,
                    Cache cache,
                    CacheNotifier cacheNotifier,
                    Configuration configuration,
                    GlobalConfiguration globalConfiguration,
                    RpcManager rpcManager,
                    GroupManager groupManager,
                    LocalTopologyManager localTopologyManager) {
      this.stateConsumer = stateConsumer;
      this.stateProvider = stateProvider;
      this.cacheName = cache.getName();
      this.cacheNotifier = cacheNotifier;
      this.configuration = configuration;
      this.globalConfiguration = globalConfiguration;
      this.rpcManager = rpcManager;
      this.groupManager = groupManager;
      this.localTopologyManager = localTopologyManager;
   }

   // needs to be AFTER the DistributionManager and *after* the cache loader manager (if any) inits and preloads
   @Start(priority = 60)
   @Override
   public void start() throws Exception {
      if (trace) {
         log.tracef("Starting StateTransferManager of cache %s on node %s", cacheName, rpcManager.getAddress());
      }

      CacheJoinInfo joinInfo = new CacheJoinInfo(
            pickConsistentHashFactory(),
            configuration.clustering().hash().hash(),
            configuration.clustering().hash().numSegments(),
            configuration.clustering().hash().numOwners(),
            configuration.clustering().stateTransfer().timeout()
      );

      localTopologyManager.join(cacheName, joinInfo, new CacheTopologyHandler() {
         @Override
         public void updateConsistentHash(CacheTopology cacheTopology) {
            doTopologyUpdate(cacheTopology, false);
         }

         @Override
         public void rebalance(CacheTopology cacheTopology) {
            doTopologyUpdate(cacheTopology, true);
         }
      });
   }

   /**
    * If no ConsistentHashFactory was explicitly configured we choose a suitable one based on cache mode.
    */
   private ConsistentHashFactory pickConsistentHashFactory() {
      ConsistentHashFactory factory = configuration.clustering().hash().consistentHashFactory();
      if (factory == null) {
         CacheMode cacheMode = configuration.clustering().cacheMode();
         if (cacheMode.isClustered()) {
            if (cacheMode.isDistributed()) {
               if (globalConfiguration.transport().hasTopologyInfo()) {
                  factory = new TopologyAwareConsistentHashFactory();
               } else {
                  factory = new DefaultConsistentHashFactory();
               }
            } else {
               // this is also used for invalidation mode
               factory = new ReplicatedConsistentHashFactory();
            }
         }
      }
      return factory;
   }

   /**
    * Decorates the given cache topology to add key grouping. The ConsistentHash objects of the cache topology
    * are wrapped to provide key grouping (if configured).
    *
    * @param cacheTopology the given cache topology
    * @return the decorated topology
    */
   private CacheTopology addGrouping(CacheTopology cacheTopology) {
      if (groupManager == null) {
         return cacheTopology;
      }

      ConsistentHash currentCH = cacheTopology.getCurrentCH();
      currentCH = new GroupingConsistentHash(currentCH, groupManager);
      ConsistentHash pendingCH = cacheTopology.getPendingCH();
      if (pendingCH != null) {
         pendingCH = new GroupingConsistentHash(pendingCH, groupManager);
      }
      return new CacheTopology(cacheTopology.getTopologyId(), currentCH, pendingCH);
   }

   private void doTopologyUpdate(CacheTopology newCacheTopology, boolean isRebalance) {
      if (trace) {
         log.tracef("Installing new cache topology %s", newCacheTopology);
      }

      // handle grouping
      newCacheTopology = addGrouping(newCacheTopology);

      CacheTopology oldCacheTopology = stateConsumer.getCacheTopology();
      ConsistentHash oldCH = oldCacheTopology != null ? oldCacheTopology.getWriteConsistentHash() : null;
      ConsistentHash newCH = newCacheTopology.getWriteConsistentHash();

      cacheNotifier.notifyTopologyChanged(oldCH, newCH, true);

      stateConsumer.onTopologyUpdate(newCacheTopology, isRebalance);
      stateProvider.onTopologyUpdate(newCacheTopology, isRebalance);

      cacheNotifier.notifyTopologyChanged(oldCH, newCH, false);
   }

   @Stop(priority = 20)
   @Override
   public void stop() {
      if (trace) {
         log.tracef("Shutting down StateTransferManager of cache %s on node %s", cacheName, rpcManager.getAddress());
      }
      localTopologyManager.leave(cacheName);
   }

   @Override
   public boolean isJoinComplete() {
      return stateConsumer.getCacheTopology() != null; // TODO [anistor] this does not mean we have received a topology update or a rebalance yet
   }

   @Override
   public boolean isStateTransferInProgress() {
      // todo [anistor] this returns false until we receive the first rebalance and should actually return true if the cluster has > 1 member
      return stateConsumer.isStateTransferInProgress();
   }

   @Override
   public boolean isStateTransferInProgressForKey(Object key) {
      // todo [anistor] this returns false until we receive the first rebalance and should actually return true if the cluster has > 1 member
      return stateConsumer.isStateTransferInProgressForKey(key);
   }

   @Override
   public CacheTopology getCacheTopology() {
      return stateConsumer.getCacheTopology();
   }
}