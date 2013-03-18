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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.distribution.ch.*;
import org.infinispan.distribution.group.GroupManager;
import org.infinispan.distribution.group.GroupingConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheJoinInfo;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.CacheTopologyHandler;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * {@link StateTransferManager} implementation.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@MBean(objectName = "StateTransferManager", description = "Component that handles state transfer")
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

   private final CountDownLatch initialStateTransferComplete = new CountDownLatch(1);

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
            configuration.clustering().stateTransfer().timeout(),
            configuration.transaction().transactionProtocol().isTotalOrder(),
            configuration.clustering().cacheMode().isDistributed());

      CacheTopology initialTopology = localTopologyManager.join(cacheName, joinInfo, new CacheTopologyHandler() {
         @Override
         public void updateConsistentHash(CacheTopology cacheTopology) {
            doTopologyUpdate(cacheTopology, false);
         }

         @Override
         public void rebalance(CacheTopology cacheTopology) {
            doTopologyUpdate(cacheTopology, true);
         }
      });

      if (trace) {
         log.tracef("StateTransferManager of cache %s on node %s received initial topology %s", cacheName, rpcManager.getAddress(), initialTopology);
      }
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
         log.tracef("Installing new cache topology %s on cache %s", newCacheTopology, cacheName);
      }

      // handle grouping
      newCacheTopology = addGrouping(newCacheTopology);

      CacheTopology oldCacheTopology = stateConsumer.getCacheTopology();

      if (oldCacheTopology != null && oldCacheTopology.getTopologyId() > newCacheTopology.getTopologyId()) {
         throw new IllegalStateException("Old topology is higher: old=" + oldCacheTopology + ", new=" + newCacheTopology);
      }

      ConsistentHash oldCH = oldCacheTopology != null ? oldCacheTopology.getWriteConsistentHash() : null;
      ConsistentHash newCH = newCacheTopology.getWriteConsistentHash();

      // TODO Improve notification to contain both CHs
      cacheNotifier.notifyTopologyChanged(oldCH, newCH, newCacheTopology.getTopologyId(), true);

      stateConsumer.onTopologyUpdate(newCacheTopology, isRebalance);
      stateProvider.onTopologyUpdate(newCacheTopology, isRebalance);

      cacheNotifier.notifyTopologyChanged(oldCH, newCH, newCacheTopology.getTopologyId(), false);

      if (initialStateTransferComplete.getCount() > 0) {
         boolean isJoined = stateConsumer.getCacheTopology().getReadConsistentHash().getMembers().contains(rpcManager.getAddress());
         if (isJoined) {
            initialStateTransferComplete.countDown();
            log.tracef("Initial state transfer complete for cache %s on node %s", cacheName, rpcManager.getAddress());
         }
      }
   }

   @Start(priority = 1000)
   @SuppressWarnings("unused")
   public void waitForInitialStateTransferToComplete() throws InterruptedException {
      if (configuration.clustering().stateTransfer().awaitInitialTransfer()) {
         if (trace) log.tracef("Waiting for initial state transfer to finish for cache %s on %s", cacheName, rpcManager.getAddress());
         boolean success = initialStateTransferComplete.await(configuration.clustering().stateTransfer().timeout(), TimeUnit.MILLISECONDS);
         if (!success) {
            throw new CacheException(String.format("Initial state transfer timed out for cache %s on %s",
                  cacheName, rpcManager.getAddress()));
         }
      }
   }

   @Stop(priority = 20)
   @Override
   public void stop() {
      if (trace) {
         log.tracef("Shutting down StateTransferManager of cache %s on node %s", cacheName, rpcManager.getAddress());
      }
      initialStateTransferComplete.countDown();
      localTopologyManager.leave(cacheName);
   }

   @Override
   public boolean isJoinComplete() {
      return stateConsumer.getCacheTopology() != null; // TODO [anistor] this does not mean we have received a topology update or a rebalance yet
   }

   @Override
   public boolean isStateTransferInProgress() {
      return stateConsumer.isStateTransferInProgress();
   }

   @Override
   public boolean isStateTransferInProgressForKey(Object key) {
      return stateConsumer.isStateTransferInProgressForKey(key);
   }

   @Override
   public CacheTopology getCacheTopology() {
      return stateConsumer.getCacheTopology();
   }

   @Override
   public void forwardCommandIfNeeded(TopologyAffectedCommand command, Set<Object> affectedKeys, Address origin, boolean sync) {
      int cmdTopologyId = command.getTopologyId();
      // forward commands with older topology ids to their new targets
      // but we need to make sure we have the latest topology
      CacheTopology cacheTopology = getCacheTopology();
      int localTopologyId = cacheTopology != null ? cacheTopology.getTopologyId() : -1;
      // if it's a tx/lock/write command, forward it to the new owners
      log.tracef("CommandTopologyId=%s, localTopologyId=%s", cmdTopologyId, localTopologyId);

      if (cmdTopologyId < localTopologyId) {
         ConsistentHash writeCh = cacheTopology.getWriteConsistentHash();
         Set<Address> newTargets = new HashSet<Address>(writeCh.locateAllOwners(affectedKeys));
         newTargets.remove(rpcManager.getAddress());
         // Forwarding to the originator would create a cycle
         // TODO This may not be the "real" originator, but one of the original recipients
         // or even one of the nodes that one of the original recipients forwarded the command to.
         // In non-transactional caches, the "real" originator keeps a lock for the duration
         // of the RPC, so this means we could get a deadlock while forwarding to it.
         newTargets.remove(origin);
         if (!newTargets.isEmpty()) {
            // Update the topology id to prevent cycles
            command.setTopologyId(localTopologyId);
            log.tracef("Forwarding command %s to new targets %s", command, newTargets);
            // TODO find a way to forward the command async if it was received async
            rpcManager.invokeRemotely(newTargets, command, rpcManager.getDefaultRpcOptions(sync));
         }
      }
   }

   @Override
   public void notifyEndOfRebalance(int topologyId) {
      localTopologyManager.confirmRebalance(cacheName, topologyId, null);
   }

   @Override
   public boolean ownsData() {
      return stateConsumer.ownsData();
   }
}
