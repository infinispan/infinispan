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
import org.infinispan.commands.CommandsFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.DataContainer;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.ch.DefaultConsistentHashFactory;
import org.infinispan.distribution.ch.ReplicatedConsistentHashFactory;
import org.infinispan.distribution.ch.TopologyAwareConsistentHashFactory;
import org.infinispan.distribution.group.GroupManager;
import org.infinispan.distribution.group.GroupingConsistentHash;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.topology.CacheJoinInfo;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.CacheTopologyHandler;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.ExecutorService;

import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;

/**
 * @author anistor@redhat.com
 * @since 5.2
 */
public class StateTransferManagerImpl implements StateTransferManager {

   private static final Log log = LogFactory.getLog(StateTransferManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   private StateTransferLock stateTransferLock;

   private Configuration configuration;
   private CacheNotifier cacheNotifier;
   private GlobalConfiguration globalConfiguration;
   private LocalTopologyManager localTopologyManager;
   private RpcManager rpcManager;
   private GroupManager groupManager;
   private String cacheName;

   private StateProvider stateProvider;
   private StateConsumer stateConsumer;
   private volatile boolean rebalanceInProgress;
   private volatile CacheTopology cacheTopology;

   public StateTransferManagerImpl() {
   }

   @Inject
   public void init(@ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService asyncTransportExecutor,   //todo [anistor] use a separate ExecutorService
                    CacheNotifier cacheNotifier,
                    Configuration configuration,
                    GlobalConfiguration globalConfiguration,
                    RpcManager rpcManager,
                    CommandsFactory commandsFactory,
                    CacheLoaderManager cacheLoaderManager,
                    DataContainer dataContainer,
                    InterceptorChain interceptorChain,
                    InvocationContextContainer icc,
                    TransactionTable transactionTable,
                    LocalTopologyManager localTopologyManager,
                    StateTransferLock stateTransferLock,
                    Cache cache,
                    GroupManager groupManager) {
      this.configuration = configuration;
      this.cacheNotifier = cacheNotifier;
      this.globalConfiguration = globalConfiguration;
      this.localTopologyManager = localTopologyManager;
      this.rpcManager = rpcManager;
      this.stateTransferLock = stateTransferLock;
      this.groupManager = groupManager;
      cacheName = cache.getName();

      stateProvider = new StateProviderImpl(
            asyncTransportExecutor,
            configuration,
            rpcManager,
            commandsFactory,
            cacheLoaderManager,
            dataContainer,
            transactionTable,
            stateTransferLock);

      stateConsumer = new StateConsumerImpl(
            this,
            cacheNotifier,
            interceptorChain,
            icc,
            configuration,
            rpcManager,
            commandsFactory,
            cacheLoaderManager,
            dataContainer,
            transactionTable,
            stateTransferLock);
   }

   // needs to be AFTER the DistributionManager and *after* the cache loader manager (if any) inits and preloads
   @Start(priority = 60)
   private void start() throws Exception {
      if (trace) {
         log.tracef("Starting state transfer manager on " + rpcManager.getAddress());
      }

      CacheJoinInfo joinInfo = new CacheJoinInfo(
            pickConsistentHashFactory(),
            configuration.clustering().hash().hash(),
            configuration.clustering().hash().numSegments(),
            configuration.clustering().hash().numOwners(),
            configuration.clustering().stateTransfer().timeout());

      CacheTopologyHandler handler = new CacheTopologyHandler() {

         @Override
         public void updateConsistentHash(CacheTopology cacheTopology) {
            doTopologyUpdate(cacheTopology, false);
         }

         @Override
         public void rebalance(CacheTopology cacheTopology) {
            doTopologyUpdate(cacheTopology, true);
         }

         private void doTopologyUpdate(CacheTopology newCacheTopology, boolean isRebalance) {
            if (trace) log.tracef("Installing new cache topology %s", newCacheTopology);

            // handle grouping
            if (groupManager != null) {
               ConsistentHash currentCH = cacheTopology.getCurrentCH();
               currentCH = new GroupingConsistentHash(currentCH, groupManager);
               ConsistentHash pendingCH = cacheTopology.getPendingCH();
               if (pendingCH != null) {
                  pendingCH = new GroupingConsistentHash(pendingCH, groupManager);
               }
               cacheTopology = new CacheTopology(cacheTopology.getTopologyId(), currentCH, pendingCH);
            }

            ConsistentHash oldCH = cacheTopology != null ? cacheTopology.getWriteConsistentHash() : null;
            ConsistentHash newCH = newCacheTopology.getWriteConsistentHash();

            rebalanceInProgress |= isRebalance;

            cacheNotifier.notifyTopologyChanged(oldCH, newCH, true);
            cacheTopology = newCacheTopology;
            cacheNotifier.notifyTopologyChanged(oldCH, newCH, false);

            onTopologyUpdate(newCacheTopology.getTopologyId(), newCacheTopology.getReadConsistentHash(), newCacheTopology.getWriteConsistentHash());
         }
      };

      localTopologyManager.join(cacheName, joinInfo, handler);
   }

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
               factory = new ReplicatedConsistentHashFactory();
            }
         }
      }
      return factory;
   }

   @Stop(priority = 20)
   public void stop() {
      localTopologyManager.leave(cacheName);
      stateProvider.shutdown();
      stateConsumer.shutdown();
   }

   @Override
   public StateProvider getStateProvider() {
      return stateProvider;
   }

   @Override
   public StateConsumer getStateConsumer() {
      return stateConsumer;
   }

   @Override
   public void onTopologyUpdate(int topologyId, ConsistentHash readCh, ConsistentHash writeCh) {
      stateProvider.onTopologyUpdate(topologyId, readCh, writeCh);
      stateConsumer.onTopologyUpdate(topologyId, readCh, writeCh);
   }

   @Override
   public boolean isJoinComplete() {
      return cacheTopology != null;
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
      return cacheTopology;
   }

   public void notifyEndOfStateTransfer(int topologyId) {
      if (rebalanceInProgress) {
         rebalanceInProgress = false;
         localTopologyManager.confirmRebalance(cacheName, topologyId, null);
      }
   }
}