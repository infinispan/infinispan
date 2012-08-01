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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.CacheException;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.concurrent.ConcurrentMapFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;

/**
 * The {@code ClusterTopologyManager} implementation.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class ClusterTopologyManagerImpl implements ClusterTopologyManager {
   private static Log log = LogFactory.getLog(ClusterTopologyManagerImpl.class);

   private Transport transport;
   private RebalancePolicy rebalancePolicy;
   private GlobalConfiguration globalConfiguration;
   private GlobalComponentRegistry gcr;
   private CacheManagerNotifier cacheManagerNotifier;
   private ExecutorService asyncTransportExecutor;
   private boolean isCoordinator;

   //private ConcurrentMap<String, CacheJoinInfo> clusterCaches = ConcurrentMapFactory.makeConcurrentMap();
   private final ConcurrentMap<String, RebalanceInfo> rebalanceStatusMap = ConcurrentMapFactory.makeConcurrentMap();
   private ClusterTopologyManagerImpl.ClusterViewListener listener;

   @Inject
   public void inject(Transport transport, RebalancePolicy rebalancePolicy,
                      @ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService asyncTransportExecutor,
                      GlobalConfiguration globalConfiguration, GlobalComponentRegistry gcr,
                      CacheManagerNotifier cacheManagerNotifier) {
      this.transport = transport;
      this.rebalancePolicy = rebalancePolicy;
      this.asyncTransportExecutor = asyncTransportExecutor;
      this.globalConfiguration = globalConfiguration;
      this.gcr = gcr;
      this.cacheManagerNotifier = cacheManagerNotifier;
   }

   @Start(priority = 100)
   public void start() {
      this.isCoordinator = transport.isCoordinator();

      listener = new ClusterViewListener();
      cacheManagerNotifier.addListener(listener);
      // The listener already missed the initial view
      handleNewView(transport.getMembers(), false);
   }

   @Stop(priority = 100)
   public void stop() {
      cacheManagerNotifier.removeListener(listener);
   }

   @Override
   public void updateConsistentHash(String cacheName, int topologyId, ConsistentHash currentCH,
                                    ConsistentHash pendingCH) throws Exception {
      log.debugf("Updating cluster-wide consistent hash for cache %s, topology id = %d, currentCH = %s, pendingCH = %s",
            cacheName, topologyId, currentCH, pendingCH);
      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
            CacheTopologyControlCommand.Type.CH_UPDATE, transport.getAddress(), topologyId, currentCH, pendingCH);
      executeOnClusterSync(command, getGlobalTimeout());

      RebalanceInfo rebalanceInfo = rebalanceStatusMap.get(cacheName);
      if (rebalanceInfo != null) {
         List<Address> members = pendingCH != null ? pendingCH.getMembers() : currentCH.getMembers();
         if (rebalanceInfo.updateMembers(members)) {
            // all the nodes that haven't confirmed yet have left the cache/cluster
            onRebalanceCompleted(cacheName, topologyId, rebalanceInfo);
         }
      }
   }

   private void onRebalanceCompleted(String cacheName, int topologyId, RebalanceInfo rebalanceInfo) throws Exception {
      rebalancePolicy.onRebalanceCompleted(cacheName, topologyId);
      log.debugf("Removing rebalance information for topology id %d", topologyId);
      rebalanceStatusMap.remove(cacheName);
   }

   @Override
   public void rebalance(String cacheName, int topologyId, ConsistentHash currentCH, ConsistentHash pendingCH) throws Exception {
      log.debugf("Starting cluster-wide rebalance for cache %s, topology id = %s, new CH = %s",
            cacheName, topologyId, pendingCH);
      RebalanceInfo existingRebalance = rebalanceStatusMap.putIfAbsent(cacheName, new RebalanceInfo(topologyId, pendingCH.getMembers()));
      if (existingRebalance != null) {
         throw new IllegalStateException("Aborting the current rebalance, there is another operation in progress: " + existingRebalance);
      }
      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
            CacheTopologyControlCommand.Type.REBALANCE_START, transport.getAddress(), topologyId, currentCH, pendingCH);
      executeOnClusterAsync(command);
   }

   @Override
   public CacheTopology handleJoin(String cacheName, Address joiner, CacheJoinInfo joinInfo) throws Exception {
      rebalancePolicy.initCache(cacheName, joinInfo);
      rebalancePolicy.updateMembersList(cacheName, Collections.singletonList(joiner), Collections.<Address>emptyList());
      return rebalancePolicy.getTopology(cacheName);
   }

   @Override
   public void handleLeave(String cacheName, Address leaver) throws Exception {
      rebalancePolicy.updateMembersList(cacheName, Collections.<Address>emptyList(), Collections.singletonList(leaver));
   }

   @Override
   public void handleRebalanceCompleted(String cacheName, Address node, int topologyId) throws Exception {
      // TODO Don't send a rebalance confirmation if the node is not part of the new ch
      // TODO Check for stale/invalid rebalance confirmations here
      log.debugf("Finished local rebalance for cache %s on node %s, topology id = %d", cacheName, node,
            topologyId);
      RebalanceInfo rebalanceInfo = rebalanceStatusMap.get(cacheName);
      if (rebalanceInfo == null || topologyId != rebalanceInfo.topologyId) {
         throw new CacheException(String.format("%s: Received invalid rebalance confirmation from %s for " +
               "rebalance status is %s", cacheName, node, rebalanceInfo));
      }

      if (rebalanceInfo.confirmRebalance(node)) {
         onRebalanceCompleted(cacheName, topologyId, rebalanceInfo);
      }
   }

   private Map<Address, Object> executeOnClusterSync(final ReplicableCommand command, final int timeout)
         throws Exception {
      // first invoke remotely
      Future<Map<Address, Response>> remoteFuture = asyncTransportExecutor.submit(new Callable<Map<Address, Response>>() {
         @Override
         public Map<Address, Response> call() throws Exception {
            Map<Address, Response> rspList = transport.invokeRemotely(null, command,
                  ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, timeout, false, null);
            return rspList;
         }
      });

      // now invoke the command on the local node
      Future<Object> localFuture = asyncTransportExecutor.submit(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            gcr.wireDependencies(command);
            try {
               return command.perform(null);
            } catch (Throwable t) {
               throw new Exception(t);
            }
         }
      });

      // wait for the remote commands to finish
      Map<Address, Response> responseMap = remoteFuture.get(timeout, TimeUnit.MILLISECONDS);

      // parse the responses
      Map<Address, Object> responseValues = new HashMap<Address, Object>(transport.getMembers().size());
      for (Map.Entry<Address, Response> entry : responseMap.entrySet()) {
         Address address = entry.getKey();
         Response response = entry.getValue();
         if (!response.isSuccessful()) {
            throw new CacheException("Unsuccessful response received from node " + address + ": " + response);
         }
         responseValues.put(address, ((SuccessfulResponse) response).getResponseValue());
      }

      // now wait for the local command
      Response localResponse = (Response) localFuture.get(timeout, TimeUnit.MILLISECONDS);
      if (!localResponse.isSuccessful()) {
         throw new CacheException("Unsuccessful local response");
      }
      responseValues.put(transport.getAddress(), ((SuccessfulResponse) localResponse).getResponseValue());

      return responseValues;
   }

   private void executeOnClusterAsync(ReplicableCommand command)
         throws Exception {
      transport.invokeRemotely(null, command, ResponseMode.ASYNCHRONOUS_WITH_SYNC_MARSHALLING, -1, false, null);

      gcr.wireDependencies(command);
      try {
         command.perform(null);
      } catch (Throwable throwable) {
         log.errorf("Error executing command %s on local node", command);
      }
   }


   // need to recover existing caches asynchronously (in case we just became the coordinator)
   @Listener(sync = false)
   public class ClusterViewListener {
      @Merged
      @ViewChanged
      public void handleViewChange(final ViewChangedEvent e) {
         handleNewView(e.getNewMembers(), e.isMergeView());
      }
   }

   private void handleNewView(List<Address> newMembers, boolean mergeView) {
      boolean becameCoordinator = !isCoordinator && transport.isCoordinator();
      if (mergeView || becameCoordinator) {
         try {
            HashMap<String, List<CacheTopology>> clusterCacheMap = recoverClusterStatus();

            for (Map.Entry<String, List<CacheTopology>> e : clusterCacheMap.entrySet()) {
               String cacheName = e.getKey();
               List<CacheTopology> topologyList = e.getValue();
               rebalancePolicy.initCache(cacheName, topologyList);
            }
         } catch (Exception e) {
            //TODO log.errorRecoveringClusterState(e);
            log.errorf(e, "Error recovering cluster state");
         }
      } else {
         try {
            rebalancePolicy.updateMembersList(newMembers);
         } catch (Exception e) {
            log.errorf(e, "Error updating the cluster member list");
         }
      }
   }

   private HashMap<String, List<CacheTopology>> recoverClusterStatus() throws Exception {
      log.debugf("Recovering running caches in the cluster");
      ReplicableCommand command = new CacheTopologyControlCommand(null,
         CacheTopologyControlCommand.Type.GET_STATUS, transport.getAddress());
      Map<Address, Object> statusResponses = executeOnClusterSync(command, getGlobalTimeout());

      HashMap<String, List<CacheTopology>> clusterCacheMap = new HashMap<String, List<CacheTopology>>();
      for (Object o : statusResponses.values()) {
         Map<String, CacheTopology> nodeStatus = (Map<String, CacheTopology>) o;
         for (Map.Entry<String, CacheTopology> e : nodeStatus.entrySet()) {
            String cacheName = e.getKey();
            CacheTopology cacheTopology = e.getValue();
            List<CacheTopology> topologyList = clusterCacheMap.get(cacheName);
            if (topologyList == null) {
               topologyList = new ArrayList<CacheTopology>();
               clusterCacheMap.put(cacheName, topologyList);
            }
            topologyList.add(cacheTopology);
         }
      }
      return clusterCacheMap;
   }

   private int getGlobalTimeout() {
      // TODO Rename setting to something like globalRpcTimeout
      return (int) globalConfiguration.transport().distributedSyncTimeout();
   }


   private static class RebalanceInfo {
      private final int topologyId;
      private final Set<Address> confirmationsNeeded;

      public RebalanceInfo(int topologyId, Collection<Address> members) {
         this.topologyId = topologyId;
         this.confirmationsNeeded = new HashSet<Address>(members);
      }

      /**
       * @return {@code true} if everyone has confirmed
       */
      public boolean confirmRebalance(Address node) {
         synchronized (this) {
            confirmationsNeeded.remove(node);
            return confirmationsNeeded.isEmpty();
         }
      }

      /**
       * @return {@code true} if everyone has confirmed
       */
      public boolean updateMembers(Collection<Address> newMembers) {
         synchronized (this) {
            // only return true the first time
            if (confirmationsNeeded.isEmpty())
               return false;

            confirmationsNeeded.retainAll(newMembers);
            return confirmationsNeeded.isEmpty();
         }
      }

      @Override
      public String toString() {
         return "RebalanceInfo{" +
               "topologyId=" + topologyId +
               ", confirmationsNeeded=" + confirmationsNeeded +
               '}';
      }
   }
}