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
import org.infinispan.remoting.responses.ExceptionResponse;
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
   private volatile boolean isCoordinator;
   private volatile boolean isShuttingDown;
   private volatile int viewId = -1;
   private final Object viewUpdateLock = new Object();


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
      isShuttingDown = false;
      isCoordinator = transport.isCoordinator();

      listener = new ClusterViewListener();
      cacheManagerNotifier.addListener(listener);
      // The listener already missed the initial view
      handleNewView(transport.getMembers(), false, transport.getViewId());
   }

   @Stop(priority = 100)
   public void stop() {
      isShuttingDown = true;
      cacheManagerNotifier.removeListener(listener);

      // Stop blocking cache topology commands.
      // The synchronization also ensures that the listener has finished executing
      // so we don't get InterruptedExceptions when the notification thread pool shuts down
      synchronized (viewUpdateLock) {
         viewId = Integer.MAX_VALUE;
         viewUpdateLock.notifyAll();
      }

   }

   @Override
   public void updateConsistentHash(String cacheName, CacheTopology cacheTopology) throws Exception {
      log.debugf("Updating cluster-wide consistent hash for cache %s, topology = %s",
            cacheName, cacheTopology);
      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
            CacheTopologyControlCommand.Type.CH_UPDATE, transport.getAddress(), cacheTopology,
            transport.getViewId());
      executeOnClusterSync(command, getGlobalTimeout());

      RebalanceInfo rebalanceInfo = rebalanceStatusMap.get(cacheName);
      if (rebalanceInfo != null) {
         List<Address> members = cacheTopology.getMembers();
         if (rebalanceInfo.updateMembers(members)) {
            // all the nodes that haven't confirmed yet have left the cache/cluster
            onClusterRebalanceCompleted(cacheName, cacheTopology.getTopologyId(), rebalanceInfo);
         }
      }
   }

   private void onClusterRebalanceCompleted(String cacheName, int topologyId, RebalanceInfo rebalanceInfo) throws Exception {
      log.debugf("Removing rebalance information for topology id %d", topologyId);
      rebalanceStatusMap.remove(cacheName);
      rebalancePolicy.onRebalanceCompleted(cacheName, topologyId);
   }

   @Override
   public void rebalance(String cacheName, CacheTopology cacheTopology) throws Exception {
      log.debugf("Starting cluster-wide rebalance for cache %s, topology = %s", cacheName, cacheTopology);
      int topologyId = cacheTopology.getTopologyId();
      Collection<Address> members = cacheTopology.getPendingCH().getMembers();
      RebalanceInfo existingRebalance = rebalanceStatusMap.putIfAbsent(cacheName,
            new RebalanceInfo(cacheName, topologyId, members));
      if (existingRebalance != null) {
         throw new IllegalStateException("Aborting the current rebalance, there is another operation " +
               "in progress: " + existingRebalance);
      }
      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
            CacheTopologyControlCommand.Type.REBALANCE_START, transport.getAddress(), cacheTopology,
            viewId);
      executeOnClusterAsync(command);
   }

   @Override
   public CacheTopology handleJoin(String cacheName, Address joiner, CacheJoinInfo joinInfo, int viewId) throws Exception {
      waitForView(viewId);
      if (isShuttingDown) {
         log.debugf("Ignoring join request from %s for cache %s, the local cache manager is shutting down",
               joiner, cacheName);
         return null;
      }
      rebalancePolicy.initCache(cacheName, joinInfo);
      return rebalancePolicy.addJoiners(cacheName, Collections.singletonList(joiner));
   }

   @Override
   public void handleLeave(String cacheName, Address leaver, int viewId) throws Exception {
      if (isShuttingDown) {
         log.debugf("Ignoring leave request from %s for cache %s, the local cache manager is shutting down",
               leaver, cacheName);
         return;
      }
      rebalancePolicy.removeLeavers(cacheName, Collections.singletonList(leaver));
   }

   @Override
   public void handleRebalanceCompleted(String cacheName, Address node, int topologyId, Throwable throwable, int viewId) throws Exception {
      if (throwable != null) {
         // TODO We could try to update the pending CH such that nodes reporting errors are not considered to hold any state
         // For now we are just logging the error and proceeding as if the rebalance was successful everywhere
         log.rebalanceError(cacheName, node, throwable);
      }
      log.debugf("Finished local rebalance for cache %s on node %s, topology id = %d", cacheName, node,
            topologyId);
      RebalanceInfo rebalanceInfo = rebalanceStatusMap.get(cacheName);
      if (rebalanceInfo == null) {
         throw new CacheException(String.format("Received invalid rebalance confirmation from %s " +
               "for cache %s, we don't have a rebalance in progress", node, cacheName));
      }

      if (rebalanceInfo.confirmRebalance(node, topologyId)) {
         onClusterRebalanceCompleted(cacheName, topologyId, rebalanceInfo);
      }
   }

   private void waitForView(int viewId) throws InterruptedException {
      if (this.viewId < viewId) {
         log.tracef("Received a cache topology command with a higher view id: %s, our view id is %s", viewId, this.viewId);
      }
      synchronized (viewUpdateLock) {
         while (this.viewId < viewId) {
            viewUpdateLock.wait(1000);
         }
      }
   }

   private Map<Address, Object> executeOnClusterSync(final ReplicableCommand command, final int timeout)
         throws Exception {
      // first invoke remotely
      Future<Map<Address, Response>> remoteFuture = asyncTransportExecutor.submit(new Callable<Map<Address, Response>>() {
         @Override
         public Map<Address, Response> call() throws Exception {
            return transport.invokeRemotely(null, command,
                  ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, timeout, true, null);
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
            Throwable cause = response instanceof ExceptionResponse ? ((ExceptionResponse) response).getException() : null;
            throw new CacheException("Unsuccessful response received from node " + address + ": " + response, cause);
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

   private void executeOnClusterAsync(final ReplicableCommand command)
         throws Exception {
      transport.invokeRemotely(null, command, ResponseMode.ASYNCHRONOUS_WITH_SYNC_MARSHALLING, -1, true, null);

      asyncTransportExecutor.submit(new Callable<Object>() {
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
   }


   // need to recover existing caches asynchronously (in case we just became the coordinator)
   @Listener(sync = false)
   public class ClusterViewListener {
      @Merged
      @ViewChanged
      public void handleViewChange(final ViewChangedEvent e) {
         handleNewView(e.getNewMembers(), e.isMergeView(), e.getViewId());
      }
   }

   private void handleNewView(List<Address> newMembers, boolean mergeView, int newViewId) {
      // check to ensure this is not an older view
      if (newViewId <= viewId) {
         log.tracef("Ignoring old cluster view notification: %s", newViewId);
         return;
      }

      log.tracef("Received new cluster view: %s", newViewId);
      boolean becameCoordinator = !isCoordinator && transport.isCoordinator();
      isCoordinator = transport.isCoordinator();

      if (mergeView || becameCoordinator) {
         try {
            Map<String, List<CacheTopology>> clusterCacheMap = recoverClusterStatus();

            for (Map.Entry<String, List<CacheTopology>> e : clusterCacheMap.entrySet()) {
               String cacheName = e.getKey();
               List<CacheTopology> topologyList = e.getValue();
               rebalancePolicy.initCache(cacheName, topologyList);
            }
         } catch (InterruptedException e) {
            log.tracef("Cluster state recovery interrupted because the coordinator is shutting down");
            // the CTMI has already stopped, no need to update the view id or notify waiters
            return;
         } catch (Exception e) {
            // TODO Retry?
            log.failedToRecoverClusterState(e);
         }
      } else if (isCoordinator) {
         try {
            rebalancePolicy.updateMembersList(newMembers);
         } catch (Exception e) {
            log.errorUpdatingMembersList(e);
         }
      }

      synchronized (viewUpdateLock) {
         // update the view id last, so join requests from other nodes wait until we recovered existing members' info
         viewId = newViewId;
         viewUpdateLock.notifyAll();
      }
   }

   private HashMap<String, List<CacheTopology>> recoverClusterStatus() throws Exception {
      log.debugf("Recovering running caches in the cluster");
      ReplicableCommand command = new CacheTopologyControlCommand(null,
         CacheTopologyControlCommand.Type.GET_STATUS, transport.getAddress(), transport.getViewId());
      Map<Address, Object> statusResponses = executeOnClusterSync(command, getGlobalTimeout());

      HashMap<String, List<CacheTopology>> clusterCacheMap = new HashMap<String, List<CacheTopology>>();
      for (Object o : statusResponses.values()) {
         Map<String, Object[]> nodeStatus = (Map<String, Object[]>) o;
         for (Map.Entry<String, Object[]> e : nodeStatus.entrySet()) {
            String cacheName = e.getKey();
            CacheJoinInfo joinInfo = (CacheJoinInfo) e.getValue()[0];
            CacheTopology cacheTopology = (CacheTopology) e.getValue()[1];

            List<CacheTopology> topologyList = clusterCacheMap.get(cacheName);
            if (topologyList == null) {
               // this is the first CacheJoinInfo we got for this cache
               rebalancePolicy.initCache(cacheName, joinInfo);

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
      private final String cacheName;
      private final int topologyId;
      private final Set<Address> confirmationsNeeded;

      public RebalanceInfo(String cacheName, int topologyId, Collection<Address> members) {
         this.cacheName = cacheName;
         this.topologyId = topologyId;
         this.confirmationsNeeded = new HashSet<Address>(members);
         log.tracef("Initialized rebalance confirmation collector %d, initial list is %s", topologyId, confirmationsNeeded);
      }

      /**
       * @return {@code true} if everyone has confirmed
       */
      public boolean confirmRebalance(Address node, int receivedTopologyId) {
         synchronized (this) {
            if (topologyId != receivedTopologyId) {
               throw new CacheException(String.format("Received invalid rebalance confirmation from %s " +
                     "for cache %s, expecting topology id %d but got %d", node, cacheName, topologyId, receivedTopologyId));
            }

            boolean removed = confirmationsNeeded.remove(node);
            if (!removed) {
               log.tracef("Rebalance confirmation collector %d ignored confirmation for %s, which is not a member",
                     topologyId, node);
               return false;
            }

            log.tracef("Rebalance confirmation collector %d received confirmation for %s, remaining list is %s",
                  topologyId, node, confirmationsNeeded);
            return confirmationsNeeded.isEmpty();
         }
      }

      /**
       * @return {@code true} if everyone has confirmed
       */
      public boolean updateMembers(Collection<Address> newMembers) {
         synchronized (this) {
            // only return true the first time
            boolean modified = confirmationsNeeded.retainAll(newMembers);
            return modified && confirmationsNeeded.isEmpty();
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