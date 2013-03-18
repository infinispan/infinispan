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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.infinispan.CacheException;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
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
 * The {@code LocalTopologyManager} implementation.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class LocalTopologyManagerImpl implements LocalTopologyManager {
   private static Log log = LogFactory.getLog(LocalTopologyManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   private Transport transport;
   private ExecutorService asyncTransportExecutor;
   private GlobalComponentRegistry gcr;

   private final ConcurrentMap<String, LocalCacheStatus> runningCaches = ConcurrentMapFactory.makeConcurrentMap();
   private volatile boolean running;

   @Inject
   public void inject(Transport transport,
                      @ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService asyncTransportExecutor,
                      GlobalComponentRegistry gcr) {
      this.transport = transport;
      this.asyncTransportExecutor = asyncTransportExecutor;
      this.gcr = gcr;
   }

   // Arbitrary value, only need to start after JGroupsTransport
   @Start(priority = 100)
   public void start() {
      running = true;
   }

   // Need to stop before the JGroupsTransport
   @Stop(priority = 9)
   public void stop() {
      running = false;
   }

   @Override
   public CacheTopology join(String cacheName, CacheJoinInfo joinInfo, CacheTopologyHandler stm)
         throws Exception {
      log.debugf("Node %s joining cache %s", transport.getAddress(), cacheName);
      LocalCacheStatus cacheStatus = new LocalCacheStatus(joinInfo, stm);
      runningCaches.put(cacheName, cacheStatus);

      int viewId = transport.getViewId();
      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
            CacheTopologyControlCommand.Type.JOIN, transport.getAddress(), joinInfo, viewId);
      long timeout = joinInfo.getTimeout();
      long endTime = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
      while (true) {
         try {
            CacheTopology initialTopology = (CacheTopology) executeOnCoordinator(command, timeout);
            // if the current coordinator is shutting down, it will return a null CacheTopology
            if (initialTopology != null) {
               handleConsistentHashUpdate(cacheName, initialTopology, viewId);
               return initialTopology;
            }
         } catch (Exception e) {
            log.debugf(e, "Error sending join request for cache %s to coordinator", cacheName);
            if (endTime <= System.nanoTime()) {
               throw e;
            }
            // TODO Add some configuration for this, or use a fraction of state transfer timeout
            Thread.sleep(1000);
         }
      }
   }

   @Override
   public void leave(String cacheName) {
      log.debugf("Node %s leaving cache %s", transport.getAddress(), cacheName);
      LocalCacheStatus cacheStatus = runningCaches.remove(cacheName);

      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
            CacheTopologyControlCommand.Type.LEAVE, transport.getAddress(), transport.getViewId());
      try {
         executeOnCoordinator(command, cacheStatus.getJoinInfo().getTimeout());
      } catch (Exception e) {
         log.debugf(e, "Error sending the leave request for cache %s to coordinator", cacheName);
      }
   }

   @Override
   public void confirmRebalance(String cacheName, int topologyId, Throwable throwable) {
      // Note that if the coordinator changes again after we sent the command, we will get another
      // query for the status of our running caches. So we don't need to retry if the command failed.
      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
            CacheTopologyControlCommand.Type.REBALANCE_CONFIRM, transport.getAddress(),
            topologyId, throwable, transport.getViewId());
      try {
         executeOnCoordinatorAsync(command);
      } catch (Exception e) {
         log.debugf(e, "Error sending the rebalance completed notification for cache %s to the coordinator",
               cacheName);
      }
   }

   // called by the coordinator
   @Override
   public Map<String, Object[]> handleStatusRequest(int viewId) {
      Map<String, Object[]> response = new HashMap<String, Object[]>();
      for (Map.Entry<String, LocalCacheStatus> e : runningCaches.entrySet()) {
         String cacheName = e.getKey();
         LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
         response.put(e.getKey(), new Object[]{cacheStatus.getJoinInfo(), cacheStatus.getTopology()});
      }
      return response;
   }

   @Override
   public void handleConsistentHashUpdate(String cacheName, CacheTopology cacheTopology, int viewId) throws InterruptedException {
      if (!running) {
         log.debugf("Ignoring consistent hash update %s for cache %s, the local cache manager is not running",
               cacheTopology.getTopologyId(), cacheName);
         return;
      }
      waitForView(viewId);

      LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
      if (cacheStatus == null) {
         log.tracef("Ignoring consistent hash update %s for cache %s that doesn't exist locally",
               cacheTopology.getTopologyId(), cacheName);
         return;
      }

      synchronized (cacheStatus) {
         CacheTopology existingTopology = cacheStatus.getTopology();
         if (existingTopology != null && cacheTopology.getTopologyId() < existingTopology.getTopologyId()){
            log.tracef("Ignoring consistent hash update %s for cache %s, we have already received a newer topology %s",
                  cacheTopology.getTopologyId(), cacheName, existingTopology.getTopologyId());
            return;
         }

         log.debugf("Updating local consistent hash(es) for cache %s: new topology = %s", cacheName, cacheTopology);
         cacheStatus.setTopology(cacheTopology);
         ConsistentHash unionCH = null;
         if (cacheTopology.getPendingCH() != null) {
            unionCH = cacheStatus.getJoinInfo().getConsistentHashFactory().union(cacheTopology.getCurrentCH(),
                  cacheTopology.getPendingCH());
         }

         CacheTopologyHandler handler = cacheStatus.getHandler();
         CacheTopology unionTopology = new CacheTopology(cacheTopology.getTopologyId(),
               cacheTopology.getCurrentCH(), unionCH);
         unionTopology.logRoutingTableInformation();
         if ((existingTopology == null || existingTopology.getPendingCH() == null) && unionCH != null) {
            // This CH_UPDATE command was sent after a REBALANCE_START command, but arrived first.
            // We will start the rebalance now and ignore the REBALANCE_START command when it arrives.
            log.tracef("This topology update has a pending CH, starting the rebalance now");
            handler.rebalance(unionTopology);
         } else {
            handler.updateConsistentHash(unionTopology);
         }
      }
   }

   @Override
   public void handleRebalance(String cacheName, CacheTopology cacheTopology, int viewId) throws InterruptedException {
      if (!running) {
         log.debugf("Ignoring rebalance request %s for cache %s, the local cache manager is not running",
               cacheTopology.getTopologyId(), cacheName);
         return;
      }
      waitForView(viewId);

      LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
      if (cacheStatus == null) {
         log.tracef("Ignoring rebalance %s for cache %s that doesn't exist locally",
               cacheTopology.getTopologyId(), cacheName);
         return;
      }

      synchronized (cacheStatus) {
         CacheTopology existingTopology = cacheStatus.getTopology();
         if (existingTopology != null && cacheTopology.getTopologyId() < existingTopology.getTopologyId()) {
            // Start rebalance commands are sent asynchronously to the entire cluster
            // So it's possible to receive an old one on a joiner after the joiner has already become a member.
            log.debugf("Ignoring old rebalance for cache %s: %s", cacheName, cacheTopology.getTopologyId());
            return;
         }

         log.debugf("Starting local rebalance for cache %s, topology = %s", cacheName, cacheTopology);
         cacheTopology.logRoutingTableInformation();
         cacheStatus.setTopology(cacheTopology);

         ConsistentHash unionCH = cacheStatus.getJoinInfo().getConsistentHashFactory().union(
               cacheTopology.getCurrentCH(), cacheTopology.getPendingCH());
         CacheTopologyHandler handler = cacheStatus.getHandler();
         handler.rebalance(new CacheTopology(cacheTopology.getTopologyId(), cacheTopology.getCurrentCH(), unionCH));
      }
   }

   @Override
   public CacheTopology getCacheTopology(String cacheName) {
      LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
      return cacheStatus.getTopology();
   }

   private void waitForView(int viewId) throws InterruptedException {
      if (transport.getViewId() < viewId) {
         log.tracef("Received a cache topology command with a higher view id: %s, our view id is %s", viewId,
               transport.getViewId());
      }
      while (transport.getViewId() < viewId) {
         Thread.sleep(100);
      }
   }

   private Object executeOnCoordinator(ReplicableCommand command, long timeout) throws Exception {
      Response response;
      if (transport.isCoordinator()) {
         try {
            gcr.wireDependencies(command);
            response = (Response) command.perform(null);
         } catch (Throwable t) {
            throw new CacheException("Error handling join request", t);
         }
      } else {
         // this node is not the coordinator
         Address coordinator = transport.getCoordinator();
         Map<Address, Response> responseMap = transport.invokeRemotely(Collections.singleton(coordinator),
               command, ResponseMode.SYNCHRONOUS, timeout, true, null, false, false);
         response = responseMap.get(coordinator);
      }
      if (response == null || !response.isSuccessful()) {
         Throwable exception = response instanceof ExceptionResponse
               ? ((ExceptionResponse)response).getException() : null;
         throw new CacheException("Bad response received from coordinator: " + response, exception);
      }
      return ((SuccessfulResponse) response).getResponseValue();
   }

   private void executeOnCoordinatorAsync(final ReplicableCommand command) throws Exception {
      // if we are the coordinator, the execution is actually synchronous
      if (transport.isCoordinator()) {
         asyncTransportExecutor.submit(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
               gcr.wireDependencies(command);
               try {
                  return command.perform(null);
               } catch (Throwable t) {
                  log.errorf(t, "Failed to execute ReplicableCommand %s on coordinator async: %s", command, t.getMessage());
                  throw new Exception(t);
               }
            }
         });
      } else {
         Address coordinator = transport.getCoordinator();
         // ignore the responses
         transport.invokeRemotely(Collections.singleton(coordinator), command, ResponseMode.ASYNCHRONOUS_WITH_SYNC_MARSHALLING, 0, true, null, false, false);
      }
   }

}

class LocalCacheStatus {
   private final CacheJoinInfo joinInfo;
   private final CacheTopologyHandler handler;
   private volatile CacheTopology topology;

   public LocalCacheStatus(CacheJoinInfo joinInfo, CacheTopologyHandler handler) {
      this.joinInfo = joinInfo;
      this.handler = handler;
   }

   public CacheJoinInfo getJoinInfo() {
      return joinInfo;
   }

   public CacheTopologyHandler getHandler() {
      return handler;
   }

   public CacheTopology getTopology() {
      return topology;
   }

   public void setTopology(CacheTopology topology) {
      this.topology = topology;
   }
}
