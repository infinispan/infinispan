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

   private Transport transport;
   private ExecutorService asyncTransportExecutor;
   private GlobalComponentRegistry gcr;

   private ConcurrentMap<String, LocalCacheStatus> runningCaches = ConcurrentMapFactory.makeConcurrentMap();

   @Inject
   public void inject(Transport transport,
                      @ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService asyncTransportExecutor,
                      GlobalComponentRegistry gcr) {
      this.transport = transport;
      this.asyncTransportExecutor = asyncTransportExecutor;
      this.gcr = gcr;
   }

   @Override
   public CacheTopology join(String cacheName, CacheJoinInfo joinInfo, CacheTopologyHandler stm)
         throws Exception {
      log.debugf("Node %s joining cache %s", transport.getAddress(), cacheName);
      LocalCacheStatus cacheStatus = new LocalCacheStatus(joinInfo, stm);
      runningCaches.put(cacheName, cacheStatus);

      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
            CacheTopologyControlCommand.Type.JOIN, transport.getAddress(), joinInfo);
      long timeout = joinInfo.getTimeout();
      long endTime = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
      while (true) {
         try {
            CacheTopology initialTopology = (CacheTopology) executeOnCoordinator(command, timeout);
            handleConsistentHashUpdate(cacheName, initialTopology);
            return initialTopology;
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
      runningCaches.remove(cacheName);

      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
            CacheTopologyControlCommand.Type.LEAVE, transport.getAddress());
      try {
         executeOnCoordinatorAsync(command);
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
            topologyId, throwable);
      try {
         executeOnCoordinatorAsync(command);
      } catch (Exception e) {
         log.debugf(e, "Error sending the rebalance completed notification for cache %s to the coordinator",
               cacheName);
      }
   }

   // called by the coordinator
   @Override
   public Map<String, Object[]> handleStatusRequest() {
      Map<String, Object[]> response = new HashMap<String, Object[]>();
      for (Map.Entry<String, LocalCacheStatus> e : runningCaches.entrySet()) {
         String cacheName = e.getKey();
         LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
         response.put(e.getKey(), new Object[]{cacheStatus.getJoinInfo(), cacheStatus.getTopology()});
      }
      return response;
   }

   @Override
   public void handleConsistentHashUpdate(String cacheName, CacheTopology cacheTopology) {
      LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
      if (cacheStatus == null) {
         log.tracef("Ignoring consistent hash update %s for cache %s that doesn't exist locally",
               cacheTopology.getTopologyId(), cacheName);
         return;
      }

      synchronized (cacheStatus) {
         if (cacheStatus.getTopology() != null && cacheStatus.getTopology().getTopologyId() > cacheTopology.getTopologyId()){
            log.tracef("Ignoring consistent hash update %s for cache %s, we have already received a newer topology %s",
                  cacheTopology.getTopologyId(), cacheName, cacheStatus.getTopology().getTopologyId());
            return;
         }

         log.debugf("Updating local consistent hash(es) for cache %s: new topology = %s",
               cacheName, cacheTopology);
         cacheStatus.setTopology(cacheTopology);
         ConsistentHash unionCH = null;
         if (cacheTopology.getPendingCH() != null) {
            unionCH = cacheStatus.getJoinInfo().getConsistentHashFactory().union(cacheTopology.getCurrentCH(),
                  cacheTopology.getPendingCH());
         }

         CacheTopologyHandler handler = cacheStatus.getHandler();
         CacheTopology unionTopology = new CacheTopology(cacheTopology.getTopologyId(),
               cacheTopology.getCurrentCH(), unionCH);
         handler.updateConsistentHash(unionTopology);
      }
   }

   @Override
   public void handleRebalance(String cacheName, CacheTopology cacheTopology) throws InterruptedException {
      LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
      if (cacheStatus == null) {
         log.tracef("Ignoring rebalance %s for cache %s that doesn't exist locally",
               cacheTopology.getTopologyId(), cacheName);
         return;
      }
      log.debugf("Starting local rebalance for cache %s, topology = %s", cacheName, cacheTopology);

      synchronized (cacheStatus) {
         cacheStatus.setTopology(cacheTopology);
      }

      ConsistentHash unionCH = cacheStatus.getJoinInfo().getConsistentHashFactory().union(
            cacheTopology.getCurrentCH(), cacheTopology.getPendingCH());
      CacheTopologyHandler handler = cacheStatus.getHandler();
      handler.rebalance(new CacheTopology(cacheTopology.getTopologyId(), cacheTopology.getCurrentCH(), unionCH));
   }

   @Override
   public CacheTopology getCacheTopology(String cacheName) {
      LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
      return cacheStatus.getTopology();
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
               command, ResponseMode.SYNCHRONOUS, timeout, true, null);
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
                  throw new Exception(t);
               }
            }
         });
      } else {
         Address coordinator = transport.getCoordinator();
         // ignore the responses
         transport.invokeRemotely(Collections.singleton(coordinator),
               command, ResponseMode.ASYNCHRONOUS_WITH_SYNC_MARSHALLING, 0, true, null);
      }
   }

}

class LocalCacheStatus {
   private final CacheJoinInfo joinInfo;
   private final CacheTopologyHandler handler;
   private volatile CacheTopology topology;

   private boolean joined;

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

   public boolean isJoined() {
      return joined;
   }

   public void setJoined(boolean joined) {
      this.joined = joined;
   }
}
