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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.infinispan.CacheException;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.concurrent.ConcurrentMapFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * The {@code LocalTopologyManager} implementation.
 *
 * @author Dan Berindei
 * @since 5.2
 */
public class LocalTopologyManagerImpl implements LocalTopologyManager {
   private static Log log = LogFactory.getLog(LocalTopologyManagerImpl.class);

   private Transport transport;
   private GlobalConfiguration globalConfiguration;
   private GlobalComponentRegistry gcr;

   private ConcurrentMap<String, LocalCacheStatus> runningCaches = ConcurrentMapFactory.makeConcurrentMap();

   @Inject
   public void inject(Transport transport, GlobalConfiguration globalConfiguration,
                      GlobalComponentRegistry gcr) {
      this.transport = transport;
      this.globalConfiguration = globalConfiguration;
      this.gcr = gcr;
   }

   @Override
   public CacheTopology join(String cacheName, CacheJoinInfo joinInfo, CacheTopologyHandler stm)
         throws Exception {
      log.debugf("Node %s joining cache %s", transport.getAddress(), cacheName);
      LocalCacheStatus status = new LocalCacheStatus(joinInfo, stm);
      runningCaches.put(cacheName, status);

      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
            CacheTopologyControlCommand.Type.JOIN, transport.getAddress(), joinInfo);
      long timeout = joinInfo.getTimeout();
      double endTime = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
      while (true) {
         try {
            CacheTopology initialTopology = (CacheTopology) executeOnCoordinator(command, timeout);
            status.topology = initialTopology;
            status.joinedLatch.countDown();
            return initialTopology;
         } catch (Exception e) {
            log.debugf(e, "Error sending join request for cache %s to coordinator", cacheName);
            if (endTime <= System.nanoTime()) {
               throw e;
            }
            // TODO Add some configuration for this, or use timeout/n
            Thread.sleep(1000);
         }
      }
   }

   @Override
   public void leave(String cacheName) {
      log.debugf("Node %s leaving cache %s", transport.getAddress(), cacheName);
      runningCaches.remove(cacheName);

      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
            CacheTopologyControlCommand.Type.LEAVE, transport.getAddress(), null);
      try {
         executeOnCoordinatorAsync(command);
      } catch (Exception e) {
         log.debugf(e, "Error sending the leave request for cache %s to coordinator", cacheName);
      }
   }

   // called by the coordinator
   @Override
   public Map<String, CacheTopology> handleStatusRequest() {
      Map<String, CacheTopology> response = new HashMap<String, CacheTopology>();
      for (Map.Entry<String, LocalCacheStatus> e : runningCaches.entrySet()) {
         String cacheName = e.getKey();
         LocalCacheStatus status = runningCaches.get(cacheName);
         CacheTopology topology = status.topology;
         response.put(e.getKey(), topology);
      }
      return response;
   }

   @Override
   public void handleConsistentHashUpdate(String cacheName, int topologyId, ConsistentHash currentCH,
                                          ConsistentHash pendingCH) {
      LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
      if (cacheStatus == null) {
         log.tracef("Ignoring consistent hash update %s for cache %s that doesn't exist locally",
               topologyId, cacheName);
         return;
      }
      log.debugf("Updating local consistent hash(es) for cache %s: currentCH = %s, pendingCH = %s",
            cacheName, currentCH, pendingCH);
      cacheStatus.topology = new CacheTopology(topologyId, currentCH, pendingCH);
      ConsistentHash unionCH;
      if (pendingCH != null)
         unionCH = cacheStatus.joinInfo.getConsistentHashFactory().union(currentCH, pendingCH);
      else unionCH = null;
      CacheTopologyHandler handler = cacheStatus.handler;
      handler.updateConsistentHash(topologyId, currentCH, unionCH);
   }

   @Override
   public void handleRebalance(String cacheName, int topologyId, ConsistentHash currentCH,
                               ConsistentHash pendingCH) throws InterruptedException {
      LocalCacheStatus cacheStatus = runningCaches.get(cacheName);
      if (cacheStatus == null) {
         log.tracef("Ignoring rebalance %s for cache %s that doesn't exist locally", topologyId, cacheName);
         return;
      }
      log.debugf("Starting local rebalance for cache %s, topology id = %d, new CH = %s", cacheName,
            topologyId, pendingCH);
      cacheStatus.joinedLatch.await();

      cacheStatus.topology = new CacheTopology(topologyId, currentCH, pendingCH);
      ConsistentHash unionCH = cacheStatus.joinInfo.getConsistentHashFactory().union(currentCH, pendingCH);
      CacheTopologyHandler handler = cacheStatus.handler;
      try {
         handler.rebalance(topologyId, currentCH, unionCH);
      } finally {
         // TODO If there was an exception, propagate the exception back to the coordinator
         // We don't want to block further rebalancing just because we got an exception on one node

         // The coordinator can change during the state transfer, so we make the rebalance RPC async
         // and we send the response as a different command.
         // Note that if the coordinator changes again after we sent the command, we will get another
         // query for the status of our running caches. So we don't need to retry if the command failed.
         ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
               CacheTopologyControlCommand.Type.REBALANCE_CONFIRM, transport.getAddress(), topologyId, null,
               null);
         try {
            executeOnCoordinatorAsync(command);
         } catch (Exception e) {
            log.debugf(e, "Error sending the rebalance completed notification for cache %s to coordinator",
                  cacheName);
         }
      }
   }

   @Override
   public CacheTopology getCacheTopology(String cacheName) {
      return runningCaches.get(cacheName).topology;
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
               command, ResponseMode.SYNCHRONOUS, timeout, false, null);
         response = responseMap.get(coordinator);
      }
      if (response == null || !response.isSuccessful()) {
         throw new CacheException("Bad response received from coordinator: " + response);
      }
      return ((SuccessfulResponse) response).getResponseValue();
   }

   private void executeOnCoordinatorAsync(ReplicableCommand command) throws Exception {
      // if we are the coordinator, the execution is actually synchronous
      if (transport.isCoordinator()) {
         try {
            gcr.wireDependencies(command);
            command.perform(null);
         } catch (Throwable t) {
            throw new CacheException("Error handling join request", t);
         }
      }

      Address coordinator = transport.getCoordinator();
      // ignore the responses
      transport.invokeRemotely(Collections.singleton(coordinator),
            command, ResponseMode.ASYNCHRONOUS_WITH_SYNC_MARSHALLING, 0, false, null);
   }

   public static class LocalCacheStatus {
      public final CacheJoinInfo joinInfo;
      public final CacheTopologyHandler handler;
      public final CountDownLatch joinedLatch = new CountDownLatch(1);
      public volatile CacheTopology topology;

      public LocalCacheStatus(CacheJoinInfo joinInfo, CacheTopologyHandler handler) {
         this.joinInfo = joinInfo;
         this.handler = handler;
      }
   }
}
