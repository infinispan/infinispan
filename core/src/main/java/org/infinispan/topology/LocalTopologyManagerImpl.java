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
import java.util.concurrent.TimeUnit;

import org.infinispan.CacheException;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.distribution.newch.ConsistentHash;
import org.infinispan.factories.GlobalComponentRegistry;
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
class LocalTopologyManagerImpl implements LocalTopologyManager {
   private static Log log = LogFactory.getLog(LocalTopologyManagerImpl.class);

   private Transport transport;
   private GlobalConfiguration globalConfiguration;

   private ConcurrentMap<String, CacheTopologyHandler> runningCaches = ConcurrentMapFactory.makeConcurrentMap();

   public void inject(Transport transport, GlobalConfiguration globalConfiguration, GlobalComponentRegistry gcr) {
      this.transport = transport;
      this.globalConfiguration = globalConfiguration;
   }

   @Override
   public CacheTopology join(String cacheName, CacheJoinInfo joinInfo, CacheTopologyHandler stm) throws Exception {
      runningCaches.put(cacheName, stm);

      ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
            CacheTopologyControlCommand.Type.JOIN, transport.getAddress(), joinInfo);
      int timeout = joinInfo.getTimeout();
      double endTime = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeout);
      while (true) {
         try {
            return (CacheTopology) executeOnCoordinator(command, timeout);
         } catch (Exception e) {
            log.debugf(e, "Error sending join request for cache %s to coordinator", cacheName);
            if (endTime <= System.nanoTime()) {
               throw e;
            }
         }
      }
   }

   @Override
   public void leave(String cacheName) {
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
      for (Map.Entry<String, CacheTopologyHandler> e : runningCaches.entrySet()) {
         String cacheName = e.getKey();
         CacheTopologyHandler handler = runningCaches.get(cacheName);
         CacheTopology topology = handler.getStatus();
         response.put(e.getKey(), topology);
      }
      return response;
   }

   @Override
   public void handleConsistentHashUpdate(String cacheName, ConsistentHash currentCH, ConsistentHash pendingCH) {
      CacheTopologyHandler handler = runningCaches.get(cacheName);
      handler.updateConsistentHash(currentCH, pendingCH);
   }

   @Override
   public void handleRebalance(String cacheName, int topologyId, ConsistentHash pendingCH) {
      CacheTopologyHandler handler = runningCaches.get(cacheName);
      try {
         handler.rebalance(topologyId, pendingCH);
      } finally {
         // TODO If there was an exception, propagate the exception back to the coordinator
         // We don't want to block further rebalancing just because we got an exception on one node

         // The coordinator can change during the state transfer, so we make the rebalance RPC async
         // and we send the response as a different command.
         // Note that if the coordinator changes again after we sent the command, we will get another
         // query for the status of our running caches. So we don't need to retry if the command failed.
         ReplicableCommand command = new CacheTopologyControlCommand(cacheName,
               CacheTopologyControlCommand.Type.REBALANCE_CONFIRM, transport.getAddress(), null);
         try {
            executeOnCoordinatorAsync(command);
         } catch (Exception e) {
            log.debugf(e, "Error sending the rebalance completed notification for cache %s to coordinator", cacheName);
         }
      }
   }

   private Object executeOnCoordinator(ReplicableCommand command, int timeout)
         throws Exception {
      if (transport.isCoordinator()) {
         try {
            command.perform(null);
         } catch (Throwable t) {
            throw new CacheException("Error handling join request", t);
         }
      }

      Address coordinator = transport.getCoordinator();
      Map<Address, Response> responseMap = transport.invokeRemotely(Collections.singleton(coordinator),
            command, ResponseMode.SYNCHRONOUS, timeout, false, null);
      Response response = responseMap.get(coordinator);
      if (response != null && !response.isSuccessful()) {
         throw new CacheException("Bad response received from coordinator: " + response);
      }
      return ((SuccessfulResponse) response).getResponseValue();
   }

   private void executeOnCoordinatorAsync(ReplicableCommand command)
         throws Exception {
      // if we are the coordinator, the execution is actually synchronous
      if (transport.isCoordinator()) {
         try {
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

}
