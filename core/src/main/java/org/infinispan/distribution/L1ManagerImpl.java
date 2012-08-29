/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.distribution;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.AggregatingNotifyingFutureImpl;
import org.infinispan.util.concurrent.ConcurrentMapFactory;
import org.infinispan.util.concurrent.NoOpFuture;
import org.infinispan.util.concurrent.NotifyingFutureImpl;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.infinispan.factories.KnownComponentNames.ASYNC_TRANSPORT_EXECUTOR;

public class L1ManagerImpl implements L1Manager {

   private static final Log log = LogFactory.getLog(L1ManagerImpl.class);
   private final boolean trace = log.isTraceEnabled();

   private Configuration configuration;
   private RpcManager rpcManager;
   private CommandsFactory commandsFactory;
   private int threshold;
   private long rpcTimeout;
   private long l1Lifespan;
   private ExecutorService asyncTransportExecutor;

   // TODO replace this with a custom, expirable collection
   private final ConcurrentMap<Object, ConcurrentMap<Address, Long>> requestors;
   private ScheduledExecutorService scheduledExecutor;
   private ScheduledFuture<?> scheduledRequestorsCleanupTask;


   public L1ManagerImpl() {
	   requestors = ConcurrentMapFactory.makeConcurrentMap();
   }

   @Inject
   public void init(Configuration configuration, RpcManager rpcManager, CommandsFactory commandsFactory,
                    @ComponentName(ASYNC_TRANSPORT_EXECUTOR) ExecutorService asyncTransportExecutor,
                    @ComponentName(KnownComponentNames.EVICTION_SCHEDULED_EXECUTOR) ScheduledExecutorService scheduledExecutor) {
      this.rpcManager = rpcManager;
      this.commandsFactory = commandsFactory;
      this.configuration = configuration;
      this.asyncTransportExecutor = asyncTransportExecutor;
      this.scheduledExecutor = scheduledExecutor;
   }
   
   @Start (priority = 3)
   public void start() {
      this.threshold = configuration.clustering().l1().invalidationThreshold();
      this.rpcTimeout = configuration.clustering().sync().replTimeout();
      this.l1Lifespan = configuration.clustering().l1().lifespan();
      if (configuration.clustering().l1().cleanupTaskFrequency() > 0) {
         scheduledRequestorsCleanupTask = scheduledExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
               cleanUpRequestors();
            }
         }, configuration.clustering().l1().cleanupTaskFrequency(),
               configuration.clustering().l1().cleanupTaskFrequency(), TimeUnit.MILLISECONDS);
      } else {
         log.warn("Not using an L1 invalidation reaper thread. This could lead to memory leaks as the requestors map may grow indefinitely!");
      }
   }

   @Stop (priority = 3)
   public void stop() {
      if (scheduledRequestorsCleanupTask != null) scheduledRequestorsCleanupTask.cancel(true);
   }

   private void cleanUpRequestors() {
      long expiryTime = System.currentTimeMillis() - l1Lifespan;
      for (Map.Entry<Object, ConcurrentMap<Address, Long>> entry: requestors.entrySet()) {
         Object key = entry.getKey();
         ConcurrentMap<Address, Long> reqs = entry.getValue();
         prune(reqs, expiryTime);
         if (reqs.isEmpty()) requestors.remove(key);
      }
   }
   
   private void prune(ConcurrentMap<Address, Long> reqs, long expiryTime) {
      for (Map.Entry<Address, Long> req: reqs.entrySet()) {
         if (req.getValue() < expiryTime) reqs.remove(req.getKey());
      }
   }
   
   @Override
   public void addRequestor(Object key, Address origin) {
      if (trace) log.tracef("Add %s as L1 requestor for %s", origin, key);
      //we do a plain get first as that's likely to be enough
      ConcurrentMap<Address, Long> as = requestors.get(key);
      long now = System.currentTimeMillis();
      if (as == null) {
         // only if needed we create a new HashSet, but make sure we don't replace another one being created
         as = ConcurrentMapFactory.makeConcurrentMap();
         as.put(origin, now);
         ConcurrentMap<Address, Long> previousAs = requestors.putIfAbsent(key, as);
         if (previousAs != null) {
            //another thread added it already, so use his copy and discard our proposed instance
            previousAs.put(origin, now);
         }
      } else {
         as.put(origin, now);
      }
   }

   @Override
   public Future<Object> flushCacheWithSimpleFuture(Collection<Object> keys, Object retval, Address origin, boolean assumeOriginKeptEntryInL1) {
      return flushCache(keys, retval, origin, assumeOriginKeptEntryInL1, false);
   }

   @Override
   public NotifyingNotifiableFuture<Object> flushCache(Collection<Object> keys, Object retval, Address origin, boolean assumeOriginKeptEntryInL1) {
      return (NotifyingNotifiableFuture<Object>) flushCache(keys, retval, origin, assumeOriginKeptEntryInL1, true);
   }

   private Future<Object> flushCache(Collection<Object> keys, final Object retval, Address origin, boolean assumeOriginKeptEntryInL1, boolean useNotifyingFuture) {
      if (trace) log.tracef("Invalidating L1 caches for keys %s", keys);

      final Collection<Address> invalidationAddresses = buildInvalidationAddressList(keys, origin, assumeOriginKeptEntryInL1);

      int nodes = invalidationAddresses.size();

      if (nodes > 0) {
         // No need to invalidate at all if there is no one to invalidate!
         boolean multicast = isUseMulticast(nodes);

         if (trace)
            log.tracef("There are %s nodes involved in invalidation. Threshold is: %s; using multicast: %s", nodes, threshold, multicast);

         if (multicast) {
            if (trace) log.tracef("Invalidating keys %s via multicast", keys);
            final InvalidateCommand ic = commandsFactory.buildInvalidateFromL1Command(origin, false, keys);
            if (useNotifyingFuture) {
               NotifyingNotifiableFuture<Object> future = new AggregatingNotifyingFutureImpl(retval, 2);
               rpcManager.broadcastRpcCommandInFuture(ic, future);
               return future;
            } else {
               return asyncTransportExecutor.submit(new Callable<Object>() {
                  @Override
                  public Object call() throws Exception {
                     rpcManager.broadcastRpcCommand(ic, true);
                     return retval;
                  }
               });
            }
         } else {
            final CacheRpcCommand rpc = commandsFactory.buildSingleRpcCommand(commandsFactory.buildInvalidateFromL1Command(origin, false, keys));
            // Ask the caches who have requested from us to remove
            if (trace) log.tracef("Keys %s needs invalidation on %s", keys, invalidationAddresses);
            if (useNotifyingFuture) {
               NotifyingNotifiableFuture<Object> future = new AggregatingNotifyingFutureImpl(retval, 2);
               rpcManager.invokeRemotelyInFuture(invalidationAddresses, rpc, true, future, rpcTimeout, true);
               return future;
            } else {
               return asyncTransportExecutor.submit(new Callable<Object>() {
                  @Override
                  public Object call() throws Exception {
                     rpcManager.invokeRemotely(invalidationAddresses, rpc, ResponseMode.SYNCHRONOUS, rpcTimeout, true);
                     return retval;
                  }
               });
            }
         }
      } else {
         if (trace) log.trace("No L1 caches to invalidate");
         return useNotifyingFuture ? new NotifyingFutureImpl(retval) : new NoOpFuture<Object>(retval);
      }
   }

   private Collection<Address> buildInvalidationAddressList(Collection<Object> keys, Address origin, boolean assumeOriginKeptEntryInL1) {
      Collection<Address> addresses = new HashSet<Address>(2);
      boolean originIsInRequestorsList = false;
      for (Object key : keys) {
         ConcurrentMap<Address, Long> as = requestors.remove(key);
         if (as != null) {
            Set<Address> requestorAddresses = as.keySet();
            addresses.addAll(requestorAddresses);
            if (assumeOriginKeptEntryInL1 && origin != null && requestorAddresses.contains(origin)) {
               originIsInRequestorsList = true;
               // re-add the origin as a requestor since the key will still be in the origin's L1 cache
               addRequestor(key, origin);
            }
         }
      }
      // Prevent a loop by not sending the invalidation message to the origin
      if (originIsInRequestorsList) addresses.remove(origin);
      return addresses;
   }

   private boolean isUseMulticast(int nodes) {
      // User has requested unicast only
      if (threshold == -1) return false;
      // Underlying transport is not multicast capable
      if (!rpcManager.getTransport().isMulticastCapable()) return false;
      // User has requested multicast only
      if (threshold == 0) return true;
      // we decide:
      return nodes > threshold;
   }
}
