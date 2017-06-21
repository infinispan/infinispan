package org.infinispan.distribution.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.L1Manager;
import org.infinispan.distribution.RemoteValueRetrievedListener;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.distribution.L1WriteSynchronizer;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class L1ManagerImpl implements L1Manager, RemoteValueRetrievedListener {

   private static final Log log = LogFactory.getLog(L1ManagerImpl.class);
   private final boolean trace = log.isTraceEnabled();

   private Configuration configuration;
   private RpcManager rpcManager;
   private CommandsFactory commandsFactory;
   private int threshold;
   private long l1Lifespan;

   // TODO replace this with a custom, expirable collection
   private final ConcurrentMap<Object, ConcurrentMap<Address, Long>> requestors;
   private final ConcurrentMap<Object, L1WriteSynchronizer> synchronizers;
   private ScheduledExecutorService scheduledExecutor;
   private ScheduledFuture<?> scheduledRequestorsCleanupTask;
   private TimeService timeService;

   private RpcOptions syncIgnoreLeaversRpcOptions;

   public L1ManagerImpl() {
      requestors = CollectionFactory.makeConcurrentMap();
      synchronizers = CollectionFactory.makeConcurrentMap();
   }

   @Inject
   public void init(Configuration configuration, RpcManager rpcManager, CommandsFactory commandsFactory,
                    @ComponentName(KnownComponentNames.EXPIRATION_SCHEDULED_EXECUTOR) ScheduledExecutorService scheduledExecutor,
                    TimeService timeService) {
      this.rpcManager = rpcManager;
      this.commandsFactory = commandsFactory;
      this.configuration = configuration;
      this.scheduledExecutor = scheduledExecutor;
      this.timeService = timeService;
   }

   @Start (priority = 3)
   public void start() {
      this.threshold = configuration.clustering().l1().invalidationThreshold();
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
         log.warnL1NotHavingReaperThread();
      }
      // L1 invalidations can ignore a member leaving while sending invalidation, since their value is no longer
      // cached any longer
      syncIgnoreLeaversRpcOptions = rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, DeliverOrder.NONE)
            .build();
   }

   @Stop (priority = 3)
   public void stop() {
      if (scheduledRequestorsCleanupTask != null) scheduledRequestorsCleanupTask.cancel(true);
   }

   private void cleanUpRequestors() {
      long expiryTime = timeService.wallClockTime() - l1Lifespan;
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
      //we do a plain get first as that's likely to be enough
      ConcurrentMap<Address, Long> as = requestors.get(key);
      log.tracef("Registering requestor %s for key '%s'", origin, key);
      long now = timeService.wallClockTime();
      if (as == null) {
         // only if needed we create a new HashSet, but make sure we don't replace another one being created
         as = CollectionFactory.makeConcurrentMap();
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
   public CompletableFuture<?> flushCache(Collection<Object> keys, Address origin, boolean assumeOriginKeptEntryInL1) {
      final Collection<Address> invalidationAddresses = buildInvalidationAddressList(keys, origin, assumeOriginKeptEntryInL1);

      int nodes = invalidationAddresses.size();

      if (nodes > 0) {
         InvalidateCommand ic = commandsFactory.buildInvalidateFromL1Command(origin, EnumUtil.EMPTY_BIT_SET, keys);
         final SingleRpcCommand rpcCommand = commandsFactory.buildSingleRpcCommand(ic);

         // No need to invalidate at all if there is no one to invalidate!
         boolean multicast = isUseMulticast(nodes);
         if (trace) log.tracef("Invalidating keys %s on nodes %s. Use multicast? %s", keys, invalidationAddresses, multicast);

         CompletableFuture<Map<Address, Response>> future;
         if (multicast) {
            future = rpcManager.invokeRemotelyAsync(null, rpcCommand, syncIgnoreLeaversRpcOptions);
         } else {
            future = rpcManager.invokeRemotelyAsync(invalidationAddresses, rpcCommand, syncIgnoreLeaversRpcOptions);
         }
         return future;
      } else {
         if (trace) log.tracef("No L1 caches to invalidate for keys %s", keys);
         return null;
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

   @Override
   public void registerL1WriteSynchronizer(Object key, L1WriteSynchronizer sync) {
      if (synchronizers.put(key, sync) != null) {
         if (trace) {
            log.tracef("Replaced existing L1 write synchronizer for key %s as there was a concurrent L1 attempt to " +
                             "update", key);
         }
      }
   }

   @Override
   public void unregisterL1WriteSynchronizer(Object key, L1WriteSynchronizer sync) {
      synchronizers.remove(key, sync);
   }

   @Override
   public void remoteValueFound(InternalCacheEntry ice) {
      L1WriteSynchronizer synchronizer = synchronizers.get(ice.getKey());
      if (synchronizer != null) {
         synchronizer.runL1UpdateIfPossible(ice);
      }
   }

   @Override
   public void remoteValueNotFound(Object key) {
      L1WriteSynchronizer synchronizer = synchronizers.get(key);
      if (synchronizer != null) {
         // we assume synchronizer supports null value properly
         synchronizer.runL1UpdateIfPossible(null);
      }
   }
}
