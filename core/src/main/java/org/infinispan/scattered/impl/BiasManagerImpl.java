package org.infinispan.scattered.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.remote.RenewBiasCommand;
import org.infinispan.commands.remote.RevokeBiasCommand;
import org.infinispan.commons.util.ByRef;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.MapResponseCollector;
import org.infinispan.scattered.BiasManager;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

@Listener
public class BiasManagerImpl implements BiasManager {
   private static Log log = LogFactory.getLog(BiasManager.class);
   private static final boolean trace = log.isTraceEnabled();
   // TODO: size bounding?
   // TODO: we could keep last access timestamp for local bias and refresh the lease
   // if this gets close to acquisition timestamp so that the primary owner does not withdraw it
   private ConcurrentMap<Object, LocalBias> localBias = new ConcurrentHashMap<>();
   private ConcurrentMap<Object, RemoteBias> remoteBias = new ConcurrentHashMap<>();
   private long renewLeasePeriod;

   private AdvancedCache cache;
   private Configuration configuration;
   private TimeService timeService;
   private DistributionManager distributionManager;
   private CommandsFactory commandsFactory;
   private RpcManager rpcManager;
   private KeyPartitioner keyPartitioner;
   private ScheduledExecutorService executor;

   @Inject
   public void init(AdvancedCache cache, Configuration configuration, TimeService timeService, DistributionManager distributionManager,
                    CommandsFactory commandsFactory, RpcManager rpcManager, KeyPartitioner keyPartitioner,
                    @ComponentName(KnownComponentNames.EXPIRATION_SCHEDULED_EXECUTOR) ScheduledExecutorService executor) {
      this.cache = cache;
      this.configuration = configuration;
      this.timeService = timeService;
      this.distributionManager = distributionManager;
      this.commandsFactory = commandsFactory;
      this.rpcManager = rpcManager;
      this.keyPartitioner = keyPartitioner;
      this.executor = executor;
   }

   @Start
   public void start() {
      // TODO: Should we add another configuration option?
      executor.scheduleAtFixedRate(this::removeOldBiasses, 0, configuration.expiration().wakeUpInterval(), TimeUnit.MILLISECONDS);
      executor.scheduleAtFixedRate(this::renewLocalBiasses, 0, configuration.expiration().wakeUpInterval(), TimeUnit.MILLISECONDS);
      renewLeasePeriod = configuration.clustering().biasLifespan() - configuration.clustering().remoteTimeout();
      cache.addListener(this);
   }

   @TopologyChanged
   public void onTopologyChange(TopologyChangedEvent event) {
      // Forget about remote nodes if we're no longer the primary owner
      ConsistentHash ch = event.getWriteConsistentHashAtEnd();
      Set<Integer> localSegments = ch.getMembers().contains(rpcManager.getAddress()) ?
            ch.getSegmentsForOwner(rpcManager.getAddress()) : Collections.emptySet();
      remoteBias.keySet().removeIf(key -> !localSegments.contains(keyPartitioner.getSegment(key)));
      // If we haven't been members of the last topology, then we probably had a split brain and we should just forget
      // all local biasses.
      ConsistentHash previousHash = event.getReadConsistentHashAtStart();
      if (previousHash != null && !previousHash.getMembers().contains(rpcManager.getAddress())) {
         localBias.clear();
      }
   }

   private void removeOldBiasses() {
      log.trace("Purging old biasses");
      long minTimestamp = timeService.wallClockTime() - configuration.clustering().biasLifespan();
      remoteBias.forEach((key, bias) -> {
         if (bias.acquiredTimestamp < minTimestamp && bias.revoking == null) {
            RemoteBias bias3 = remoteBias.computeIfPresent(key, (k, bias2) -> {
               bias2.revoking = bias2.biased;
               bias2.future = new CompletableFuture<>();
               return bias2;
            });
            if (bias3 == null)
               return;
            if (trace) {
               log.tracef("Revoking old bias for key %s from %s", key, bias3.biased);
            }
            // TODO: maybe some batching here; on the other side we don't want to block writes for too long
            RevokeBiasCommand revokeBiasCommand = commandsFactory.buildRevokeBiasCommand(null, 0, 0, Collections.singleton(key));
            rpcManager.invokeCommand(bias3.biased, revokeBiasCommand, MapResponseCollector.ignoreLeavers(),
                                     rpcManager.getSyncRpcOptions())
                      .whenComplete((nil, throwable) -> {
               CompletableFuture<?> future = bias3.future;
               if (throwable != null) {
                  if (trace) {
                     log.tracef(throwable, "Bias revocation for %s failed", key);
                  }
                  remoteBias.compute(key, (k, bias4) -> {
                     assert bias4 != null;
                     bias4.revoking = null;
                     bias4.future = null;
                     return bias4;
                  });
               } else {
                  remoteBias.remove(key);
                  if (trace) {
                     log.tracef("Bias for %s has been revoked", key);
                  }
               }
               future.complete(null);
            });
         }
      });
      log.trace("Purge completed");
   }

   private void renewLocalBiasses() {
      log.trace("Renewing local biasses");
      long timestamp = timeService.wallClockTime();
      // TODO: again, another configuration option?
      int batchSize = configuration.clustering().invalidationBatchSize();
      Map<Address, Collection<Object>> toRenew = new HashMap<>();
      localBias.forEach((key, bias) -> {
         // In order to renew the timestamp, we expect at least one read after acquisition.
         // This may be refined (requiring read within shorter interval)
         if (bias.acquisitionTimestamp + renewLeasePeriod < timestamp && bias.lastAccessTimestamp > bias.acquisitionTimestamp) {
            DistributionInfo distribution = distributionManager.getCacheTopology().getDistribution(key);
            if (distribution.primary() != null) {
               Collection<Object> keys = toRenew.computeIfAbsent(distribution.primary(), a -> new ArrayList<>(batchSize));
               keys.add(key);
               bias.acquisitionTimestamp = timestamp;
               if (keys.size() >= batchSize) {
                  RenewBiasCommand renewBiasCommand = commandsFactory.buildRenewBiasCommand(keys.toArray(new Object[batchSize]));
                  rpcManager.sendTo(distribution.primary(), renewBiasCommand, DeliverOrder.NONE);
                  keys.clear();
               }
            }
         }
      });
      for (Map.Entry<Address, Collection<Object>> entry : toRenew.entrySet()) {
         Collection<Object> keys = entry.getValue();
         if (keys.isEmpty()) continue;
         RenewBiasCommand renewBiasCommand = commandsFactory.buildRenewBiasCommand(keys.toArray(new Object[keys.size()]));
         rpcManager.sendTo(entry.getKey(), renewBiasCommand, DeliverOrder.NONE);
      }
      log.trace("Renewal completed local biasses");
   }

   @Override
   public void addLocalBias(Object key, int topologyId) {
      int currentTopologyId = distributionManager.getCacheTopology().getTopologyId();
      if (topologyId >= currentTopologyId) {
         if (trace) {
            log.tracef("%s: adding local bias for %s in topology %d", rpcManager.getAddress(), key, topologyId);
         }
         localBias.put(key, new LocalBias(timeService.wallClockTime()));
      } else if (trace) {
         log.tracef("%s: not adding local bias for %s in topology %d as current topology is %d",
               rpcManager.getAddress(), key, topologyId, currentTopologyId);
      }
   }

   @Override
   public void revokeLocalBias(Object key) {
      if (trace) {
         log.tracef("%s: revoking local bias for %s", rpcManager.getAddress(), key);
      }
      localBias.remove(key);
   }

   @Override
   public void revokeLocalBiasForSegments(Set<Integer> segments) {
      localBias.keySet().removeIf(key -> segments.contains(keyPartitioner.getSegment(key)));
   }

   @Override
   public boolean hasLocalBias(Object key) {
      LocalBias localBias = this.localBias.get(key);
      if (trace) {
         log.tracef("%s: local bias for %s? %s", rpcManager.getAddress(), key, localBias);
      }
      if (localBias != null && renewLeasePeriod > 0) {
         localBias.lastAccessTimestamp = timeService.wallClockTime();
      }
      return localBias != null;
   }

   @Override
   public List<Address> getRemoteBias(Object key) {
      RemoteBias remoteBias = this.remoteBias.get(key);
      return remoteBias != null ? remoteBias.biased : null;
   }

   @Override
   public Revocation startRevokingRemoteBias(Object key, Address newBiased) {
      ByRef<Revocation> ref = new ByRef<>(null);
      remoteBias.compute(key, (k, bias) -> {
         if (bias == null) {
            if (trace) {
               log.tracef("No bias for %s no need to revoke.", key);
            }
            return new RemoteBias(newBiased, timeService.wallClockTime());
         } else if (bias.revoking == null) {
            List<Address> revoking;
            if (bias.biased.contains(newBiased)) {
               if (bias.biased.size() == 1) {
                  if (trace) {
                     log.tracef("Not revoking bias for %s as the new biased is the same as previous: %s", k, newBiased);
                  }
                  bias.acquiredTimestamp = timeService.wallClockTime();
                  return bias;
               } else {
                  revoking = new ArrayList<>(bias.biased);
                  revoking.remove(newBiased);
               }
            } else {
               revoking = bias.biased;
            }
            if (trace) {
               log.tracef("Revoking remote bias for %s, %s -> %s", key, bias.biased, newBiased);
            }
            bias.revoking = revoking;
            bias.newBiased = Collections.singletonList(newBiased);
            bias.future = new CompletableFuture<>();
            ref.set(new RevocationImpl(k, revoking, bias.future));
            return bias;
         } else {
            if (trace) {
               log.tracef("Revocation already in progress for %s, %s -> %s", key, bias.revoking, bias.newBiased);
            }
            ref.set(new RevocationImpl(k, null, bias.future));
            return bias;
         }
      });
      return ref.get();
   }

   @Override
   public void renewRemoteBias(Object key, Address origin) {
      RemoteBias bias = this.remoteBias.get(key);
      if (bias != null) {
         bias.acquiredTimestamp = timeService.wallClockTime();
      }
   }

   @Override
   public void clear() {
      localBias.clear();
      remoteBias.clear();
   }

   private static class RemoteBias {
      private List<Address> biased;
      private List<Address> revoking;
      private List<Address> newBiased;
      private CompletableFuture<?> future;
      private long acquiredTimestamp;

      public RemoteBias(Address newBiased, long acquiredTimestamp) {
         biased = Collections.singletonList(newBiased);
         this.acquiredTimestamp = acquiredTimestamp;
      }
   }

   private class RevocationImpl implements Revocation, BiFunction<Object, RemoteBias, RemoteBias> {
      private final Object key;
      private final List<Address> biased;
      private final CompletableFuture<?> future;

      private RevocationImpl(Object key, List<Address> biased, CompletableFuture<?> future) {
         this.key = key;
         this.biased = biased;
         this.future = future;
      }

      @Override
      public boolean shouldRevoke() {
         return biased != null;
      }

      @Override
      public List<Address> biased() {
         return biased;
      }

      @Override
      public void complete() {
         remoteBias.compute(key, this);
      }

      @Override
      public RemoteBias apply(Object key, RemoteBias bias) {
         if (bias == null) {
            // this happens when a clear was called
            log.tracef("Missing bias information for %s", key);
            return null;
         }
         bias.biased = bias.newBiased;
         bias.revoking = null;
         bias.newBiased = null;
         bias.acquiredTimestamp = timeService.wallClockTime();
         bias.future = null;
         if (trace) {
            log.tracef("Bias for %s has been transferred to %s", key, bias.biased);
         }
         future.complete(null);
         return bias;
      }

      @Override
      public void fail() {
         remoteBias.compute(key, (k, bias) -> {
            if (bias == null) {
               // this happens when a clear was called
               log.tracef("Missing bias information for %s", key);
               return null;
            }
            if (trace) {
               log.tracef("Bias transfer for %s to %s failed, keeping %s", key, bias.newBiased, bias.biased);
            }
            bias.revoking = null;
            bias.newBiased = null;
            bias.future = null;
            future.complete(null);
            return bias;
         });
      }

      @Override
      public CompletionStage<?> toCompletionStage() {
         return future;
      }

      @Override
      public <T> CompletableFuture<T> handleCompose(Supplier<CompletionStage<T>> supplier) {
         return future.handle((nil, throwable) -> null).thenCompose(nil -> supplier.get());
      }
   }

   private static class LocalBias {
      private volatile long acquisitionTimestamp;
      private volatile long lastAccessTimestamp;

      public LocalBias(long timestamp) {
         acquisitionTimestamp = lastAccessTimestamp = timestamp;
      }

      @Override
      public String toString() {
         return "LocalBias{acq=" + acquisitionTimestamp + ", last=" + lastAccessTimestamp + '}';
      }
   }
}
