package org.infinispan.statetransfer;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.ch.impl.ScatteredConsistentHashFactory;
import org.infinispan.distribution.ch.impl.SyncConsistentHashFactory;
import org.infinispan.distribution.ch.impl.SyncReplicatedConsistentHashFactory;
import org.infinispan.distribution.ch.impl.TopologyAwareSyncConsistentHashFactory;
import org.infinispan.distribution.group.impl.PartitionerConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.globalstate.GlobalStateManager;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheJoinInfo;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.CacheTopologyHandler;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * {@link StateTransferManager} implementation.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
public class StateTransferManagerImpl implements StateTransferManager {

   private static final Log log = LogFactory.getLog(StateTransferManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   private StateConsumer stateConsumer;
   private StateProvider stateProvider;
   private PartitionHandlingManager partitionHandlingManager;
   private DistributionManager distributionManager;
   private String cacheName;
   private CacheNotifier cacheNotifier;
   private Configuration configuration;
   private GlobalConfiguration globalConfiguration;
   private RpcManager rpcManager;
   private LocalTopologyManager localTopologyManager;
   private Optional<Integer> persistentStateChecksum;

   private final CountDownLatch initialStateTransferComplete = new CountDownLatch(1);
   // The first topology in which the local node was a member. Any command with a lower
   // topology id will be ignored.
   private volatile int firstTopologyAsMember = Integer.MAX_VALUE;
   private KeyPartitioner keyPartitioner;

   public StateTransferManagerImpl() {
   }

   @Inject
   public void init(StateConsumer stateConsumer,
                    StateProvider stateProvider,
                    Cache cache,
                    CacheNotifier cacheNotifier,
                    Configuration configuration,
                    GlobalConfiguration globalConfiguration,
                    RpcManager rpcManager,
                    KeyPartitioner keyPartitioner,
                    LocalTopologyManager localTopologyManager,
                    PartitionHandlingManager partitionHandlingManager,
                    GlobalStateManager globalStateManager,
                    DistributionManager distributionManager) {
      this.stateConsumer = stateConsumer;
      this.stateProvider = stateProvider;
      this.cacheName = cache.getName();
      this.cacheNotifier = cacheNotifier;
      this.configuration = configuration;
      this.globalConfiguration = globalConfiguration;
      this.rpcManager = rpcManager;
      this.keyPartitioner = keyPartitioner;
      this.localTopologyManager = localTopologyManager;
      this.partitionHandlingManager = partitionHandlingManager;
      this.distributionManager = distributionManager;
      persistentStateChecksum = globalStateManager.readScopedState(cacheName).map(ScopedPersistentState::getChecksum);
   }

   // needs to be AFTER the DistributionManager and *after* the cache loader manager (if any) inits and preloads
   @Start(priority = 60)
   @Override
   public void start() throws Exception {
      if (trace) {
         log.tracef("Starting StateTransferManager of cache %s on node %s", cacheName, rpcManager.getAddress());
      }

      CacheJoinInfo joinInfo = new CacheJoinInfo(pickConsistentHashFactory(),
            configuration.clustering().hash().hash(),
            configuration.clustering().hash().numSegments(),
            configuration.clustering().hash().numOwners(),
            configuration.clustering().stateTransfer().timeout(),
            configuration.transaction().transactionProtocol().isTotalOrder(),
            configuration.clustering().cacheMode(),
            configuration.clustering().hash().capacityFactor(),
            localTopologyManager.getPersistentUUID(),
            persistentStateChecksum);

      CacheTopology initialTopology = localTopologyManager.join(cacheName, joinInfo, new CacheTopologyHandler() {
         @Override
         public void updateConsistentHash(CacheTopology cacheTopology) {
            doTopologyUpdate(cacheTopology, false);
         }

         @Override
         public void rebalance(CacheTopology cacheTopology) {
            doTopologyUpdate(cacheTopology, true);
         }
      }, partitionHandlingManager);

      if (trace) {
         log.tracef("StateTransferManager of cache %s on node %s received initial topology %s", cacheName, rpcManager.getAddress(), initialTopology);
      }
   }

   /**
    * If no ConsistentHashFactory was explicitly configured we choose a suitable one based on cache mode.
    */
   private ConsistentHashFactory pickConsistentHashFactory() {
      ConsistentHashFactory factory = configuration.clustering().hash().consistentHashFactory();
      if (factory == null) {
         CacheMode cacheMode = configuration.clustering().cacheMode();
         if (cacheMode.isClustered()) {
            if (cacheMode.isDistributed()) {
               if (globalConfiguration.transport().hasTopologyInfo()) {
                  factory = new TopologyAwareSyncConsistentHashFactory();
               } else {
                  factory = new SyncConsistentHashFactory();
               }
            } else if (cacheMode.isReplicated() || cacheMode.isInvalidation()) {
               factory = new SyncReplicatedConsistentHashFactory();
            } else if (cacheMode.isScattered()) {
               factory = new ScatteredConsistentHashFactory();
            } else {
               throw new CacheException("Unexpected cache mode: " + cacheMode);
            }
         }
      }
      return factory;
   }

   /**
    * Decorates the given cache topology to add a key partitioner.
    *
    * The key partitioner may include support for grouping as well.
    */
   private CacheTopology addPartitioner(CacheTopology cacheTopology) {
      ConsistentHash currentCH = cacheTopology.getCurrentCH();
      currentCH = new PartitionerConsistentHash(currentCH, keyPartitioner);
      ConsistentHash pendingCH = cacheTopology.getPendingCH();
      if (pendingCH != null) {
         pendingCH = new PartitionerConsistentHash(pendingCH, keyPartitioner);
      }
      ConsistentHash unionCH = cacheTopology.getUnionCH();
      if (unionCH != null) {
         unionCH = new PartitionerConsistentHash(unionCH, keyPartitioner);
      }
      return new CacheTopology(cacheTopology.getTopologyId(), cacheTopology.getRebalanceId(), currentCH, pendingCH,
            unionCH, cacheTopology.getPhase(), cacheTopology.getActualMembers(), cacheTopology.getMembersPersistentUUIDs());
   }

   private void doTopologyUpdate(CacheTopology newCacheTopology, boolean isRebalance) {
      CacheTopology oldCacheTopology = stateConsumer.getCacheTopology();

      int newTopologyId = newCacheTopology.getTopologyId();
      if (oldCacheTopology != null && oldCacheTopology.getTopologyId() > newTopologyId) {
         throw new IllegalStateException("Old topology is higher: old=" + oldCacheTopology + ", new=" + newCacheTopology);
      }

      if (trace) {
         log.tracef("Installing new cache topology %s on cache %s", newCacheTopology, cacheName);
      }

      // No need for extra synchronization here, since LocalTopologyManager already serializes topology updates.
      if (firstTopologyAsMember == Integer.MAX_VALUE && newCacheTopology.getMembers().contains(rpcManager.getAddress())) {
         firstTopologyAsMember = newTopologyId;
         if (trace) log.tracef("This is the first topology %d in which the local node is a member", firstTopologyAsMember);
      }

      // handle the partitioner
      newCacheTopology = addPartitioner(newCacheTopology);
      int newRebalanceId = newCacheTopology.getRebalanceId();
      CacheTopology.Phase phase = newCacheTopology.getPhase();

      cacheNotifier.notifyTopologyChanged(oldCacheTopology, newCacheTopology, newTopologyId, true);

      CompletableFuture<Void> consumerFuture = stateConsumer.onTopologyUpdate(newCacheTopology, isRebalance);
      CompletableFuture<Void> providerFuture = stateProvider.onTopologyUpdate(newCacheTopology, isRebalance);
      CompletableFuture.allOf(consumerFuture, providerFuture).thenRun(() -> {
         switch (phase) {
            case TRANSITORY:
            case READ_OLD_WRITE_ALL:
            case READ_ALL_WRITE_ALL:
            case READ_NEW_WRITE_ALL:
               localTopologyManager.confirmRebalancePhase(cacheName, newTopologyId, newRebalanceId, null);
         }
      });

      cacheNotifier.notifyTopologyChanged(oldCacheTopology, newCacheTopology, newTopologyId, false);

      if (initialStateTransferComplete.getCount() > 0) {
         assert stateConsumer.getCacheTopology() == newCacheTopology;
         boolean isJoined = phase == CacheTopology.Phase.NO_REBALANCE
               && newCacheTopology.getReadConsistentHash().getMembers().contains(rpcManager.getAddress());
         if (isJoined) {
            initialStateTransferComplete.countDown();
            log.tracef("Initial state transfer complete for cache %s on node %s", cacheName, rpcManager.getAddress());
         }
      }
      partitionHandlingManager.onTopologyUpdate(newCacheTopology);
   }

   @Start(priority = 1000)
   @SuppressWarnings("unused")
   public void waitForInitialStateTransferToComplete() throws Exception {
      if (configuration.clustering().stateTransfer().awaitInitialTransfer()) {
         if (!localTopologyManager.isCacheRebalancingEnabled(cacheName)) {
            initialStateTransferComplete.countDown();
         }
         if (trace) log.tracef("Waiting for initial state transfer to finish for cache %s on %s", cacheName, rpcManager.getAddress());
         boolean success = initialStateTransferComplete.await(configuration.clustering().stateTransfer().timeout(), TimeUnit.MILLISECONDS);
         if (!success) {
            throw new CacheException(String.format("Initial state transfer timed out for cache %s on %s",
                  cacheName, rpcManager.getAddress()));
         }
      }
   }

   @Stop(priority = 0)
   @Override
   public void stop() {
      if (trace) {
         log.tracef("Shutting down StateTransferManager of cache %s on node %s", cacheName, rpcManager.getAddress());
      }
      initialStateTransferComplete.countDown();
      localTopologyManager.leave(cacheName);
   }

   @Override
   public boolean isJoinComplete() {
      return stateConsumer.getCacheTopology() != null; // TODO [anistor] this does not mean we have received a topology update or a rebalance yet
   }

   @Override
   public String getRebalancingStatus() throws Exception {
      return localTopologyManager.getRebalancingStatus(cacheName).toString();
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
      return stateConsumer.getCacheTopology();
   }

   @Override
   public Map<Address, Response> forwardCommandIfNeeded(TopologyAffectedCommand command, Set<Object> affectedKeys,
                                                        Address origin) {
      LocalizedCacheTopology cacheTopology = distributionManager.getCacheTopology();
      if (cacheTopology == null) {
         if (trace) {
            log.tracef("Not fowarding command %s because topology is null.", command);
         }
         return Collections.emptyMap();
      }
      int cmdTopologyId = command.getTopologyId();
      // forward commands with older topology ids to their new targets
      // but we need to make sure we have the latest topology
      int localTopologyId = cacheTopology.getTopologyId();
      // if it's a tx/lock/write command, forward it to the new owners
      if (trace) {
         log.tracef("CommandTopologyId=%s, localTopologyId=%s", cmdTopologyId, localTopologyId);
      }

      if (cmdTopologyId < localTopologyId) {
         Collection<Address> newTargets = new HashSet<>(cacheTopology.getWriteOwners(affectedKeys));
         newTargets.remove(rpcManager.getAddress());
         // Forwarding to the originator would create a cycle
         // TODO This may not be the "real" originator, but one of the original recipients
         // or even one of the nodes that one of the original recipients forwarded the command to.
         // In non-transactional caches, the "real" originator keeps a lock for the duration
         // of the RPC, so this means we could get a deadlock while forwarding to it.
         newTargets.remove(origin);
         if (!newTargets.isEmpty()) {
            // Update the topology id to prevent cycles
            command.setTopologyId(localTopologyId);
            if (trace) {
               log.tracef("Forwarding command %s to new targets %s", command, newTargets);
            }
            final RpcOptions rpcOptions = rpcManager.getDefaultRpcOptions(false, DeliverOrder.NONE);
            // TODO find a way to forward the command async if it was received async
            // TxCompletionNotificationCommands are the only commands forwarded asynchronously, and they must be OOB
            return rpcManager.invokeRemotely(newTargets, command, rpcOptions);
         }
      }
      return Collections.emptyMap();
   }

   // TODO Investigate merging ownsData() and getFirstTopologyAsMember(), as they serve a similar purpose
   @Override
   public boolean ownsData() {
      return stateConsumer.ownsData();
   }

   @Override
   public int getFirstTopologyAsMember() {
      return firstTopologyAsMember;
   }

   @Override

   public String toString() {
      return "StateTransferManagerImpl [" + cacheName + "@" + rpcManager.getAddress() + "]";
   }
}
