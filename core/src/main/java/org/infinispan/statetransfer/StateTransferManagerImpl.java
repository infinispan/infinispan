package org.infinispan.statetransfer;

import static org.infinispan.globalstate.GlobalConfigurationManager.CONFIG_STATE_CACHE_NAME;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.versioning.irac.IracVersionGenerator;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.distribution.ch.impl.ScatteredConsistentHashFactory;
import org.infinispan.distribution.ch.impl.SyncConsistentHashFactory;
import org.infinispan.distribution.ch.impl.SyncReplicatedConsistentHashFactory;
import org.infinispan.distribution.ch.impl.TopologyAwareSyncConsistentHashFactory;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.globalstate.GlobalStateManager;
import org.infinispan.globalstate.ScopedPersistentState;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.persistence.manager.PreloadManager;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheJoinInfo;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.CacheTopologyHandler;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.irac.IracManager;

/**
 * {@link StateTransferManager} implementation.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@MBean(objectName = "StateTransferManager", description = "Component that handles state transfer")
@Scope(Scopes.NAMED_CACHE)
public class StateTransferManagerImpl implements StateTransferManager {

   private static final Log log = LogFactory.getLog(StateTransferManagerImpl.class);

   @ComponentName(KnownComponentNames.CACHE_NAME)
   @Inject protected String cacheName;
   @Inject StateConsumer stateConsumer;
   @Inject StateProvider stateProvider;
   @Inject PartitionHandlingManager partitionHandlingManager;
   @Inject DistributionManager distributionManager;
   @Inject CacheNotifier<?, ?> cacheNotifier;
   @Inject Configuration configuration;
   @Inject GlobalConfiguration globalConfiguration;
   @Inject RpcManager rpcManager;
   @Inject LocalTopologyManager localTopologyManager;
   @Inject KeyPartitioner keyPartitioner;
   @Inject GlobalStateManager globalStateManager;
   // Only join the cluster after preloading
   @Inject PreloadManager preloadManager;
   // Make sure we can handle incoming requests before joining
   @Inject PerCacheInboundInvocationHandler inboundInvocationHandler;
   @Inject IracManager iracManager;
   @Inject IracVersionGenerator iracVersionGenerator;

   private final CountDownLatch initialStateTransferComplete = new CountDownLatch(1);

   @Start(priority = 60)
   @Override
   public void start() throws Exception {
      if (log.isTraceEnabled()) {
         log.tracef("Starting StateTransferManager of cache %s on node %s", cacheName, rpcManager.getAddress());
      }

      Optional<Integer> persistentStateChecksum;
      if (globalStateManager != null) {
         persistentStateChecksum = globalStateManager.readScopedState(cacheName).map(ScopedPersistentState::getChecksum);
      } else {
         persistentStateChecksum = Optional.empty();
      }

      float capacityFactor = globalConfiguration.isZeroCapacityNode() && !CONFIG_STATE_CACHE_NAME.equals(cacheName) ? 0.0f :
            configuration.clustering().hash().capacityFactor();

      CacheJoinInfo joinInfo = new CacheJoinInfo(pickConsistentHashFactory(globalConfiguration, configuration),
            configuration.clustering().hash().numSegments(),
            configuration.clustering().hash().numOwners(),
            configuration.clustering().stateTransfer().timeout(),
            configuration.clustering().cacheMode(),
            capacityFactor,
            localTopologyManager.getPersistentUUID(),
            persistentStateChecksum);

      CompletionStage<CacheTopology> stage = localTopologyManager.join(cacheName, joinInfo, new CacheTopologyHandler() {
         @Override
         public CompletionStage<Void> updateConsistentHash(CacheTopology cacheTopology) {
            return doTopologyUpdate(cacheTopology, false);
         }

         @Override
         public CompletionStage<Void> rebalance(CacheTopology cacheTopology) {
            return doTopologyUpdate(cacheTopology, true);
         }
      }, partitionHandlingManager);

      CacheTopology initialTopology = CompletionStages.join(stage);
      if (log.isTraceEnabled()) {
         log.tracef("StateTransferManager of cache %s on node %s received initial topology %s", cacheName, rpcManager.getAddress(), initialTopology);
      }
   }

   /**
    * If no ConsistentHashFactory was explicitly configured we choose a suitable one based on cache mode.
    */
   public static ConsistentHashFactory pickConsistentHashFactory(GlobalConfiguration globalConfiguration, Configuration configuration) {
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

   private CompletionStage<Void> doTopologyUpdate(CacheTopology newCacheTopology, boolean isRebalance) {
      CacheTopology oldCacheTopology = distributionManager.getCacheTopology();

      int newTopologyId = newCacheTopology.getTopologyId();
      if (oldCacheTopology != null && oldCacheTopology.getTopologyId() > newTopologyId) {
         throw new IllegalStateException(
               "Old topology is higher: old=" + oldCacheTopology + ", new=" + newCacheTopology);
      }

      if (log.isTraceEnabled()) {
         log.tracef("Installing new cache topology %s on cache %s", newCacheTopology, cacheName);
      }

      // No need for extra synchronization here, since LocalTopologyManager already serializes topology updates.
      if (newCacheTopology.getMembers().contains(rpcManager.getAddress())) {
         if (!distributionManager.getCacheTopology().isConnected() ||
             !distributionManager.getCacheTopology().getMembersSet().contains(rpcManager.getAddress())) {
            if (log.isTraceEnabled())
               log.tracef("This is the first topology %d in which the local node is a member", newTopologyId);
            inboundInvocationHandler.setFirstTopologyAsMember(newTopologyId);
         }
      }

      int newRebalanceId = newCacheTopology.getRebalanceId();
      CacheTopology.Phase phase = newCacheTopology.getPhase();
      iracManager.onTopologyUpdate(oldCacheTopology, newCacheTopology);

      return cacheNotifier.notifyTopologyChanged(oldCacheTopology, newCacheTopology, newTopologyId, true)
            .thenCompose(
                  ignored -> updateProviderAndConsumer(isRebalance, newTopologyId, newCacheTopology, newRebalanceId, phase)
            ).thenCompose(
                  ignored -> cacheNotifier.notifyTopologyChanged(oldCacheTopology, newCacheTopology, newTopologyId, false)
            ).thenRun(() -> {
               completeInitialTransferIfNeeded(newCacheTopology, phase);
               partitionHandlingManager.onTopologyUpdate(newCacheTopology);
               iracVersionGenerator.onTopologyChange(newCacheTopology);
            });
   }

   private CompletionStage<?> updateProviderAndConsumer(boolean isRebalance, int newTopologyId, CacheTopology newCacheTopology,
                                                           int newRebalanceId, CacheTopology.Phase phase) {
      CompletionStage<CompletionStage<Void>> consumerUpdateFuture =
            stateConsumer.onTopologyUpdate(newCacheTopology, isRebalance);
      CompletionStage<Void> consumerTransferFuture = consumerUpdateFuture.thenCompose(Function.identity());
      CompletableFuture<Void> providerFuture = stateProvider.onTopologyUpdate(newCacheTopology, isRebalance);
      consumerTransferFuture.runAfterBoth(providerFuture, () -> {
         switch (phase) {
            case TRANSITORY:
            case READ_OLD_WRITE_ALL:
            case READ_ALL_WRITE_ALL:
            case READ_NEW_WRITE_ALL:
               localTopologyManager.confirmRebalancePhase(cacheName, newTopologyId, newRebalanceId, null);
         }
      });
      // Block topology updates until the consumer finishes applying the topology update
      return consumerUpdateFuture;
   }

   private void completeInitialTransferIfNeeded(CacheTopology newCacheTopology, CacheTopology.Phase phase) {
      if (initialStateTransferComplete.getCount() > 0) {
         assert distributionManager.getCacheTopology().getTopologyId() == newCacheTopology.getTopologyId();
         boolean isJoined = phase == CacheTopology.Phase.NO_REBALANCE &&
                            newCacheTopology.getReadConsistentHash().getMembers().contains(rpcManager.getAddress());
         if (isJoined) {
            initialStateTransferComplete.countDown();
            log.tracef("Initial state transfer complete for cache %s on node %s", cacheName, rpcManager.getAddress());
         }
      }
   }

   @Override
   public void waitForInitialStateTransferToComplete() {
      if (configuration.clustering().stateTransfer().awaitInitialTransfer()) {
         try {
            if (!localTopologyManager.isCacheRebalancingEnabled(cacheName) ||
                partitionHandlingManager.getAvailabilityMode() == AvailabilityMode.DEGRADED_MODE) {
               initialStateTransferComplete.countDown();
            }
            if (log.isTraceEnabled())
               log.tracef("Waiting for initial state transfer to finish for cache %s on %s", cacheName,
                          rpcManager.getAddress());
            boolean success = initialStateTransferComplete.await(configuration.clustering().stateTransfer().timeout(), TimeUnit.MILLISECONDS);
            if (!success) {
               throw new CacheException(String.format("Initial state transfer timed out for cache %s on %s",
                                                      cacheName, rpcManager.getAddress()));
            }
         } catch (CacheException e) {
            throw e;
         } catch (Exception e) {
            throw new CacheException(e);
         }
      }
   }

   @Stop(priority = 0)
   @Override
   public void stop() {
      if (log.isTraceEnabled()) {
         log.tracef("Shutting down StateTransferManager of cache %s on node %s", cacheName, rpcManager.getAddress());
      }
      initialStateTransferComplete.countDown();
      localTopologyManager.leave(cacheName, configuration.clustering().remoteTimeout());
   }

   @ManagedAttribute(description = "If true, the node has successfully joined the grid and is considered to hold state.  If false, the join process is still in progress.", displayName = "Is join completed?", dataType = DataType.TRAIT)
   @Override
   public boolean isJoinComplete() {
      return distributionManager.getCacheTopology() != null; // TODO [anistor] this does not mean we have received a topology update or a rebalance yet
   }

   @ManagedAttribute(description = "Retrieves the rebalancing status for this cache. Possible values are PENDING, SUSPENDED, IN_PROGRESS, BALANCED", displayName = "Rebalancing progress", dataType = DataType.TRAIT)
   @Override
   public String getRebalancingStatus() throws Exception {
      return localTopologyManager.getRebalancingStatus(cacheName).toString();
   }

   @ManagedAttribute(description = "Checks whether there is a pending inbound state transfer on this cluster member.", displayName = "Is state transfer in progress?", dataType = DataType.TRAIT)
   @Override
   public boolean isStateTransferInProgress() {
      return stateConsumer.isStateTransferInProgress();
   }

   @Override
   public Map<Address, Response> forwardCommandIfNeeded(TopologyAffectedCommand command, Set<Object> affectedKeys,
                                                        Address origin) {
      LocalizedCacheTopology cacheTopology = distributionManager.getCacheTopology();
      if (cacheTopology == null) {
         if (log.isTraceEnabled()) {
            log.tracef("Not fowarding command %s because topology is null.", command);
         }
         return Collections.emptyMap();
      }
      int cmdTopologyId = command.getTopologyId();
      // forward commands with older topology ids to their new targets
      // but we need to make sure we have the latest topology
      int localTopologyId = cacheTopology.getTopologyId();
      // if it's a tx/lock/write command, forward it to the new owners
      if (log.isTraceEnabled()) {
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
            if (log.isTraceEnabled()) {
               log.tracef("Forwarding command %s to new targets %s", command, newTargets);
            }
            // TxCompletionNotificationCommands are the only commands being forwarded now,
            // and they must be OOB + asynchronous
            rpcManager.sendToMany(newTargets, command, DeliverOrder.NONE);
         }
      }
      return Collections.emptyMap();
   }

   @Override
   public StateConsumer getStateConsumer() {
      return stateConsumer;
   }

   @Override
   public StateProvider getStateProvider() {
      return stateProvider;
   }

   @Override
   public String toString() {
      return "StateTransferManagerImpl [" + cacheName + "@" + rpcManager.getAddress() + "]";
   }
}
