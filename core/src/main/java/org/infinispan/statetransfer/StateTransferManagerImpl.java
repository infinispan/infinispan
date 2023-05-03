package org.infinispan.statetransfer;

import static org.infinispan.globalstate.GlobalConfigurationManager.CONFIG_STATE_CACHE_NAME;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.container.versioning.irac.IracVersionGenerator;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHashFactory;
import org.infinispan.distribution.ch.KeyPartitioner;
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
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.rpc.RpcManager;
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

   private final CompletableFuture<Void> initialStateTransferComplete = new CompletableFuture<>();

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
      if (!initialStateTransferComplete.isDone()) {
         assert distributionManager.getCacheTopology().getTopologyId() == newCacheTopology.getTopologyId();
         boolean isJoined = phase == CacheTopology.Phase.NO_REBALANCE &&
                            newCacheTopology.getReadConsistentHash().getMembers().contains(rpcManager.getAddress());
         if (isJoined) {
            initialStateTransferComplete.complete(null);
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
               initialStateTransferComplete.complete(null);
            }
            if (log.isTraceEnabled())
               log.tracef("Waiting for initial state transfer to finish for cache %s on %s", cacheName,
                          rpcManager.getAddress());
            initialStateTransferComplete.get(configuration.clustering().stateTransfer().timeout(),
                                             TimeUnit.MILLISECONDS);
         } catch (TimeoutException e) {
            throw log.initialStateTransferTimeout(cacheName, rpcManager.getAddress());
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
      initialStateTransferComplete.complete(null);
      localTopologyManager.leave(cacheName, configuration.clustering().remoteTimeout());
   }

   @ManagedAttribute(description = "If true, the node has successfully joined the grid and is considered to hold state.  If false, the join process is still in progress.", displayName = "Is join completed?", dataType = DataType.TRAIT)
   @Override
   public boolean isJoinComplete() {
      return initialStateTransferComplete.isDone();
   }

   @ManagedAttribute(description = "Retrieves the rebalancing status for this cache. Possible values are PENDING, SUSPENDED, IN_PROGRESS, COMPLETE", displayName = "Rebalancing progress", dataType = DataType.TRAIT)
   @Override
   public String getRebalancingStatus() throws Exception {
      return localTopologyManager.getRebalancingStatus(cacheName).toString();
   }

   @ManagedAttribute(description = "Checks whether the local node is receiving state from other nodes", displayName = "Is state transfer in progress?", dataType = DataType.TRAIT)
   @Override
   public boolean isStateTransferInProgress() {
      return stateConsumer.isStateTransferInProgress();
   }

   @ManagedAttribute(description = "The number of in-flight segments the local node requested from other nodes", displayName = "In-flight requested segments", dataType = DataType.MEASUREMENT)
   @Override
   public long getInflightSegmentTransferCount() {
      return stateConsumer.inflightRequestCount();
   }

   @ManagedAttribute(description = "The number of in-flight transactional segments the local node requested from other nodes", displayName = "In-flight requested transactional segments", dataType = DataType.MEASUREMENT)
   @Override
   public long getInflightTransactionalSegmentCount() {
      return stateConsumer.inflightTransactionSegmentCount();
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
