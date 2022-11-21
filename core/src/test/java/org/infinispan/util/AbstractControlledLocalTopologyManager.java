package org.infinispan.util;

import java.util.concurrent.CompletionStage;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.CacheJoinInfo;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.CacheTopologyHandler;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.topology.LocalTopologyManagerImpl;
import org.infinispan.topology.ManagerStatusResponse;
import org.infinispan.topology.PersistentUUID;
import org.infinispan.topology.RebalancingStatus;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.events.EventLogManager;

/**
 * Class to be extended to allow some control over the local topology manager when testing Infinispan.
 * <p/>
 * Note: create before/after method lazily when need.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Scope(Scopes.GLOBAL)
public abstract class AbstractControlledLocalTopologyManager implements LocalTopologyManager {

   private final LocalTopologyManager delegate;

   @Inject
   void inject(BasicComponentRegistry bcr) {
      bcr.wireDependencies(delegate, false);
      bcr.getComponent(EventLogManager.class).running();
   }

   @Start
   void start() {
      TestingUtil.startComponent(delegate);
   }

   @Stop
   void stop() {
      TestingUtil.stopComponent(delegate);
   }

   protected AbstractControlledLocalTopologyManager(LocalTopologyManager delegate) {
      this.delegate = delegate;
   }

   @Override
   public final CompletionStage<CacheTopology> join(String cacheName, CacheJoinInfo joinInfo, CacheTopologyHandler stm,
                                                    PartitionHandlingManager phm) throws Exception {
      return delegate.join(cacheName, joinInfo, stm, phm);
   }

   @Override
   public final void leave(String cacheName, long timeout) {
      delegate.leave(cacheName, timeout);
   }

   @Override
   public final void confirmRebalancePhase(String cacheName, int topologyId, int rebalanceId, Throwable throwable) {
      TestingUtil.sequence(beforeConfirmRebalancePhase(cacheName, topologyId, throwable), () -> {
         delegate.confirmRebalancePhase(cacheName, topologyId, rebalanceId, throwable);
         return CompletableFutures.completedNull();
      });
   }

   @Override
   public final CompletionStage<ManagerStatusResponse> handleStatusRequest(int viewId) {
      return delegate.handleStatusRequest(viewId);
   }

   @Override
   public final CompletionStage<Void> handleTopologyUpdate(String cacheName, CacheTopology cacheTopology,
                                                           AvailabilityMode availabilityMode, int viewId,
                                                           Address sender) {
      return TestingUtil.sequence(beforeHandleTopologyUpdate(cacheName, cacheTopology, viewId),
            () -> delegate.handleTopologyUpdate(cacheName, cacheTopology, availabilityMode, viewId, sender));
   }

   @Override
   public final CompletionStage<Void> handleRebalance(String cacheName, CacheTopology cacheTopology, int viewId,
                                                      Address sender) {
      return TestingUtil.sequence(beforeHandleRebalance(cacheName, cacheTopology, viewId),
            () -> delegate.handleRebalance(cacheName, cacheTopology, viewId, sender));
   }

   @Override
   public final CacheTopology getCacheTopology(String cacheName) {
      return delegate.getCacheTopology(cacheName);
   }

   @Override
   public CompletionStage<Void> handleStableTopologyUpdate(String cacheName, CacheTopology cacheTopology,
                                                           final Address sender, int viewId) {
      return delegate.handleStableTopologyUpdate(cacheName, cacheTopology, sender, viewId);
   }

   @Override
   public CacheTopology getStableCacheTopology(String cacheName) {
      return delegate.getStableCacheTopology(cacheName);
   }

   @Override
   public boolean isRebalancingEnabled() throws Exception {
      return delegate.isRebalancingEnabled();
   }

   @Override
   public void setRebalancingEnabled(boolean enabled) throws Exception {
      delegate.setRebalancingEnabled(enabled);
   }

   @Override
   public boolean isCacheRebalancingEnabled(String cacheName) throws Exception {
      return delegate.isCacheRebalancingEnabled(cacheName);
   }

   @Override
   public void setCacheRebalancingEnabled(String cacheName, boolean enabled) throws Exception {
      delegate.setCacheRebalancingEnabled(cacheName, enabled);
   }

   @Override
   public RebalancingStatus getRebalancingStatus(String cacheName) throws Exception {
      return delegate.getRebalancingStatus(cacheName);
   }

   @Override
   public AvailabilityMode getCacheAvailability(String cacheName) {
      return delegate.getCacheAvailability(cacheName);
   }

   @Override
   public void setCacheAvailability(String cacheName, AvailabilityMode availabilityMode) throws Exception {
      delegate.setCacheAvailability(cacheName, availabilityMode);
   }

   // Arbitrary value, only need to start after JGroupsTransport
   @Start(priority = 100)
   public final void startDelegate() {
      if (delegate instanceof LocalTopologyManagerImpl) {
         ((LocalTopologyManagerImpl) delegate).start();
      }
   }

   // Need to stop before the JGroupsTransport
   @Stop(priority = 9)
   public final void stopDelegate() {
      if (delegate instanceof LocalTopologyManagerImpl) {
         ((LocalTopologyManagerImpl) delegate).stop();
      }
   }

   protected CompletionStage<Void> beforeHandleTopologyUpdate(String cacheName, CacheTopology cacheTopology, int viewId) {
      return CompletableFutures.completedNull();
   }

   protected CompletionStage<Void> beforeHandleRebalance(String cacheName, CacheTopology cacheTopology, int viewId) {
      return CompletableFutures.completedNull();
   }

   protected CompletionStage<Void> beforeConfirmRebalancePhase(String cacheName, int topologyId, Throwable throwable) {
      //no-op by default
      return CompletableFutures.completedNull();
   }

   @Override
   public PersistentUUID getPersistentUUID() {
      return delegate.getPersistentUUID();
   }

   @Override
   public void cacheShutdown(String name) {
      delegate.cacheShutdown(name);
   }

   @Override
   public CompletionStage<Void> handleCacheShutdown(String cacheName) {
      return delegate.handleCacheShutdown(cacheName);
   }

   @Override
   public CompletionStage<Void> stableTopologyCompletion(String cacheName) {
      return CompletableFutures.completedNull();
   }
}
