package org.infinispan.util;

import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheJoinInfo;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.CacheTopologyHandler;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.topology.LocalTopologyManagerImpl;
import org.infinispan.topology.ManagerStatusResponse;
import org.infinispan.topology.PersistentUUID;
import org.infinispan.topology.RebalancingStatus;

/**
 * Class to be extended to allow some control over the local topology manager when testing Infinispan.
 * <p/>
 * Note: create before/after method lazily when need.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public abstract class AbstractControlledLocalTopologyManager implements LocalTopologyManager {

   private final LocalTopologyManager delegate;

   protected AbstractControlledLocalTopologyManager(LocalTopologyManager delegate) {
      this.delegate = delegate;
   }

   @Override
   public final CacheTopology join(String cacheName, CacheJoinInfo joinInfo, CacheTopologyHandler stm, PartitionHandlingManager phm) throws Exception {
      return delegate.join(cacheName, joinInfo, stm, phm);
   }

   @Override
   public final void leave(String cacheName) {
      delegate.leave(cacheName);
   }

   @Override
   public final void confirmRebalancePhase(String cacheName, int topologyId, int rebalanceId, Throwable throwable) {
      beforeConfirmRebalancePhase(cacheName, topologyId, throwable);
      delegate.confirmRebalancePhase(cacheName, topologyId, rebalanceId, throwable);
   }

   @Override
   public final ManagerStatusResponse handleStatusRequest(int viewId) {
      return delegate.handleStatusRequest(viewId);
   }

   @Override
   public final void handleTopologyUpdate(String cacheName, CacheTopology cacheTopology,
         AvailabilityMode availabilityMode, int viewId, Address sender) throws InterruptedException {
      beforeHandleTopologyUpdate(cacheName, cacheTopology, viewId);
      delegate.handleTopologyUpdate(cacheName, cacheTopology, availabilityMode, viewId, sender);
   }

   @Override
   public final void handleRebalance(String cacheName, CacheTopology cacheTopology, int viewId, Address sender)
         throws InterruptedException {
      beforeHandleRebalance(cacheName, cacheTopology, viewId);
      delegate.handleRebalance(cacheName, cacheTopology, viewId, sender);
   }

   @Override
   public final CacheTopology getCacheTopology(String cacheName) {
      return delegate.getCacheTopology(cacheName);
   }

   @Override
   public void handleStableTopologyUpdate(String cacheName, CacheTopology cacheTopology, final Address sender,
         int viewId) {
      delegate.handleStableTopologyUpdate(cacheName, cacheTopology, sender, viewId);
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

   @Override
   public boolean isTotalOrderCache(String cacheName) {
      return delegate.isTotalOrderCache(cacheName);
   }

   protected void beforeHandleTopologyUpdate(String cacheName, CacheTopology cacheTopology, int viewId) {
      //no-op by default
   }

   protected void beforeHandleRebalance(String cacheName, CacheTopology cacheTopology, int viewId) {
      //no-op by default
   }

   protected void beforeConfirmRebalancePhase(String cacheName, int topologyId, Throwable throwable) {
      //no-op by default
   }

   @Override
   public PersistentUUID getPersistentUUID() {
      return delegate.getPersistentUUID();
   }

   @Override
   public void cacheShutdown(String name) throws Exception {
      delegate.cacheShutdown(name);
   }

   @Override
   public void handleCacheShutdown(String cacheName) {
      delegate.handleCacheShutdown(cacheName);
   }

}
