package org.infinispan.util;

import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.topology.CacheJoinInfo;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.CacheTopologyHandler;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.topology.LocalTopologyManagerImpl;

import java.util.Map;

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
   public final CacheTopology join(String cacheName, CacheJoinInfo joinInfo, CacheTopologyHandler stm) throws Exception {
      return delegate.join(cacheName, joinInfo, stm);
   }

   @Override
   public final void leave(String cacheName) {
      delegate.leave(cacheName);
   }

   @Override
   public final void confirmRebalance(String cacheName, int topologyId, Throwable throwable) {
      beforeConfirmRebalance(cacheName, topologyId, throwable);
      delegate.confirmRebalance(cacheName, topologyId, throwable);
   }

   @Override
   public final Map<String, Object[]> handleStatusRequest(int viewId) {
      return delegate.handleStatusRequest(viewId);
   }

   @Override
   public final void handleConsistentHashUpdate(String cacheName, CacheTopology cacheTopology, int viewId) throws InterruptedException {
      beforeHandleConsistentHashUpdate(cacheName, cacheTopology, viewId);
      delegate.handleConsistentHashUpdate(cacheName, cacheTopology, viewId);
   }

   @Override
   public final void handleRebalance(String cacheName, CacheTopology cacheTopology, int viewId) throws InterruptedException {
      beforeHandleRebalance(cacheName, cacheTopology, viewId);
      delegate.handleRebalance(cacheName, cacheTopology, viewId);
   }

   @Override
   public final CacheTopology getCacheTopology(String cacheName) {
      return delegate.getCacheTopology(cacheName);
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

   protected void beforeHandleConsistentHashUpdate(String cacheName, CacheTopology cacheTopology, int viewId) {
      //no-op by default
   }

   protected void beforeHandleRebalance(String cacheName, CacheTopology cacheTopology, int viewId) {
      //no-op by default
   }

   protected void beforeConfirmRebalance(String cacheName, int topologyId, Throwable throwable) {
      //no-op by default
   }
}
