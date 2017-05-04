package org.infinispan.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.CacheJoinInfo;
import org.infinispan.topology.CacheStatusResponse;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.ClusterCacheStatus;
import org.infinispan.topology.ClusterTopologyManager;
import org.infinispan.topology.RebalancingStatus;

public class BlockingClusterTopologyManager implements ClusterTopologyManager {
   private final ClusterTopologyManager delegate;
   private final CopyOnWriteArrayList<Handle<CacheTopology>> topologyUpdates = new CopyOnWriteArrayList<>();
   private final CopyOnWriteArrayList<Handle<Integer>> topologyConfirmations = new CopyOnWriteArrayList<>();

   public static BlockingClusterTopologyManager replace(EmbeddedCacheManager cacheManager) {
      ClusterTopologyManager original = TestingUtil.extractGlobalComponent(cacheManager, ClusterTopologyManager.class);
      BlockingClusterTopologyManager bctm = new BlockingClusterTopologyManager(original);
      TestingUtil.replaceComponent(cacheManager, ClusterTopologyManager.class, bctm, true);

      try {
         Field cacheStatusMapField = original.getClass().getDeclaredField("cacheStatusMap");
         cacheStatusMapField.setAccessible(true);
         ConcurrentMap<String, ClusterCacheStatus> cacheStatusMap = (ConcurrentMap<String, ClusterCacheStatus>) cacheStatusMapField.get(original);

         Field clusterTopologyManagerField = ClusterCacheStatus.class.getDeclaredField("clusterTopologyManager");
         Field modifiers = Field.class.getDeclaredField("modifiers");
         modifiers.setAccessible(true);
         modifiers.setInt(clusterTopologyManagerField, clusterTopologyManagerField.getModifiers() & ~Modifier.FINAL);
         clusterTopologyManagerField.setAccessible(true);

         for (ClusterCacheStatus status : cacheStatusMap.values()) {
            clusterTopologyManagerField.set(status, bctm);
         }
      } catch (Exception e) {
         new IllegalStateException(e);
      }
      return bctm;
   }

   protected BlockingClusterTopologyManager(ClusterTopologyManager delegate) {
      this.delegate = delegate;
   }

   @Override
   public CacheStatusResponse handleJoin(String cacheName, Address joiner, CacheJoinInfo joinInfo, int viewId) throws Exception {
      return delegate.handleJoin(cacheName, joiner, joinInfo, viewId);
   }

   @Override
   public void handleLeave(String cacheName, Address leaver, int viewId) throws Exception {
      delegate.handleLeave(cacheName, leaver, viewId);
   }

   public Handle<Integer> startBlockingTopologyConfirmations(Predicate<Integer> condition) {
      Handle<Integer> handle = new Handle<>(condition);
      topologyConfirmations.add(handle);
      return handle;
   }

   @Override
   public void handleRebalancePhaseConfirm(String cacheName, Address node, int topologyId, Throwable throwable, int viewId) throws Exception {
      for (Handle<Integer> h : topologyConfirmations) {
         if (h.condition.test(topologyId)) {
            h.latch.blockIfNeeded();
         }
      }
      delegate.handleRebalancePhaseConfirm(cacheName, node, topologyId, throwable, viewId);
   }

   @Override
   public void broadcastRebalanceStart(String cacheName, CacheTopology cacheTopology, boolean totalOrder, boolean distributed) {
      delegate.broadcastRebalanceStart(cacheName, cacheTopology, totalOrder, distributed);
   }

   public Handle<CacheTopology> startBlockingTopologyUpdate(Predicate<CacheTopology> condition) {
      Handle<CacheTopology> handle = new Handle<>(condition);
      topologyUpdates.add(handle);
      return handle;
   }

   @Override
   public void broadcastTopologyUpdate(String cacheName, CacheTopology cacheTopology, AvailabilityMode availabilityMode, boolean totalOrder, boolean distributed) {
      for (Handle<CacheTopology> h : topologyUpdates) {
         if (h.condition.test(cacheTopology)) {
            h.latch.blockIfNeeded();
         }
      }
      delegate.broadcastTopologyUpdate(cacheName, cacheTopology, availabilityMode, totalOrder, distributed);
   }

   @Override
   public void broadcastStableTopologyUpdate(String cacheName, CacheTopology cacheTopology, boolean totalOrder, boolean distributed) {
      delegate.broadcastStableTopologyUpdate(cacheName, cacheTopology, totalOrder, distributed);
   }

   @Override
   public boolean isRebalancingEnabled() {
      return delegate.isRebalancingEnabled();
   }

   @Override
   public boolean isRebalancingEnabled(String cacheName) {
      return delegate.isRebalancingEnabled(cacheName);
   }

   @Override
   public void setRebalancingEnabled(boolean enabled) {
      delegate.setRebalancingEnabled(enabled);
   }

   @Override
   public void setRebalancingEnabled(String cacheName, boolean enabled) {
      delegate.setRebalancingEnabled(cacheName, enabled);
   }

   @Override
   public RebalancingStatus getRebalancingStatus(String cacheName) {
      return delegate.getRebalancingStatus(cacheName);
   }

   @Override
   public void forceRebalance(String cacheName) {
      delegate.forceRebalance(cacheName);
   }

   @Override
   public void forceAvailabilityMode(String cacheName, AvailabilityMode availabilityMode) {
      delegate.forceAvailabilityMode(cacheName, availabilityMode);
   }

   @Override
   public void handleShutdownRequest(String cacheName) throws Exception {
      delegate.handleShutdownRequest(cacheName);
   }

   @Override
   public void broadcastShutdownCache(String cacheName, CacheTopology currentTopology, boolean totalOrder, boolean distributed) throws Exception {
      delegate.broadcastShutdownCache(cacheName, currentTopology, totalOrder, distributed);
   }

   public class Handle<T> {
      private final Predicate<T> condition;
      private final NotifierLatch latch = new NotifierLatch();

      public Handle(Predicate<T> condition) {
         this.condition = condition;
         latch.startBlocking();
      }

      public void stopBlocking() {
         latch.stopBlocking();
         topologyUpdates.remove(this);
      }

      public void waitToBlock() throws InterruptedException {
         latch.waitToBlock();
      }

      public void unblockOnce() {
         latch.unblockOnce();
      }
   }
}
