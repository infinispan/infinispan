package org.infinispan.topology;

import static java.util.Collections.singletonMap;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Mock implementation of {@link LocalTopologyManager} with a single cache.
 *
 * @author Dan Berindei
 * @since 9.2
 */
class MockLocalTopologyManager implements LocalTopologyManager {
   private static final Log log = LogFactory.getLog(MockLocalTopologyManager.class);

   private final String cacheName;
   private final BlockingQueue<CacheTopology> topologies = new LinkedBlockingDeque<>();

   private CacheStatusResponse status;


   MockLocalTopologyManager(String cacheName) {
      this.cacheName = cacheName;
   }

   public void init(CacheJoinInfo joinInfo, CacheTopology topology, CacheTopology stableTopology,
                    AvailabilityMode availabilityMode) {
      this.status = new CacheStatusResponse(joinInfo, topology, stableTopology, availabilityMode, Collections.emptyList());
   }

   public void verifyTopology(CacheTopology topology, int topologyId, List<Address> currentMembers,
                              List<Address> pendingMembers, CacheTopology.Phase phase) {
      log.debugf("Verifying topology %s", topology);
      assertEquals(topologyId, topology.getTopologyId());
      assertEquals(phase, topology.getPhase());
      assertEquals(currentMembers, topology.getCurrentCH().getMembers());
      if (pendingMembers != null) {
         assertEquals(pendingMembers, topology.getPendingCH().getMembers());
      } else {
         assertNull(topology.getPendingCH());
      }
   }

   public void expectTopology(int topologyId, List<Address> currentMembers, List<Address> pendingMembers,
                              CacheTopology.Phase phase) throws Exception {
      CacheTopology topology = topologies.poll(10, TimeUnit.SECONDS);
      assertNotNull("Timed out waiting for topology " + topologyId, topology);

      verifyTopology(topology, topologyId, currentMembers, pendingMembers, phase);
   }

   @Override
   public CompletionStage<CacheTopology> join(String cacheName, CacheJoinInfo joinInfo, CacheTopologyHandler stm,
                                              PartitionHandlingManager phm) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void leave(String cacheName, long timeout) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void confirmRebalancePhase(String cacheName, int topologyId, int rebalanceId, Throwable throwable) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<ManagerStatusResponse> handleStatusRequest(int viewId) {
      Map<String, CacheStatusResponse> caches = status.getCacheJoinInfo() != null ?
                                                singletonMap(cacheName, status) :
                                                Collections.emptyMap();
      return CompletableFuture.completedFuture(new ManagerStatusResponse(caches, true));
   }

   @Override
   public CompletionStage<Void> handleTopologyUpdate(String cacheName, CacheTopology cacheTopology, AvailabilityMode availabilityMode,
                                                     int viewId, Address sender) {
      status = new CacheStatusResponse(status.getCacheJoinInfo(), cacheTopology,
                                       status.getStableTopology(), availabilityMode, status.joinedMembers());
      topologies.add(cacheTopology);
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> handleStableTopologyUpdate(String cacheName, CacheTopology cacheTopology, Address sender, int viewId) {
      status = new CacheStatusResponse(status.getCacheJoinInfo(), status.getCacheTopology(),
                                       cacheTopology, status.getAvailabilityMode(), status.joinedMembers());
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Void> handleRebalance(String cacheName, CacheTopology cacheTopology, int viewId, Address sender) {
      status = new CacheStatusResponse(status.getCacheJoinInfo(), cacheTopology,
                                       status.getStableTopology(), status.getAvailabilityMode(), status.joinedMembers());
      topologies.add(cacheTopology);
      return CompletableFutures.completedNull();
   }

   @Override
   public CacheTopology getCacheTopology(String cacheName) {
      return status.getCacheTopology();
   }

   @Override
   public CacheTopology getStableCacheTopology(String cacheName) {
      return status.getStableTopology();
   }

   @Override
   public boolean isRebalancingEnabled() {
      return true;
   }

   @Override
   public boolean isCacheRebalancingEnabled(String cacheName) {
      return true;
   }

   @Override
   public void setRebalancingEnabled(boolean enabled) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void setCacheRebalancingEnabled(String cacheName, boolean enabled) {
      throw new UnsupportedOperationException();
   }

   @Override
   public RebalancingStatus getRebalancingStatus(String cacheName) {
      throw new UnsupportedOperationException();
   }

   @Override
   public AvailabilityMode getCacheAvailability(String cacheName) {
      return status.getAvailabilityMode();
   }

   @Override
   public void setCacheAvailability(String cacheName, AvailabilityMode availabilityMode) {
      throw new UnsupportedOperationException();
   }

   @Override
   public PersistentUUID getPersistentUUID() {
      return null;
   }

   @Override
   public void cacheShutdown(String name) {
      throw new UnsupportedOperationException();
   }

    @Override
   public CompletionStage<Void> handleCacheShutdown(String cacheName) {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Void> stableTopologyCompletion(String cacheName) {
      return CompletableFutures.completedNull();
   }
}
