package org.infinispan.topology;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

/**
 * A rebalance start and a topology update are broadcast with {@link org.infinispan.remoting.inboundhandler.DeliverOrder#NONE},
 * so a node can receive them in either order. When the rebalance start wins the race it installs a topology newer than
 * the one in the topology update, and the topology update is then discarded as stale. Tests that the availability mode
 * the topology update was carrying is not lost in that case, because the rebalance start carries it as well.
 *
 * @since 16.3
 */
@Test(groups = "functional", testName = "topology.RebalanceStartAvailabilityModeTest")
@CleanupAfterMethod
public class RebalanceStartAvailabilityModeTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.clustering().partitionHandling().whenSplit(PartitionHandling.DENY_READ_WRITES);
      createCluster(builder, 2);
      waitForClusterToForm();
   }

   /**
    * The coordinator leaves degraded mode, but the rebalance start overtakes the topology update announcing it.
    */
   public void testAvailabilityModeNotLostWhenRebalanceStartArrivesFirst() throws Exception {
      degradeCluster();

      // The topology update the coordinator sent before the rebalance start, which will arrive late and be discarded
      CacheTopology chUpdate = nextTopology(1, CacheTopology.Phase.NO_REBALANCE);
      // The rebalance start, which overtook it
      CacheTopology rebalanceStart = nextTopology(2, CacheTopology.Phase.READ_OLD_WRITE_ALL);

      deliverRebalance(rebalanceStart, AvailabilityMode.AVAILABLE);
      deliverTopologyUpdate(chUpdate, AvailabilityMode.AVAILABLE);

      assertEquals(AvailabilityMode.AVAILABLE, phm(1).getAvailabilityMode(),
                   "The rebalance start should have applied the availability mode, " +
                   "the topology update that carried it was discarded as stale");
   }

   /**
    * A coordinator older than 16.3 does not send an availability mode with the rebalance start. The receiver must then
    * keep the mode it already has rather than assume the default.
    */
   public void testAvailabilityModeUnchangedWhenRebalanceStartHasNoMode() throws Exception {
      degradeCluster();

      deliverRebalance(nextTopology(1, CacheTopology.Phase.READ_OLD_WRITE_ALL), null);

      assertEquals(AvailabilityMode.DEGRADED_MODE, phm(1).getAvailabilityMode(),
                   "A rebalance start without an availability mode must not change the current one");
   }

   private void degradeCluster() throws Exception {
      ltm(0).setCacheAvailability(cacheName(), AvailabilityMode.DEGRADED_MODE);
      eventuallyEquals(AvailabilityMode.DEGRADED_MODE, () -> phm(1).getAvailabilityMode());
   }

   /**
    * Builds a topology {@code offset} ids after the one node 1 currently has. The consistent hashes are left untouched
    * so that the rebalance does not actually transfer any state.
    */
   private CacheTopology nextTopology(int offset, CacheTopology.Phase phase) {
      CacheTopology current = ltm(1).getCacheTopology(cacheName());
      boolean rebalance = phase != CacheTopology.Phase.NO_REBALANCE;
      return new CacheTopology(current.getTopologyId() + offset,
                               current.getRebalanceId() + (rebalance ? 1 : 0),
                               current.getCurrentCH(),
                               rebalance ? current.getCurrentCH() : null,
                               phase, current.getActualMembers(), current.getMembersPersistentUUIDs());
   }

   private void deliverRebalance(CacheTopology topology, AvailabilityMode availabilityMode) {
      Transport transport = transport();
      CompletionStages.join(ltm(1).handleRebalance(cacheName(), topology, availabilityMode, transport.getViewId(),
                                                   transport.getCoordinator()));
   }

   private void deliverTopologyUpdate(CacheTopology topology, AvailabilityMode availabilityMode) {
      Transport transport = transport();
      CompletionStages.join(ltm(1).handleTopologyUpdate(cacheName(), topology, availabilityMode, transport.getViewId(),
                                                        transport.getCoordinator()));
   }

   private Transport transport() {
      return TestingUtil.extractGlobalComponent(manager(1), Transport.class);
   }

   private LocalTopologyManager ltm(int index) {
      return TestingUtil.extractGlobalComponent(manager(index), LocalTopologyManager.class);
   }

   private PartitionHandlingManager phm(int index) {
      return TestingUtil.extractComponent(cache(index), PartitionHandlingManager.class);
   }

   private String cacheName() {
      return TestingUtil.getDefaultCacheName(manager(0));
   }
}
