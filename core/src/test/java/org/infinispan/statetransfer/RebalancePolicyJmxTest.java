package org.infinispan.statetransfer;

import static org.infinispan.test.TestingUtil.killCacheManagers;
import static org.infinispan.test.fwk.TestCacheManagerFactory.configureJmx;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.InCacheMode;
import org.infinispan.topology.ClusterTopologyManager;
import org.infinispan.topology.RebalancingStatus;
import org.testng.annotations.Test;

/**
 * @author Dan Berindei
 */
@Test(groups = "functional", testName = "statetransfer.RebalancePolicyJmxTest")
@CleanupAfterMethod
@InCacheMode({ CacheMode.DIST_SYNC })
public class RebalancePolicyJmxTest extends MultipleCacheManagersTest {

   private static final String REBALANCING_ENABLED = "rebalancingEnabled";

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   public void testJoinAndLeaveWithRebalanceSuspended() throws Exception {
      doTest(false);
   }

   public void testJoinAndLeaveWithRebalanceSuspendedAwaitingInitialTransfer() throws Exception {
      doTest(true);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      //no-op
   }

   private ConfigurationBuilder getConfigurationBuilder(boolean awaitInitialTransfer) {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(cacheMode)
            .stateTransfer().awaitInitialTransfer(awaitInitialTransfer);
      return cb;
   }

   private GlobalConfigurationBuilder getGlobalConfigurationBuilder(String rackId) {
      int index = cacheManagers.size();
      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb.transport().rackId(rackId);
      configureJmx(gcb, getClass().getSimpleName() + index, mBeanServerLookup);
      return gcb;
   }

   private void doTest(boolean awaitInitialTransfer) throws Exception {
      addClusterEnabledCacheManager(getGlobalConfigurationBuilder("r1"), getConfigurationBuilder(awaitInitialTransfer));
      addClusterEnabledCacheManager(getGlobalConfigurationBuilder("r1"), getConfigurationBuilder(awaitInitialTransfer));
      waitForClusterToForm();

      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      String domain0 = manager(1).getCacheManagerConfiguration().jmx().domain();
      ObjectName ltmName0 = TestingUtil.getCacheManagerObjectName(domain0, "DefaultCacheManager", "LocalTopologyManager");
      String domain1 = manager(1).getCacheManagerConfiguration().jmx().domain();
      ObjectName ltmName1 = TestingUtil.getCacheManagerObjectName(domain1, "DefaultCacheManager", "LocalTopologyManager");

      // Check initial state
      DistributionManager dm0 = advancedCache(0).getDistributionManager();
      assertEquals(Arrays.asList(address(0), address(1)), dm0.getCacheTopology().getCurrentCH().getMembers());
      assertNull(dm0.getCacheTopology().getPendingCH());

      assertTrue(mBeanServer.isRegistered(ltmName0));
      assertTrue((Boolean) mBeanServer.getAttribute(ltmName0, REBALANCING_ENABLED));

      // Suspend rebalancing
      mBeanServer.setAttribute(ltmName0, new Attribute(REBALANCING_ENABLED, false));
      assertFalse((Boolean) mBeanServer.getAttribute(ltmName0, REBALANCING_ENABLED));

      // Add 2 nodes
      log.debugf("Starting 2 new nodes");
      addClusterEnabledCacheManager(getGlobalConfigurationBuilder("r2"), getConfigurationBuilder(awaitInitialTransfer));
      addClusterEnabledCacheManager(getGlobalConfigurationBuilder("r2"), getConfigurationBuilder(awaitInitialTransfer));
      cache(2);
      cache(3);

      // Check that rebalance is suspended on the new nodes
      ClusterTopologyManager ctm2 = TestingUtil.extractGlobalComponent(manager(2), ClusterTopologyManager.class);
      assertFalse(ctm2.isRebalancingEnabled());
      ClusterTopologyManager ctm3 = TestingUtil.extractGlobalComponent(manager(3), ClusterTopologyManager.class);
      assertFalse(ctm3.isRebalancingEnabled());
      StateTransferManager stm0 = TestingUtil.extractComponent(cache(0), StateTransferManager.class);
      assertEquals(RebalancingStatus.SUSPENDED.toString(), stm0.getRebalancingStatus());

      // Check that no rebalance happened after 1 second
      Thread.sleep(1000);
      assertFalse((Boolean) mBeanServer.getAttribute(ltmName1, REBALANCING_ENABLED));
      assertNull(dm0.getCacheTopology().getPendingCH());
      assertEquals(Arrays.asList(address(0), address(1)), dm0.getCacheTopology().getCurrentCH().getMembers());

      // Re-enable rebalancing
      log.debugf("Rebalancing with nodes %s %s %s %s", address(0), address(1), address(2), address(3));
      mBeanServer.setAttribute(ltmName0, new Attribute(REBALANCING_ENABLED, true));
      assertTrue((Boolean) mBeanServer.getAttribute(ltmName0, REBALANCING_ENABLED));
      // Duplicate request to enable rebalancing - should be ignored
      mBeanServer.setAttribute(ltmName0, new Attribute(REBALANCING_ENABLED, true));

      // Check that the cache now has 4 nodes, and the CH is balanced
      TestingUtil.waitForNoRebalance(cache(0), cache(1), cache(2), cache(3));
      assertNull(dm0.getCacheTopology().getPendingCH());
      assertEquals(RebalancingStatus.COMPLETE.toString(), stm0.getRebalancingStatus());
      ConsistentHash ch = dm0.getCacheTopology().getCurrentCH();
      assertEquals(Arrays.asList(address(0), address(1), address(2), address(3)), ch.getMembers());
      int numOwners = Math.min(cache(0).getCacheConfiguration().clustering().hash().numOwners(), ch.getMembers().size());
      for (int i = 0; i < ch.getNumSegments(); i++) {
         assertEquals(numOwners, ch.locateOwnersForSegment(i).size());
      }

      // Suspend rebalancing again
      mBeanServer.setAttribute(ltmName1, new Attribute(REBALANCING_ENABLED, false));
      assertFalse((Boolean) mBeanServer.getAttribute(ltmName0, REBALANCING_ENABLED));
      assertFalse((Boolean) mBeanServer.getAttribute(ltmName1, REBALANCING_ENABLED));
      // Duplicate request to enable rebalancing - should be ignored
      mBeanServer.setAttribute(ltmName1, new Attribute(REBALANCING_ENABLED, false));

      // Kill the first 2 nodes
      log.debugf("Stopping nodes %s %s", address(0), address(1));
      killCacheManagers(manager(0), manager(1));

      // Check that the nodes are no longer in the CH, but every segment only has one copy
      // Implicitly, this also checks that no data has been lost - if both a segment's owners had left,
      // the CH factory would have assigned 2 owners.
      Thread.sleep(1000);
      DistributionManager dm2 = advancedCache(2).getDistributionManager();
      assertNull(dm2.getCacheTopology().getPendingCH());
      ch = dm2.getCacheTopology().getCurrentCH();
      assertEquals(Arrays.asList(address(2), address(3)), ch.getMembers());
      // Scattered cache cannot reliably tolerate failure of two nodes, some segments may get lost
      if (cacheMode.isDistributed()) {
         for (int i = 0; i < ch.getNumSegments(); i++) {
            assertEquals(1, ch.locateOwnersForSegment(i).size());
         }
      }
      StateTransferManager stm2 = TestingUtil.extractComponent(cache(2), StateTransferManager.class);
      assertEquals(RebalancingStatus.SUSPENDED.toString(), stm2.getRebalancingStatus());

      // Enable rebalancing again
      log.debugf("Rebalancing with nodes %s %s", address(2), address(3));
      String domain2 = manager(2).getCacheManagerConfiguration().jmx().domain();
      ObjectName ltmName2 = TestingUtil.getCacheManagerObjectName(domain2, "DefaultCacheManager", "LocalTopologyManager");
      String domain3 = manager(2).getCacheManagerConfiguration().jmx().domain();
      ObjectName ltmName3 = TestingUtil.getCacheManagerObjectName(domain3, "DefaultCacheManager", "LocalTopologyManager");
      mBeanServer.setAttribute(ltmName2, new Attribute(REBALANCING_ENABLED, true));
      assertTrue((Boolean) mBeanServer.getAttribute(ltmName2, REBALANCING_ENABLED));
      assertTrue((Boolean) mBeanServer.getAttribute(ltmName3, REBALANCING_ENABLED));

      // Check that the CH is now balanced (and every segment has 2 copies)
      TestingUtil.waitForNoRebalance(cache(2), cache(3));
      assertEquals(RebalancingStatus.COMPLETE.toString(), stm2.getRebalancingStatus());
      assertNull(dm2.getCacheTopology().getPendingCH());
      ch = dm2.getCacheTopology().getCurrentCH();
      assertEquals(Arrays.asList(address(2), address(3)), ch.getMembers());
      for (int i = 0; i < ch.getNumSegments(); i++) {
         assertEquals(numOwners, ch.locateOwnersForSegment(i).size());
      }
   }
}
