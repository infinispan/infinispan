package org.infinispan.statetransfer;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.jmx.PerThreadMBeanServerLookup;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.topology.ClusterTopologyManager;
import org.testng.annotations.Test;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.util.Arrays;

import static org.infinispan.test.TestingUtil.killCacheManagers;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

/**
 * @author Dan Berindei
 */
@Test(groups = "functional", testName = "statetransfer.RebalancePolicyJmxTest")
@CleanupAfterMethod
public class RebalancePolicyJmxTest extends MultipleCacheManagersTest {

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
      cb.clustering().cacheMode(CacheMode.DIST_SYNC)
            .stateTransfer().awaitInitialTransfer(awaitInitialTransfer);
      return cb;
   }

   private GlobalConfigurationBuilder getGlobalConfigurationBuilder(String rackId) {
      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb.globalJmxStatistics()
            .enable()
            .mBeanServerLookup(new PerThreadMBeanServerLookup())
            .transport().rackId(rackId);
      return gcb;
   }

   private void doTest(boolean awaitInitialTransfer) throws Exception {
      addClusterEnabledCacheManager(getGlobalConfigurationBuilder("r1"), getConfigurationBuilder(awaitInitialTransfer));
      addClusterEnabledCacheManager(getGlobalConfigurationBuilder("r1"), getConfigurationBuilder(awaitInitialTransfer));
      waitForClusterToForm();

      MBeanServer mBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      String domain0 = manager(1).getCacheManagerConfiguration().globalJmxStatistics().domain();
      ObjectName ltmName0 = TestingUtil.getCacheManagerObjectName(domain0, "DefaultCacheManager", "LocalTopologyManager");
      String domain1 = manager(1).getCacheManagerConfiguration().globalJmxStatistics().domain();
      ObjectName ltmName1 = TestingUtil.getCacheManagerObjectName(domain1, "DefaultCacheManager", "LocalTopologyManager");

      // Check initial state
      StateTransferManager stm0 = TestingUtil.extractComponent(cache(0), StateTransferManager.class);
      assertEquals(Arrays.asList(address(0), address(1)), stm0.getCacheTopology().getCurrentCH().getMembers());
      assertNull(stm0.getCacheTopology().getPendingCH());

      assertTrue(mBeanServer.isRegistered(ltmName0));
      assertTrue((Boolean) mBeanServer.getAttribute(ltmName0, "RebalancingEnabled"));

      // Suspend rebalancing
      mBeanServer.setAttribute(ltmName0, new Attribute("RebalancingEnabled", false));
      assertFalse((Boolean) mBeanServer.getAttribute(ltmName0, "RebalancingEnabled"));

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

      // Check that no rebalance happened after 1 second
      Thread.sleep(1000);
      assertFalse((Boolean) mBeanServer.getAttribute(ltmName1, "RebalancingEnabled"));
      assertNull(stm0.getCacheTopology().getPendingCH());
      assertEquals(Arrays.asList(address(0), address(1)), stm0.getCacheTopology().getCurrentCH().getMembers());

      // Re-enable rebalancing
      log.debugf("Rebalancing with nodes %s %s %s %s", address(0), address(1), address(2), address(3));
      mBeanServer.setAttribute(ltmName0, new Attribute("RebalancingEnabled", true));
      assertTrue((Boolean) mBeanServer.getAttribute(ltmName0, "RebalancingEnabled"));
      // Duplicate request to enable rebalancing - should be ignored
      mBeanServer.setAttribute(ltmName0, new Attribute("RebalancingEnabled", true));

      // Check that the cache now has 4 nodes, and the CH is balanced
      TestingUtil.waitForRehashToComplete(cache(0), cache(1), cache(2), cache(3));
      assertNull(stm0.getCacheTopology().getPendingCH());
      ConsistentHash ch = stm0.getCacheTopology().getCurrentCH();
      assertEquals(Arrays.asList(address(0), address(1), address(2), address(3)), ch.getMembers());
      for (int i = 0; i < ch.getNumSegments(); i++) {
         assertEquals(2, ch.locateOwnersForSegment(i).size());
      }

      // Suspend rebalancing again
      mBeanServer.setAttribute(ltmName1, new Attribute("RebalancingEnabled", false));
      assertFalse((Boolean) mBeanServer.getAttribute(ltmName0, "RebalancingEnabled"));
      assertFalse((Boolean) mBeanServer.getAttribute(ltmName1, "RebalancingEnabled"));
      // Duplicate request to enable rebalancing - should be ignored
      mBeanServer.setAttribute(ltmName1, new Attribute("RebalancingEnabled", false));

      // Kill the first 2 nodes
      log.debugf("Stopping nodes %s %s", address(0), address(1));
      killCacheManagers(manager(0), manager(1));

      // Check that the nodes are no longer in the CH, but every segment only has one copy
      // Implicitly, this also checks that no data has been lost - if both a segment's owners had left,
      // the CH factory would have assigned 2 owners.
      Thread.sleep(1000);
      StateTransferManager stm2 = TestingUtil.extractComponent(cache(2), StateTransferManager.class);
      assertNull(stm2.getCacheTopology().getPendingCH());
      ch = stm2.getCacheTopology().getCurrentCH();
      assertEquals(Arrays.asList(address(2), address(3)), ch.getMembers());
      for (int i = 0; i < ch.getNumSegments(); i++) {
         assertEquals(1, ch.locateOwnersForSegment(i).size());
      }

      // Enable rebalancing again
      log.debugf("Rebalancing with nodes %s %s", address(2), address(3));
      String domain2 = manager(2).getCacheManagerConfiguration().globalJmxStatistics().domain();
      ObjectName ltmName2 = TestingUtil.getCacheManagerObjectName(domain2, "DefaultCacheManager", "LocalTopologyManager");
      String domain3 = manager(2).getCacheManagerConfiguration().globalJmxStatistics().domain();
      ObjectName ltmName3 = TestingUtil.getCacheManagerObjectName(domain3, "DefaultCacheManager", "LocalTopologyManager");
      mBeanServer.setAttribute(ltmName2, new Attribute("RebalancingEnabled", true));
      assertTrue((Boolean) mBeanServer.getAttribute(ltmName2, "RebalancingEnabled"));
      assertTrue((Boolean) mBeanServer.getAttribute(ltmName3, "RebalancingEnabled"));

      // Check that the CH is now balanced (and every segment has 2 copies)
      TestingUtil.waitForRehashToComplete(cache(2), cache(3));
      assertNull(stm2.getCacheTopology().getPendingCH());
      ch = stm2.getCacheTopology().getCurrentCH();
      assertEquals(Arrays.asList(address(2), address(3)), ch.getMembers());
      for (int i = 0; i < ch.getNumSegments(); i++) {
         assertEquals(2, ch.locateOwnersForSegment(i).size());
      }
   }
}
