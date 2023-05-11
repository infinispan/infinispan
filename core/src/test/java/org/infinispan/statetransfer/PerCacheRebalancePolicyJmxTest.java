package org.infinispan.statetransfer;

import static org.infinispan.test.fwk.TestCacheManagerFactory.configureJmx;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.List;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.Cache;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.InCacheMode;
import org.infinispan.topology.ClusterTopologyManager;
import org.testng.annotations.Test;

/**
 * @author Dan Berindei
 * @author Tristan Tarrant
 */
@Test(groups = "functional", testName = "statetransfer.PerCacheRebalancePolicyJmxTest")
@CleanupAfterMethod
@InCacheMode({CacheMode.DIST_SYNC})
public class PerCacheRebalancePolicyJmxTest extends MultipleCacheManagersTest {

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

   private void addNode(GlobalConfigurationBuilder gcb, ConfigurationBuilder builder) {
      EmbeddedCacheManager cacheManager = addClusterEnabledCacheManager(gcb, builder);
      cacheManager.defineConfiguration("a", builder.build());
      cacheManager.defineConfiguration("b", builder.build());
   }

   private void doTest(boolean awaitInitialTransfer) throws Exception {
      ConfigurationBuilder builder = getConfigurationBuilder(awaitInitialTransfer);
      addNode(getGlobalConfigurationBuilder("r1"), builder);
      addNode(getGlobalConfigurationBuilder("r1"), builder);
      waitForClusterToForm("a", "b");

      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      String domain0 = manager(1).getCacheManagerConfiguration().jmx().domain();
      ObjectName ltmName0 = TestingUtil.getCacheManagerObjectName(domain0, "DefaultCacheManager", "LocalTopologyManager");
      String domain1 = manager(1).getCacheManagerConfiguration().jmx().domain();
      ObjectName ltmName1 = TestingUtil.getCacheManagerObjectName(domain1, "DefaultCacheManager", "LocalTopologyManager");

      ObjectName jmxCacheA = TestingUtil.getCacheObjectName(domain0, "a(" + cacheMode.name().toLowerCase() + ")");
      ObjectName jmxCacheB = TestingUtil.getCacheObjectName(domain0, "b(" + cacheMode.name().toLowerCase() + ")");

      // Check initial state
      DistributionManager dm0a = advancedCache(0, "a").getDistributionManager();
      assertEquals(Arrays.asList(address(0), address(1)), dm0a.getCacheTopology().getCurrentCH().getMembers());
      assertNull(dm0a.getCacheTopology().getPendingCH());

      DistributionManager dm0b = advancedCache(0, "b").getDistributionManager();
      assertEquals(Arrays.asList(address(0), address(1)), dm0b.getCacheTopology().getCurrentCH().getMembers());
      assertNull(dm0b.getCacheTopology().getPendingCH());

      assertTrue(mBeanServer.isRegistered(ltmName0));
      assertTrue((Boolean) mBeanServer.getAttribute(ltmName0, REBALANCING_ENABLED));

      // Suspend global rebalancing
      mBeanServer.setAttribute(ltmName0, new Attribute(REBALANCING_ENABLED, false));
      assertFalse((Boolean) mBeanServer.getAttribute(ltmName0, REBALANCING_ENABLED));

      // Add 2 nodes
      log.debugf("Starting 2 new nodes");
      addNode(getGlobalConfigurationBuilder("r2"), builder);
      addNode(getGlobalConfigurationBuilder("r2"), builder);

      // Ensure the caches are started on all nodes
      TestingUtil.blockUntilViewsReceived(3000, getCaches("a"));
      TestingUtil.blockUntilViewsReceived(3000, getCaches("b"));

      // Check that rebalance is suspended on the new nodes
      ClusterTopologyManager ctm2 = TestingUtil.extractGlobalComponent(manager(2), ClusterTopologyManager.class);
      assertFalse(ctm2.isRebalancingEnabled());
      ClusterTopologyManager ctm3 = TestingUtil.extractGlobalComponent(manager(3), ClusterTopologyManager.class);
      assertFalse(ctm3.isRebalancingEnabled());

      // Check that no rebalance happened after 1 second
      Thread.sleep(1000);
      assertFalse((Boolean) mBeanServer.getAttribute(ltmName1, REBALANCING_ENABLED));
      assertNull(dm0a.getCacheTopology().getPendingCH());
      assertEquals(Arrays.asList(address(0), address(1)), dm0a.getCacheTopology().getCurrentCH().getMembers());

      // Disable rebalancing for cache b
      mBeanServer.setAttribute(jmxCacheB, new Attribute(REBALANCING_ENABLED, false));

      // Re-enable global rebalancing
      log.debugf("Rebalancing with nodes %s %s %s %s", address(0), address(1), address(2), address(3));
      mBeanServer.setAttribute(ltmName0, new Attribute(REBALANCING_ENABLED, true));
      assertTrue((Boolean) mBeanServer.getAttribute(ltmName0, REBALANCING_ENABLED));

      checkRehashed(dm0a, getCaches("a"), Arrays.asList(address(0), address(1), address(2), address(3)));

      // Check that cache "b" still has rebalancing disabled
      assertFalse((Boolean)mBeanServer.getAttribute(jmxCacheB, REBALANCING_ENABLED));
      assertEquals(Arrays.asList(address(0), address(1)), dm0b.getCacheTopology().getCurrentCH().getMembers());

      // Enable rebalancing for cache b
      mBeanServer.setAttribute(jmxCacheB, new Attribute(REBALANCING_ENABLED, true));
      // Check that cache "b" now has 4 nodes, and the CH is balanced
      checkRehashed(dm0b, getCaches("b"), Arrays.asList(address(0), address(1), address(2), address(3)));
   }

   private void checkRehashed(DistributionManager dm, List<Cache<Object,Object>> caches, List<Address> addresses) {
      TestingUtil.waitForNoRebalance(caches);
      assertNull(dm.getCacheTopology().getPendingCH());
      LocalizedCacheTopology topology = dm.getCacheTopology();
      ConsistentHash ch = topology.getCurrentCH();
      assertEquals(addresses, ch.getMembers());
      int numOwners = Math.min(caches.get(0).getCacheConfiguration().clustering().hash().numOwners(), ch.getMembers().size());
      for (int i = 0; i < ch.getNumSegments(); i++) {
         assertEquals(numOwners, ch.locateOwnersForSegment(i).size());
      }
   }
}
