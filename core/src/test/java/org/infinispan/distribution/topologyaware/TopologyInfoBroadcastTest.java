package org.infinispan.distribution.topologyaware;

import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.distribution.DistributionManagerImpl;
import org.infinispan.distribution.ch.NodeTopologyInfo;
import org.infinispan.distribution.ch.TopologyAwareConsistentHash;
import org.infinispan.distribution.ch.TopologyInfo;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "distribution.TopologyInfoBroadcastTest")
public class TopologyInfoBroadcastTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(getClusterConfig(), 3);
      updatedSiteInfo(manager(0), "s0", "r0", "m0");
      updatedSiteInfo(manager(1), "s1", "r1", "m1");
      updatedSiteInfo(manager(2), "s2", "r2", "m2");
      log.info("Here it starts");
      BaseDistFunctionalTest.RehashWaiter.waitForInitRehashToComplete(cache(0), cache(1), cache(2));
      log.info("Here it ends");
   }

   protected Configuration getClusterConfig() {
      return getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC);
   }

   private void updatedSiteInfo(EmbeddedCacheManager embeddedCacheManager, String s, String r, String m) {
      GlobalConfiguration gc = embeddedCacheManager.getGlobalConfiguration();
      gc.setSiteId(s);
      gc.setRackId(r);
      gc.setMachineId(m);
   }

   @Test(enabled = false, description = "Waiting on push-based rehash")
   public void testIsReplicated() {
      assert advancedCache(0).getDistributionManager().getConsistentHash() instanceof TopologyAwareConsistentHash;
      assert advancedCache(1).getDistributionManager().getConsistentHash() instanceof TopologyAwareConsistentHash;
      assert advancedCache(2).getDistributionManager().getConsistentHash() instanceof TopologyAwareConsistentHash;

      DistributionManagerImpl dmi = (DistributionManagerImpl) advancedCache(0).getDistributionManager();
      System.out.println("distributionManager.getTopologyInfo() = " + dmi.getTopologyInfo());
      assertTopologyInfo3Nodes(dmi.getTopologyInfo());
      dmi = (DistributionManagerImpl) advancedCache(1).getDistributionManager();
      assertTopologyInfo3Nodes(dmi.getTopologyInfo());
      dmi = (DistributionManagerImpl) advancedCache(2).getDistributionManager();
      assertTopologyInfo3Nodes(dmi.getTopologyInfo());

      TopologyAwareConsistentHash tach = (TopologyAwareConsistentHash) advancedCache(0).getDistributionManager().getConsistentHash();
      assertEquals(tach.getTopologyInfo(), dmi.getTopologyInfo());
      tach = (TopologyAwareConsistentHash) advancedCache(1).getDistributionManager().getConsistentHash();
      assertEquals(tach.getTopologyInfo(), dmi.getTopologyInfo());
      tach = (TopologyAwareConsistentHash) advancedCache(2).getDistributionManager().getConsistentHash();
      assertEquals(tach.getTopologyInfo(), dmi.getTopologyInfo());
   }

   @Test(dependsOnMethods = "testIsReplicated", enabled = false, description = "Waiting on push-based rehash")
   public void testNodeLeaves() {
      TestingUtil.killCacheManagers(manager(1));
      BaseDistFunctionalTest.RehashWaiter.waitForInitRehashToComplete(cache(0), cache(2));

      DistributionManagerImpl dmi = (DistributionManagerImpl) advancedCache(0).getDistributionManager();
      assertTopologyInfo2Nodes(dmi.getTopologyInfo());
      dmi = (DistributionManagerImpl) advancedCache(2).getDistributionManager();
      assertTopologyInfo2Nodes(dmi.getTopologyInfo());

      TopologyAwareConsistentHash tach = (TopologyAwareConsistentHash) advancedCache(0).getDistributionManager().getConsistentHash();
      assertEquals(tach.getTopologyInfo(), dmi.getTopologyInfo());
      tach = (TopologyAwareConsistentHash) advancedCache(2).getDistributionManager().getConsistentHash();
      assertEquals(tach.getTopologyInfo(), dmi.getTopologyInfo());
   }

   private void assertTopologyInfo3Nodes(TopologyInfo topologyInfo) {
      assertTopologyInfo2Nodes(topologyInfo);
      assertEquals(topologyInfo.getNodeTopologyInfo(address(1)), new NodeTopologyInfo("m1","r1", "s1", address(1)));
   }

   private void assertTopologyInfo2Nodes(TopologyInfo topologyInfo) {
      assertEquals(topologyInfo.getNodeTopologyInfo(address(0)), new NodeTopologyInfo("m0","r0", "s0", address(0)));
      assertEquals(topologyInfo.getNodeTopologyInfo(address(2)), new NodeTopologyInfo("m2","r2", "s2", address(2)));
   }
}
