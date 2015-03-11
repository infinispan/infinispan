package org.infinispan.distribution.topologyaware;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.impl.DistributionManagerImpl;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.TopologyAwareAddress;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "distribution.topologyaware.TopologyInfoBroadcastTest")
public class TopologyInfoBroadcastTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      GlobalConfigurationBuilder gc1 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      updatedSiteInfo(gc1, "s0", "r0", "m0");
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(gc1, getClusterConfig());
      cacheManagers.add(cm1);

      GlobalConfigurationBuilder gc2 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      updatedSiteInfo(gc2, "s1", "r1", "m1");
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(gc2, getClusterConfig());
      cacheManagers.add(cm2);

      GlobalConfigurationBuilder gc3 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      updatedSiteInfo(gc3, "s2", "r2", "m2");
      EmbeddedCacheManager cm3 = TestCacheManagerFactory.createClusteredCacheManager(gc3, getClusterConfig());
      cacheManagers.add(cm3);

      log.info("Here it starts");
      waitForClusterToForm();
      log.info("Here it ends");
   }

   protected ConfigurationBuilder getClusterConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
   }

   private void updatedSiteInfo(GlobalConfigurationBuilder gcb, String s, String r, String m) {
      gcb.transport().siteId(s).rackId(r).machineId(m);
   }

   public void testIsReplicated() {
//      assert advancedCache(0).getDistributionManager().getConsistentHash() instanceof TopologyAwareConsistentHash;
//      assert advancedCache(1).getDistributionManager().getConsistentHash() instanceof TopologyAwareConsistentHash;
//      assert advancedCache(2).getDistributionManager().getConsistentHash() instanceof TopologyAwareConsistentHash;

      DistributionManagerImpl dmi = (DistributionManagerImpl) advancedCache(0).getDistributionManager();
      log.trace("distributionManager.ConsistentHash() = " + dmi.getWriteConsistentHash());
      assertTopologyInfo3Nodes();
      assertTopologyInfo3Nodes();
      assertTopologyInfo3Nodes();

      ConsistentHash tach0 = advancedCache(0).getDistributionManager().getWriteConsistentHash();
      ConsistentHash tach1 = advancedCache(1).getDistributionManager().getWriteConsistentHash();
      assertEquals(tach0.getMembers(), tach1.getMembers());
      ConsistentHash tach2 = advancedCache(2).getDistributionManager().getWriteConsistentHash();
      assertEquals(tach0.getMembers(), tach2.getMembers());
   }

   @Test(dependsOnMethods = "testIsReplicated")
   public void testNodeLeaves() {
      TestingUtil.killCacheManagers(manager(1));
      TestingUtil.blockUntilViewsReceived(60000, false, cache(0), cache(2));
      TestingUtil.waitForRehashToComplete(cache(0), cache(2));

      assertTopologyInfo2Nodes();
      assertTopologyInfo2Nodes();

      ConsistentHash tach0 = advancedCache(0).getDistributionManager().getWriteConsistentHash();
      ConsistentHash tach2 = advancedCache(2).getDistributionManager().getWriteConsistentHash();
      assertEquals(tach0.getMembers(), tach2.getMembers());
   }

   private void assertTopologyInfo3Nodes() {
      assertTopologyInfo2Nodes();
      TopologyAwareAddress address1 = (TopologyAwareAddress) address(1);
      assertEquals(address1.getSiteId(), "s1");
      assertEquals(address1.getRackId(), "r1");
      assertEquals(address1.getMachineId(), "m1");
   }

   private void assertTopologyInfo2Nodes() {
      TopologyAwareAddress address0 = (TopologyAwareAddress) address(0);
      assertEquals(address0.getSiteId(), "s0");
      assertEquals(address0.getRackId(), "r0");
      assertEquals(address0.getMachineId(), "m0");
      TopologyAwareAddress address2 = (TopologyAwareAddress) address(2);
      assertEquals(address2.getSiteId(), "s2");
      assertEquals(address2.getRackId(), "r2");
      assertEquals(address2.getMachineId(), "m2");
   }
}
