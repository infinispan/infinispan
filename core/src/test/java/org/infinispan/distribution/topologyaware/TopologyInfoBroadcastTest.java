package org.infinispan.distribution.topologyaware;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.impl.DistributionManagerImpl;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

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
      log.trace("distributionManager.ConsistentHash() = " + dmi.getCacheTopology().getWriteConsistentHash());
      assertTopologyInfo3Nodes(dmi.getCacheTopology().getWriteConsistentHash().getMembers());
      dmi = (DistributionManagerImpl) advancedCache(1).getDistributionManager();
      assertTopologyInfo3Nodes(dmi.getCacheTopology().getWriteConsistentHash().getMembers());
      dmi = (DistributionManagerImpl) advancedCache(2).getDistributionManager();
      assertTopologyInfo3Nodes(dmi.getCacheTopology().getWriteConsistentHash().getMembers());

      ConsistentHash tach0 = advancedCache(0).getDistributionManager().getCacheTopology().getWriteConsistentHash();
      ConsistentHash tach1 = advancedCache(1).getDistributionManager().getCacheTopology().getWriteConsistentHash();
      assertEquals(tach0.getMembers(), tach1.getMembers());
      ConsistentHash tach2 = advancedCache(2).getDistributionManager().getCacheTopology().getWriteConsistentHash();
      assertEquals(tach0.getMembers(), tach2.getMembers());
   }

   @Test(dependsOnMethods = "testIsReplicated")
   public void testNodeLeaves() {
      TestingUtil.killCacheManagers(manager(1));
      TestingUtil.blockUntilViewsReceived(60000, false, cache(0), cache(2));
      TestingUtil.waitForNoRebalance(cache(0), cache(2));

      DistributionManagerImpl dmi = (DistributionManagerImpl) advancedCache(0).getDistributionManager();
      assertTopologyInfo2Nodes(dmi.getCacheTopology().getWriteConsistentHash().getMembers());
      dmi = (DistributionManagerImpl) advancedCache(2).getDistributionManager();
      assertTopologyInfo2Nodes(dmi.getCacheTopology().getWriteConsistentHash().getMembers());

      ConsistentHash tach0 = advancedCache(0).getDistributionManager().getCacheTopology().getWriteConsistentHash();
      ConsistentHash tach2 = advancedCache(2).getDistributionManager().getCacheTopology().getWriteConsistentHash();
      assertEquals(tach0.getMembers(), tach2.getMembers());
   }

   private void assertTopologyInfo3Nodes(List<Address> caches) {
      assertTopologyInfo2Nodes(Arrays.asList(caches.get(0), caches.get(2)));
      Address address1 = caches.get(1);
      assertEquals("s1", address1.getSiteId());
      assertEquals("r1", address1.getRackId());
      assertEquals("m1", address1.getMachineId());
   }

   private void assertTopologyInfo2Nodes(List<Address> caches) {
      Address address0 = caches.get(0);
      assertEquals("s0", address0.getSiteId());
      assertEquals("r0", address0.getRackId());
      assertEquals("m0", address0.getMachineId());
      Address address2 = caches.get(1);
      assertEquals("s2", address2.getSiteId());
      assertEquals("r2", address2.getRackId());
      assertEquals("m2", address2.getMachineId());
   }
}
