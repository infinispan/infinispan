package org.infinispan.jmx;

import static org.testng.Assert.assertEquals;
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
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * @author Dan Berindei
 */
@Test(groups = "functional", testName = "jmx.CacheAvailabilityJmxTest")
public class CacheAvailabilityJmxTest extends MultipleCacheManagersTest {

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   @Override
   protected void createCacheManagers() throws Throwable {
      addClusterEnabledCacheManager(getGlobalConfigurationBuilder("r1"), getConfigurationBuilder());
      addClusterEnabledCacheManager(getGlobalConfigurationBuilder("r1"), getConfigurationBuilder());
      waitForClusterToForm();
   }

   private ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC)
            .stateTransfer().awaitInitialTransfer(false)
            .partitionHandling().whenSplit(PartitionHandling.DENY_READ_WRITES);
      return cb;
   }

   private GlobalConfigurationBuilder getGlobalConfigurationBuilder(String rackId) {
      int nodeIndex = cacheManagers.size();
      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb.jmx().enabled(true)
         .domain(getClass().getSimpleName() + nodeIndex)
         .mBeanServerLookup(mBeanServerLookup)
         .transport().rackId(rackId);
      return gcb;
   }

   public void testAvailabilityChange() throws Exception {
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      final String cacheName = manager(0).getCacheManagerConfiguration().defaultCacheName().get();
      String domain0 = manager(1).getCacheManagerConfiguration().jmx().domain();
      final ObjectName cacheName0 = TestingUtil.getCacheObjectName(domain0, cacheName + "(dist_sync)");
      String domain1 = manager(1).getCacheManagerConfiguration().jmx().domain();
      final ObjectName cacheName1 = TestingUtil.getCacheObjectName(domain1, cacheName + "(dist_sync)");

      // Check initial state
      DistributionManager dm0 = advancedCache(0).getDistributionManager();
      assertEquals(Arrays.asList(address(0), address(1)), dm0.getCacheTopology().getCurrentCH().getMembers());
      assertNull(dm0.getCacheTopology().getPendingCH());

      assertTrue(mBeanServer.isRegistered(cacheName0));
      assertEquals("AVAILABLE", mBeanServer.getAttribute(cacheName0, "CacheAvailability"));
      assertEquals("AVAILABLE", mBeanServer.getAttribute(cacheName1, "CacheAvailability"));

      // Enter degraded mode
      log.debugf("Entering degraded mode");
      mBeanServer.setAttribute(cacheName0, new Attribute("CacheAvailability", "DEGRADED_MODE"));
      eventually(() -> {
         Object availability0 = mBeanServer.getAttribute(cacheName0, "CacheAvailability");
         Object availability1 = mBeanServer.getAttribute(cacheName1, "CacheAvailability");
         return "DEGRADED_MODE".equals(availability0) && "DEGRADED_MODE".equals(availability1);
      });

      // Add 2 nodes
      log.debugf("Starting 2 new nodes");
      addClusterEnabledCacheManager(getGlobalConfigurationBuilder("r2"), getConfigurationBuilder());
      addClusterEnabledCacheManager(getGlobalConfigurationBuilder("r2"), getConfigurationBuilder());
      cache(2);
      cache(3);

      // Check that no rebalance happened after 1 second
      Thread.sleep(1000);
      assertEquals(Arrays.asList(address(0), address(1)), dm0.getCacheTopology().getCurrentCH().getMembers());
      assertNull(dm0.getCacheTopology().getPendingCH());

      assertEquals("DEGRADED_MODE", mBeanServer.getAttribute(cacheName0, "CacheAvailability"));
      assertEquals("DEGRADED_MODE", mBeanServer.getAttribute(cacheName1, "CacheAvailability"));

      // Enter available mode
      log.debugf("Back to available mode");
      mBeanServer.setAttribute(cacheName0, new Attribute("CacheAvailability", "AVAILABLE"));
      eventually(() -> {
         Object availability0 = mBeanServer.getAttribute(cacheName0, "CacheAvailability");
         Object availability1 = mBeanServer.getAttribute(cacheName1, "CacheAvailability");
         return "AVAILABLE".equals(availability0) && "AVAILABLE".equals(availability1);
      });

      // Check that the cache now has 4 nodes, and the CH is balanced
      TestingUtil.waitForNoRebalance(cache(0), cache(1), cache(2), cache(3));
   }
}
