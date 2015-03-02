package org.infinispan.jmx;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * @author Dan Berindei
 */
@Test(groups = "functional", testName = "jmx.CacheAvailabilityJmxTest")
public class CacheAvailabilityJmxTest extends MultipleCacheManagersTest {

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
            .partitionHandling().enabled(true);
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

   public void testAvailabilityChange() throws Exception {
      final MBeanServer mBeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      String domain0 = manager(1).getCacheManagerConfiguration().globalJmxStatistics().domain();
      final ObjectName cacheName0 = TestingUtil.getCacheObjectName(domain0, CacheContainer.DEFAULT_CACHE_NAME + "(dist_sync)");
      String domain1 = manager(1).getCacheManagerConfiguration().globalJmxStatistics().domain();
      final ObjectName cacheName1 = TestingUtil.getCacheObjectName(domain1, CacheContainer.DEFAULT_CACHE_NAME + "(dist_sync)");

      // Check initial state
      StateTransferManager stm0 = TestingUtil.extractComponent(cache(0), StateTransferManager.class);
      assertEquals(Arrays.asList(address(0), address(1)), stm0.getCacheTopology().getReadConsistentHash().getMembers());
      assertEquals(stm0.getCacheTopology().getWriteConsistentHash(), stm0.getCacheTopology().getReadConsistentHash());

      assertTrue(mBeanServer.isRegistered(cacheName0));
      assertEquals("AVAILABLE", mBeanServer.getAttribute(cacheName0, "CacheAvailability"));
      assertEquals("AVAILABLE", mBeanServer.getAttribute(cacheName1, "CacheAvailability"));

      // Enter degraded mode
      log.debugf("Entering degraded mode");
      mBeanServer.setAttribute(cacheName0, new Attribute("CacheAvailability", "DEGRADED_MODE"));
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            Object availability0 = mBeanServer.getAttribute(cacheName0, "CacheAvailability");
            Object availability1 = mBeanServer.getAttribute(cacheName1, "CacheAvailability");
            return "DEGRADED_MODE".equals(availability0) && "DEGRADED_MODE".equals(availability1);
         }
      });

      // Add 2 nodes
      log.debugf("Starting 2 new nodes");
      addClusterEnabledCacheManager(getGlobalConfigurationBuilder("r2"), getConfigurationBuilder());
      addClusterEnabledCacheManager(getGlobalConfigurationBuilder("r2"), getConfigurationBuilder());
      cache(2);
      cache(3);

      // Check that no rebalance happened after 1 second
      Thread.sleep(1000);
      assertEquals(Arrays.asList(address(0), address(1)), stm0.getCacheTopology().getReadConsistentHash().getMembers());
      assertEquals(stm0.getCacheTopology().getWriteConsistentHash(), stm0.getCacheTopology().getReadConsistentHash());

      assertEquals("DEGRADED_MODE", mBeanServer.getAttribute(cacheName0, "CacheAvailability"));
      assertEquals("DEGRADED_MODE", mBeanServer.getAttribute(cacheName1, "CacheAvailability"));

      // Enter available mode
      log.debugf("Back to available mode");
      mBeanServer.setAttribute(cacheName0, new Attribute("CacheAvailability", "AVAILABLE"));
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            Object availability0 = mBeanServer.getAttribute(cacheName0, "CacheAvailability");
            Object availability1 = mBeanServer.getAttribute(cacheName1, "CacheAvailability");
            return "AVAILABLE".equals(availability0) && "AVAILABLE".equals(availability1);
         }
      });

      // Check that the cache now has 4 nodes, and the CH is balanced
      TestingUtil.waitForRehashToComplete(cache(0), cache(1), cache(2), cache(3));
   }
}
