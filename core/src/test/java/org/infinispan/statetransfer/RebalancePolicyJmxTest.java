/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.statetransfer;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.jmx.PerThreadMBeanServerLookup;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

import java.util.Arrays;
import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import static org.infinispan.test.TestingUtil.killCacheManagers;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

/**
 * @author Dan Berindei
 */
@Test(groups = "functional", testName = "jmx.RebalancePolicyJmxTest")
public class RebalancePolicyJmxTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      addClusterEnabledCacheManager(getGlobalConfigurationBuilder("r1"), getConfigurationBuilder());
      addClusterEnabledCacheManager(getGlobalConfigurationBuilder("r1"), getConfigurationBuilder());
      waitForClusterToForm();
   }

   private ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC)
         .stateTransfer().awaitInitialTransfer(false);
      return cb;
   }

   private GlobalConfigurationBuilder getGlobalConfigurationBuilder(String rackId) {
      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb.globalJmxStatistics()
            .enable()
            //.allowDuplicateDomains(true)
            //.jmxDomain(JMX_DOMAIN)
            .mBeanServerLookup(new PerThreadMBeanServerLookup())
         .transport().rackId(rackId);
      return gcb;
   }

   public void testRebalanceSuspend() throws Exception {
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
      addClusterEnabledCacheManager(getGlobalConfigurationBuilder("r2"), getConfigurationBuilder());
      addClusterEnabledCacheManager(getGlobalConfigurationBuilder("r2"), getConfigurationBuilder());

      // Check that no rebalance happened after 1 second
      Thread.sleep(1000);
      assertFalse((Boolean) mBeanServer.getAttribute(ltmName1, "RebalancingEnabled"));
      assertNull(stm0.getCacheTopology().getPendingCH());
      assertEquals(Arrays.asList(address(0), address(1)), stm0.getCacheTopology().getCurrentCH().getMembers());

      // Re-enable rebalancing
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
      assertFalse((Boolean) mBeanServer.getAttribute(ltmName1, "RebalancingEnabled"));
      // Duplicate request to enable rebalancing - should be ignored
      mBeanServer.setAttribute(ltmName1, new Attribute("RebalancingEnabled", false));

      // Kill the previously added 2 nodes
      killCacheManagers(manager(2), manager(3));

      // Check that the nodes are no longer in the CH, but every segment only has one copy
      // Implicitly, this also checks that no data has been lost - if both a segment's owners had left,
      // the CH factory would have assigned 2 owners.
      Thread.sleep(1000);
      assertNull(stm0.getCacheTopology().getPendingCH());
      ch = stm0.getCacheTopology().getCurrentCH();
      assertEquals(Arrays.asList(address(0), address(1)), ch.getMembers());
      for (int i = 0; i < ch.getNumSegments(); i++) {
         assertEquals(1, ch.locateOwnersForSegment(i).size());
      }

      // Enable rebalancing again
      mBeanServer.setAttribute(ltmName1, new Attribute("RebalancingEnabled", true));
      assertTrue((Boolean) mBeanServer.getAttribute(ltmName1, "RebalancingEnabled"));

      // Check that the CH is now balanced (and every segment has 2 copies)
      TestingUtil.waitForRehashToComplete(cache(0), cache(1));
      assertNull(stm0.getCacheTopology().getPendingCH());
      ch = stm0.getCacheTopology().getCurrentCH();
      assertEquals(Arrays.asList(address(0), address(1)), ch.getMembers());
      for (int i = 0; i < ch.getNumSegments(); i++) {
         assertEquals(2, ch.locateOwnersForSegment(i).size());
      }
   }
}
