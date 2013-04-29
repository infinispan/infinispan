/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.distribution.topologyaware;

import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.distribution.DistributionManagerImpl;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.TopologyAwareAddress;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "distribution.topologyaware.TopologyInfoBroadcastTest")
public class TopologyInfoBroadcastTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      GlobalConfiguration gc1 = GlobalConfiguration.getClusteredDefault();
      updatedSiteInfo(gc1, "s0", "r0", "m0");
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createCacheManager(gc1, getClusterConfig());
      cacheManagers.add(cm1);

      GlobalConfiguration gc2 = GlobalConfiguration.getClusteredDefault();
      updatedSiteInfo(gc2, "s1", "r1", "m1");
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createCacheManager(gc2, getClusterConfig());
      cacheManagers.add(cm2);

      GlobalConfiguration gc3 = GlobalConfiguration.getClusteredDefault();
      updatedSiteInfo(gc3, "s2", "r2", "m2");
      EmbeddedCacheManager cm3 = TestCacheManagerFactory.createCacheManager(gc3, getClusterConfig());
      cacheManagers.add(cm3);

      log.info("Here it starts");
      waitForClusterToForm();
      log.info("Here it ends");
   }

   protected Configuration getClusterConfig() {
      return getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC);
   }

   private void updatedSiteInfo(GlobalConfiguration gc, String s, String r, String m) {
      gc.setSiteId(s);
      gc.setRackId(r);
      gc.setMachineId(m);
   }

   public void testIsReplicated() {
//      assert advancedCache(0).getDistributionManager().getConsistentHash() instanceof TopologyAwareConsistentHash;
//      assert advancedCache(1).getDistributionManager().getConsistentHash() instanceof TopologyAwareConsistentHash;
//      assert advancedCache(2).getDistributionManager().getConsistentHash() instanceof TopologyAwareConsistentHash;

      DistributionManagerImpl dmi = (DistributionManagerImpl) advancedCache(0).getDistributionManager();
      log.trace("distributionManager.ConsistentHash() = " + dmi.getConsistentHash());
      assertTopologyInfo3Nodes(dmi.getConsistentHash().getMembers());
      dmi = (DistributionManagerImpl) advancedCache(1).getDistributionManager();
      assertTopologyInfo3Nodes(dmi.getConsistentHash().getMembers());
      dmi = (DistributionManagerImpl) advancedCache(2).getDistributionManager();
      assertTopologyInfo3Nodes(dmi.getConsistentHash().getMembers());

      ConsistentHash tach0 = advancedCache(0).getDistributionManager().getConsistentHash();
      ConsistentHash tach1 = advancedCache(1).getDistributionManager().getConsistentHash();
      assertEquals(tach0.getMembers(), tach1.getMembers());
      ConsistentHash tach2 = advancedCache(2).getDistributionManager().getConsistentHash();
      assertEquals(tach0.getMembers(), tach2.getMembers());
   }

   @Test(dependsOnMethods = "testIsReplicated")
   public void testNodeLeaves() {
      TestingUtil.killCacheManagers(manager(1));
      TestingUtil.blockUntilViewsReceived(60000, false, cache(0), cache(2));
      TestingUtil.waitForRehashToComplete(cache(0), cache(2));

      DistributionManagerImpl dmi = (DistributionManagerImpl) advancedCache(0).getDistributionManager();
      assertTopologyInfo2Nodes(dmi.getConsistentHash().getMembers());
      dmi = (DistributionManagerImpl) advancedCache(2).getDistributionManager();
      assertTopologyInfo2Nodes(dmi.getConsistentHash().getMembers());

      ConsistentHash tach0 = (ConsistentHash) advancedCache(0).getDistributionManager().getConsistentHash();
      ConsistentHash tach2 = (ConsistentHash) advancedCache(2).getDistributionManager().getConsistentHash();
      assertEquals(tach0.getMembers(), tach2.getMembers());
   }

   private void assertTopologyInfo3Nodes(List<Address> caches) {
      assertTopologyInfo2Nodes(caches);
      TopologyAwareAddress address1 = (TopologyAwareAddress) address(1);
      assertEquals(address1.getSiteId(), "s1");
      assertEquals(address1.getRackId(), "r1");
      assertEquals(address1.getMachineId(), "m1");
   }

   private void assertTopologyInfo2Nodes(List<Address> caches) {
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
