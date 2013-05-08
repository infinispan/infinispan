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

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.List;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "distribution.topologyaware.TopologyAwareStateTransferTest")
@CleanupAfterTest
public class TopologyAwareStateTransferTest extends MultipleCacheManagersTest {

   private Address[] addresses;

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration defaultConfig = getDefaultClusteredConfig(Configuration.CacheMode.DIST_SYNC);
      log.debug("defaultConfig = " + defaultConfig.getNumOwners());
      defaultConfig.setL1CacheEnabled(false);
      createClusteredCaches(5, defaultConfig);

      ConsistentHash hash = cache(0).getAdvancedCache().getDistributionManager().getConsistentHash();
      List<Address> members = hash.getMembers();
      addresses = members.toArray(new Address[members.size()]);
   }

   @AfterMethod
   @Override
   protected void clearContent() throws Throwable {
   }

   Cache cache(Address addr) {
      for (Cache c : caches()) {
         if (c.getAdvancedCache().getRpcManager().getAddress().equals(addr)) return c;
      }
      throw new RuntimeException("Address: " + addr);
   }

   public void testInitialState() {
      cache(0).put(addresses[0],"v0");
      cache(0).put(addresses[1],"v1");
      cache(0).put(addresses[2],"v2");
      cache(0).put(addresses[3],"v3");
      cache(0).put(addresses[4],"v4");

      log.debugf("Cache on node %s: %s", addresses[0], TestingUtil.printCache(cache(addresses[0])));
      log.debugf("Cache on node %s: %s", addresses[1], TestingUtil.printCache(cache(addresses[1])));
      log.debugf("Cache on node %s: %s", addresses[2], TestingUtil.printCache(cache(addresses[2])));
      log.debugf("Cache on node %s: %s", addresses[3], TestingUtil.printCache(cache(addresses[3])));

      assertExistence(addresses[0]);
      assertExistence(addresses[1]);
      assertExistence(addresses[2]);
      assertExistence(addresses[3]);
      assertExistence(addresses[4]);
   }

   @Test (dependsOnMethods = "testInitialState")
   public void testNodeDown() {
      EmbeddedCacheManager cm = cache(addresses[4]).getCacheManager();
      log.info("Here is where ST starts");
      TestingUtil.killCacheManagers(cm);
      cacheManagers.remove(cm);
      TestingUtil.blockUntilViewsReceived(60000, false, caches());
      TestingUtil.waitForRehashToComplete(caches());
      log.info("Here is where ST ends");
      List<Address> addressList = cache(addresses[0]).getAdvancedCache().getDistributionManager().getConsistentHash().getMembers();
      log.debug("After shutting down " + addresses[4] + " caches are " +  addressList);

      log.debugf("Cache on node %s: %s", addresses[0], TestingUtil.printCache(cache(addresses[0])));
      log.debugf("Cache on node %s: %s", addresses[1], TestingUtil.printCache(cache(addresses[1])));
      log.debugf("Cache on node %s: %s", addresses[2], TestingUtil.printCache(cache(addresses[2])));
      log.debugf("Cache on node %s: %s", addresses[3], TestingUtil.printCache(cache(addresses[3])));

      assertExistence(addresses[0]);
      assertExistence(addresses[1]);
      assertExistence(addresses[2]);
      assertExistence(addresses[3]);
      assertExistence(addresses[4]);
   }

   @Test (dependsOnMethods = "testNodeDown")
   public void testNodeDown2() {
      EmbeddedCacheManager cm = cache(addresses[2]).getCacheManager();
      TestingUtil.killCacheManagers(cm);
      cacheManagers.remove(cm);
      TestingUtil.blockUntilViewsReceived(60000, false, caches());
      TestingUtil.waitForRehashToComplete(caches());
      List<Address> addressList = cache(addresses[0]).getAdvancedCache().getDistributionManager().getConsistentHash().getMembers();
      log.debug("After shutting down " + addresses[2] + " caches are " +  addressList);

      log.debugf("Cache on node %s: %s", addresses[0], TestingUtil.printCache(cache(addresses[0])));
      log.debugf("Cache on node %s: %s", addresses[1], TestingUtil.printCache(cache(addresses[1])));
      log.debugf("Cache on node %s: %s", addresses[3], TestingUtil.printCache(cache(addresses[3])));

      assertExistence(addresses[0]);
      assertExistence(addresses[1]);
      assertExistence(addresses[2]);
      assertExistence(addresses[3]);
      assertExistence(addresses[4]);
   }

   @Test (dependsOnMethods = "testNodeDown2")
   public void testNodeDown3() {
      EmbeddedCacheManager cm = cache(addresses[1]).getCacheManager();
      TestingUtil.killCacheManagers(cm);
      cacheManagers.remove(cm);
      TestingUtil.blockUntilViewsReceived(60000, false, caches());
      TestingUtil.waitForRehashToComplete(caches());
      List<Address> addressList = cache(addresses[0]).getAdvancedCache().getDistributionManager().getConsistentHash().getMembers();
      log.debug("After shutting down " + addresses[1] + " caches are " +  addressList);

      log.debugf("Cache on node %s: %s", addresses[0], TestingUtil.printCache(cache(addresses[0])));
      log.debugf("Cache on node %s: %s", addresses[3], TestingUtil.printCache(cache(addresses[3])));

      assertExistence(addresses[0]);
      assertExistence(addresses[1]);
      assertExistence(addresses[2]);
      assertExistence(addresses[3]);
      assertExistence(addresses[4]);
   }


   private void assertExistence(final Object key) {
      org.infinispan.distribution.ch.ConsistentHash hash = cache(addresses[0]).getAdvancedCache().getDistributionManager().getConsistentHash();
      final List<Address> addresses = hash.locateOwners(key);
      log.debug(key + " should be present on = " + addresses);

      int count = 0;
      for (Cache c : caches()) {
         if (c.getAdvancedCache().getDataContainer().containsKey(key)) {
            log.debug("It is here = " + address(c));
            count++;
         }
      }
      log.debug("count = " + count);
      assert count == 2;

      for (Cache c : caches()) {
         if (addresses.contains(address(c))) {
            assert c.getAdvancedCache().getDataContainer().containsKey(key);
         } else {
            assert !c.getAdvancedCache().getDataContainer().containsKey(key);
         }
      }
   }

   @Override
   protected EmbeddedCacheManager addClusterEnabledCacheManager(Configuration deConfiguration) {
      int index = cacheManagers.size();
      String rack;
      String machine;
      switch (index) {
         case 0 : {
            rack = "r0";
            machine = "m0";
            break;
         }
         case 1 : {
            rack = "r0";
            machine = "m1";
            break;
         }
         case 2 : {
            rack = "r1";
            machine = "m0";
            break;
         }
         case 3 : {
            rack = "r2";
            machine = "m0";
            break;
         }
         case 4 : {
            rack = "r2";
            machine = "m0";
            break;
         }
         default : {
            throw new RuntimeException("Bad!");
         }
      }
      GlobalConfiguration gc = GlobalConfiguration.getClusteredDefault();
      gc.setRackId(rack);
      gc.setMachineId(machine);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(gc, deConfiguration);
      cacheManagers.add(cm);
      return cm;
   }
}
