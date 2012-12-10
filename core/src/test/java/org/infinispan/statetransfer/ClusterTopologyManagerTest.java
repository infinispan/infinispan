/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.util.Util;
import org.jgroups.protocols.DISCARD;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "statetransfer.ClusterTopologyManagerTest")
@CleanupAfterMethod
public class ClusterTopologyManagerTest extends MultipleCacheManagersTest {

   Cache c1, c2, c3;
   DISCARD d1, d2, d3;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      createClusteredCaches(3, defaultConfig, new TransportFlags().withFD(true).withMerge(true));

      c1 = cache(0, "cache");
      c2 = cache(1, "cache");
      c3 = cache(2, "cache");
      d1 = TestingUtil.getDiscardForCache(c1);
      d1.setExcludeItself(true);
      d2 = TestingUtil.getDiscardForCache(c2);
      d2.setExcludeItself(true);
      d3 = TestingUtil.getDiscardForCache(c3);
      d3.setExcludeItself(true);
   }

   public void testNodeAbruptLeave() throws Exception {
      // Create some more caches to trigger ISPN-2572
      cache(0, "cache2");
      cache(1, "cache2");
      cache(0, "cache3");
      cache(2, "cache3");
      cache(1, "cache4");
      cache(2, "cache4");
      cache(0, "cache5");
      cache(1, "cache5");

      // create the partitions
      log.debugf("Killing coordinator via discard");
      d3.setDiscardAll(true);

      // wait for the partitions to form
      long startTime = System.currentTimeMillis();
      TestingUtil.blockUntilViewsReceived(30000, false, c1, c2);
      TestingUtil.blockUntilViewsReceived(30000, false, c3);
      TestingUtil.waitForRehashToComplete(c1, c2);
      TestingUtil.waitForRehashToComplete(c3);

      TestingUtil.waitForRehashToComplete(cache(0, "cache2"), cache(1, "cache2"));
      TestingUtil.waitForRehashToComplete(cache(0, "cache3"));
      TestingUtil.waitForRehashToComplete(cache(1, "cache4"));
      TestingUtil.waitForRehashToComplete(cache(0, "cache5"), cache(1, "cache5"));

      long endTime = System.currentTimeMillis();
      log.debugf("Recovery took %s", Util.prettyPrintTime(endTime - startTime));
      assert endTime - startTime < 30000 : "Recovery took too long: " + Util.prettyPrintTime(endTime - startTime);

      // Check that a new node can join
      ConfigurationBuilder defaultConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      addClusterEnabledCacheManager(defaultConfig, new TransportFlags().withFD(true).withMerge(true));
      Cache<Object, Object> c4 = cache(3, "cache");
      TestingUtil.blockUntilViewsReceived(30000, true, c1, c2, c4);
      TestingUtil.waitForRehashToComplete(c1, c2, c4);

      cache(3, "cache2");
      cache(3, "cache3");
      cache(3, "cache4");
      cache(3, "cache5");
      TestingUtil.waitForRehashToComplete(cache(0, "cache2"), cache(1, "cache2"), cache(3, "cache2"));
      TestingUtil.waitForRehashToComplete(cache(0, "cache3"), cache(3, "cache3"));
      TestingUtil.waitForRehashToComplete(cache(1, "cache4"), cache(3, "cache4"));
      TestingUtil.waitForRehashToComplete(cache(0, "cache5"), cache(1, "cache5"), cache(3, "cache5"));
   }

   public void testClusterRecoveryAfterCoordLeave() throws Exception {
      // create the partitions
      log.debugf("Killing coordinator via discard");
      d1.setDiscardAll(true);

      // wait for the partitions to form
      long startTime = System.currentTimeMillis();
      TestingUtil.blockUntilViewsReceived(30000, false, c1);
      TestingUtil.blockUntilViewsReceived(30000, false, c2, c3);
      TestingUtil.waitForRehashToComplete(c1);
      TestingUtil.waitForRehashToComplete(c2, c3);

      long endTime = System.currentTimeMillis();
      log.debugf("Recovery took %s", Util.prettyPrintTime(endTime - startTime));
      assert endTime - startTime < 30000 : "Recovery took too long: " + Util.prettyPrintTime(endTime - startTime);

      // Check that a new node can join
      ConfigurationBuilder defaultConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      addClusterEnabledCacheManager(defaultConfig, new TransportFlags().withFD(true).withMerge(true));
      Cache<Object, Object> c4 = cache(3, "cache");
      TestingUtil.blockUntilViewsReceived(30000, true, c2, c3, c4);
      TestingUtil.waitForRehashToComplete(c2, c3, c4);
   }

   public void testClusterRecoveryAfterThreeWaySplit() throws Exception {
      // create the partitions
      log.debugf("Splitting the cluster in three");
      d1.setDiscardAll(true);
      d2.setDiscardAll(true);
      d3.setDiscardAll(true);

      // wait for the partitions to form
      TestingUtil.blockUntilViewsReceived(30000, false, c1);
      TestingUtil.blockUntilViewsReceived(30000, false, c2);
      TestingUtil.blockUntilViewsReceived(30000, false, c3);
      TestingUtil.waitForRehashToComplete(c1);
      TestingUtil.waitForRehashToComplete(c2);
      TestingUtil.waitForRehashToComplete(c3);

      // merge the remaining partitions
      log.debugf("Merging the cluster partitions");
      d1.setDiscardAll(false);
      d2.setDiscardAll(false);
      d3.setDiscardAll(false);

      // wait for the merged cluster to form
      long startTime = System.currentTimeMillis();
      TestingUtil.blockUntilViewsReceived(30000, c1, c2, c3);
      TestingUtil.waitForRehashToComplete(c1, c2, c3);

      long endTime = System.currentTimeMillis();
      log.debugf("Merge took %s", Util.prettyPrintTime(endTime - startTime));
      assert endTime - startTime < 30000 : "Merge took too long: " + Util.prettyPrintTime(endTime - startTime);

      // Check that a new node can join
      ConfigurationBuilder defaultConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      addClusterEnabledCacheManager(defaultConfig, new TransportFlags().withFD(true).withMerge(true));
      Cache<Object, Object> c4 = cache(3, "cache");
      TestingUtil.blockUntilViewsReceived(30000, true, c1, c2, c3, c4);
      TestingUtil.waitForRehashToComplete(c1, c2, c3, c4);
   }

   public void testClusterRecoveryAfterSplitAndCoordLeave() throws Exception {
      // create the partitions
      log.debugf("Splitting the cluster in three");
      d1.setDiscardAll(true);
      d2.setDiscardAll(true);
      d3.setDiscardAll(true);

      // wait for the partitions to form
      TestingUtil.blockUntilViewsReceived(30000, false, c1);
      TestingUtil.blockUntilViewsReceived(30000, false, c2);
      TestingUtil.blockUntilViewsReceived(30000, false, c3);
      TestingUtil.waitForRehashToComplete(c1);
      TestingUtil.waitForRehashToComplete(c2);
      TestingUtil.waitForRehashToComplete(c3);

      // kill the coordinator
      manager(0).stop();

      // merge the two remaining partitions
      log.debugf("Merging the cluster partitions");
      d2.setDiscardAll(false);
      d3.setDiscardAll(false);

      // wait for the merged cluster to form
      long startTime = System.currentTimeMillis();
      TestingUtil.blockUntilViewsReceived(30000, c2, c3);
      TestingUtil.waitForRehashToComplete(c2, c3);

      long endTime = System.currentTimeMillis();
      log.debugf("Merge took %s", Util.prettyPrintTime(endTime - startTime));
      assert endTime - startTime < 30000 : "Merge took too long: " + Util.prettyPrintTime(endTime - startTime);

      // Check that a new node can join
      ConfigurationBuilder defaultConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      addClusterEnabledCacheManager(defaultConfig, new TransportFlags().withFD(true).withMerge(true));
      Cache<Object, Object> c4 = cache(3, "cache");
      TestingUtil.blockUntilViewsReceived(30000, true, c2, c3, c4);
      TestingUtil.waitForRehashToComplete(c2, c3, c4);
   }
}
