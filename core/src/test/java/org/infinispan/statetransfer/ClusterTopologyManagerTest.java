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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.util.Util;
import org.jgroups.protocols.DISCARD;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anySet;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

@Test(groups = "functional", testName = "statetransfer.ClusterTopologyManagerTest")
@CleanupAfterMethod
public class ClusterTopologyManagerTest extends MultipleCacheManagersTest {

   public static final String CACHE_NAME = "cache";
   private ConfigurationBuilder defaultConfig;
   Cache c1, c2, c3;
   DISCARD d1, d2, d3;

   @Override
   protected void createCacheManagers() throws Throwable {
      defaultConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      createClusteredCaches(3, defaultConfig, new TransportFlags().withFD(true).withMerge(true));

      c1 = cache(0, CACHE_NAME);
      c2 = cache(1, CACHE_NAME);
      c3 = cache(2, CACHE_NAME);
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
      ConfigurationBuilder defaultConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      addClusterEnabledCacheManager(defaultConfig, new TransportFlags().withFD(true).withMerge(true));
      Cache<Object, Object> c4 = cache(3, CACHE_NAME);
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
      ConfigurationBuilder defaultConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      addClusterEnabledCacheManager(defaultConfig, new TransportFlags().withFD(true).withMerge(true));
      Cache<Object, Object> c4 = cache(3, CACHE_NAME);
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
      ConfigurationBuilder defaultConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      addClusterEnabledCacheManager(defaultConfig, new TransportFlags().withFD(true).withMerge(true));
      Cache<Object, Object> c4 = cache(3, CACHE_NAME);
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
      ConfigurationBuilder defaultConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      addClusterEnabledCacheManager(defaultConfig, new TransportFlags().withFD(true).withMerge(true));
      Cache<Object, Object> c4 = cache(3, CACHE_NAME);
      TestingUtil.blockUntilViewsReceived(30000, true, c2, c3, c4);
      TestingUtil.waitForRehashToComplete(c2, c3, c4);
   }

   public void testClusterRecoveryWithRebalance() throws Exception {
      // Compute the merge coordinator by sorting the JGroups addresses, the same way MERGE2/3 do
      List<Address> members = new ArrayList<Address>(manager(0).getMembers());
      Collections.sort(members);
      Address mergeCoordAddress = members.get(0);
      log.debugf("The merge coordinator will be %s", mergeCoordAddress);
      EmbeddedCacheManager mergeCoordManager = manager(mergeCoordAddress);
      int mergeCoordIndex = cacheManagers.indexOf(mergeCoordManager);

      // create the partitions
      log.debugf("Splitting the cluster in three");
      d1.setDiscardAll(true);
      d2.setDiscardAll(true);
      d3.setDiscardAll(true);

      // wait for the coordinator to be separated (don't care about the others)
      TestingUtil.blockUntilViewsReceived(30000, false, c1);
      TestingUtil.blockUntilViewsReceived(30000, false, c2);
      TestingUtil.blockUntilViewsReceived(30000, false, c3);
      TestingUtil.waitForRehashToComplete(c1);
      TestingUtil.waitForRehashToComplete(c2);
      TestingUtil.waitForRehashToComplete(c3);

      // Disable DISCARD *only* on the merge coordinator
      if (mergeCoordIndex == 0) d1.setDiscardAll(false);
      if (mergeCoordIndex == 1) d2.setDiscardAll(false);
      if (mergeCoordIndex == 2) d3.setDiscardAll(false);

      int viewIdAfterSplit = mergeCoordManager.getTransport().getViewId();
      final LocalTopologyManager localTopologyManager = TestingUtil.extractGlobalComponent(mergeCoordManager,
            LocalTopologyManager.class);
      final CheckPoint checkpoint = new CheckPoint();
      LocalTopologyManager spyLocalTopologyManager = spy(localTopologyManager);
      TestingUtil.replaceComponent(mergeCoordManager, LocalTopologyManager.class, spyLocalTopologyManager, true);
      doAnswer(new Answer<Object>() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            int viewId = (Integer) invocation.getArguments()[2];
            checkpoint.trigger("rebalance_" + viewId);
            log.debugf("Blocking the REBALANCE_START command on the merge coordinator");
            checkpoint.awaitStrict("merge", 10, TimeUnit.SECONDS);
            return invocation.callRealMethod();
         }
      }).when(spyLocalTopologyManager).handleRebalance(eq(CACHE_NAME), any(CacheTopology.class), anyInt());

      final EmbeddedCacheManager cm4 = addClusterEnabledCacheManager(defaultConfig,
            new TransportFlags().withFD(true).withMerge(true));
      Future<Cache<Object,Object>> cacheFuture = fork(new Callable<Cache<Object, Object>>() {
         @Override
         public Cache<Object, Object> call() throws Exception {
            return cm4.getCache(CACHE_NAME);
         }
      });

      log.debugf("Waiting for the REBALANCE_START command to reach the merge coordinator");
      checkpoint.awaitStrict("rebalance_" + (viewIdAfterSplit + 1), 10, TimeUnit.SECONDS);

      // merge the partitions
      log.debugf("Merging the cluster partitions");
      d1.setDiscardAll(false);
      d2.setDiscardAll(false);
      d3.setDiscardAll(false);

      // wait for the JGroups merge
      long startTime = System.currentTimeMillis();
      TestingUtil.blockUntilViewsReceived(30000, cacheManagers);

      // unblock the REBALANCE_START command
      log.debugf("Unblocking the REBALANCE_START command on the coordinator");
      checkpoint.triggerForever("merge");

      // wait for the 4th cache to finish joining
      Cache<Object, Object> c4 = cacheFuture.get(30, TimeUnit.SECONDS);
      TestingUtil.waitForRehashToComplete(c1, c2, c3, c4);

      long endTime = System.currentTimeMillis();
      log.debugf("Merge took %s", Util.prettyPrintTime(endTime - startTime));
      assert endTime - startTime < 30000 : "Merge took too long: " + Util.prettyPrintTime(endTime - startTime);

      // Check that another node can join
      ConfigurationBuilder defaultConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      EmbeddedCacheManager cm5 = addClusterEnabledCacheManager(defaultConfig,
            new TransportFlags().withFD(true).withMerge(true));
      Cache<Object, Object> c5 = cm5.getCache(CACHE_NAME);
      TestingUtil.blockUntilViewsReceived(30000, true, c1, c2, c3, c4, c5);
      TestingUtil.waitForRehashToComplete(c1, c2, c3, c4, c5);
   }

   public void testAbruptLeaveAfterGetStatus() throws TimeoutException, InterruptedException {
      // Block the GET_STATUS command on node 2
      final LocalTopologyManager localTopologyManager = TestingUtil.extractGlobalComponent(manager(1),
            LocalTopologyManager.class);
      final CheckPoint checkpoint = new CheckPoint();
      LocalTopologyManager spyLocalTopologyManager = spy(localTopologyManager);
      TestingUtil.replaceComponent(manager(1), LocalTopologyManager.class, spyLocalTopologyManager, true);
      doAnswer(new Answer<Object>() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            int viewId = (Integer) invocation.getArguments()[0];
            checkpoint.trigger("GET_STATUS_" + viewId);
            log.debugf("Blocking the GET_STATUS command on the merge coordinator");
            checkpoint.awaitStrict("3 left", 10, TimeUnit.SECONDS);
            return invocation.callRealMethod();
         }
      }).when(spyLocalTopologyManager).handleStatusRequest(anyInt());

      // Node 1 (the coordinator) dies. Node 2 becomes coordinator and tries to call GET_STATUS
      log.debugf("Killing coordinator");
      manager(0).stop();
      TestingUtil.blockUntilViewsReceived(30000, false, manager(1), manager(2));

      // Wait for the GET_STATUS command and stop node 3 abruptly
      int viewId = manager(1).getTransport().getViewId();
      checkpoint.awaitStrict("GET_STATUS_" + viewId, 10, TimeUnit.SECONDS);
      d3.setDiscardAll(true);
      manager(2).stop();
      TestingUtil.blockUntilViewsReceived(30000, false, manager(1));
      checkpoint.trigger("3 left");

      // Wait for node 2 to install a view with only itself and unblock the GET_STATUS command
      TestingUtil.waitForRehashToComplete(c2);
   }

   public void testLeaveDuringGetTransactions() throws InterruptedException, TimeoutException {
      final CheckPoint checkpoint = new CheckPoint();
      StateProvider stateProvider = TestingUtil.extractComponent(c2, StateProvider.class);
      StateProvider spyStateProvider = spy(stateProvider);
      TestingUtil.replaceComponent(c2, StateProvider.class, spyStateProvider, true);
      doAnswer(new Answer<Object>() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            int topologyId = (Integer) invocation.getArguments()[1];
            checkpoint.trigger("GET_TRANSACTIONS");
            log.debugf("Blocking the GET_TRANSACTIONS(%d) command on the %s", topologyId, c2);
            checkpoint.awaitStrict("LEAVE", 10, TimeUnit.SECONDS);
            return invocation.callRealMethod();
         }
      }).when(spyStateProvider).getTransactionsForSegments(any(Address.class), anyInt(), anySet());

      long startTime = System.currentTimeMillis();
      manager(2).stop();

      checkpoint.awaitStrict("GET_TRANSACTIONS", 10, TimeUnit.SECONDS);
      manager(1).stop();
      checkpoint.trigger("LEAVE");

      TestingUtil.blockUntilViewsReceived(30000, false, c1);
      TestingUtil.waitForRehashToComplete(c1);
      long endTime = System.currentTimeMillis();
      log.debugf("Recovery took %s", Util.prettyPrintTime(endTime - startTime));
      assert endTime - startTime < 30000 : "Recovery took too long: " + Util.prettyPrintTime(endTime - startTime);
   }
}

