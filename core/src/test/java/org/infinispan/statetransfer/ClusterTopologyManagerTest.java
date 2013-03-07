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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
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
      defaultConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
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
      ConfigurationBuilder defaultConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
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
      ConfigurationBuilder defaultConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
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
      ConfigurationBuilder defaultConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
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
      ConfigurationBuilder defaultConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
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
      doAnswer(new Answer<Object>() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            int viewId = (Integer) invocation.getArguments()[2];
            checkpoint.trigger("rebalance" + viewId);
            log.debugf("Blocking the REBALANCE_START command on the merge coordinator");
            checkpoint.awaitStrict("merge", 10, TimeUnit.SECONDS);
            return invocation.callRealMethod();
         }
      }).when(spyLocalTopologyManager).handleRebalance(eq(CACHE_NAME), any(CacheTopology.class), anyInt());
      TestingUtil.replaceComponent(mergeCoordManager, LocalTopologyManager.class, spyLocalTopologyManager, true);

      final EmbeddedCacheManager cm4 = addClusterEnabledCacheManager(defaultConfig, new TransportFlags().withFD(true).withMerge(true));
      Future<Cache<Object,Object>> cacheFuture = fork(new Callable<Cache<Object, Object>>() {
         @Override
         public Cache<Object, Object> call() throws Exception {
            return cm4.getCache(CACHE_NAME);
         }
      });

      log.debugf("Waiting for the REBALANCE_START command to reach the merge coordinator");
      checkpoint.awaitStrict("rebalance" + (viewIdAfterSplit + 1), 10, TimeUnit.SECONDS);

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
      ConfigurationBuilder defaultConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      EmbeddedCacheManager cm5 = addClusterEnabledCacheManager(defaultConfig, new TransportFlags().withFD(true).withMerge(true));
      Cache<Object, Object> c5 = cm5.getCache(CACHE_NAME);
      TestingUtil.blockUntilViewsReceived(30000, true, c1, c2, c3, c4, c5);
      TestingUtil.waitForRehashToComplete(c1, c2, c3, c4, c5);
   }

}

class CheckPoint {
   private final Lock lock = new ReentrantLock();
   private final Condition unblockCondition = lock.newCondition();
   private final Map<String, Integer> events = new HashMap<String, Integer>();

   public void awaitStrict(String event, long timeout, TimeUnit unit)
         throws InterruptedException, TimeoutException {
      awaitStrict(event, 1, timeout, unit);
   }

   public boolean await(String event, long timeout, TimeUnit unit) throws InterruptedException {
      return await(event, 1, timeout, unit);
   }

   public void awaitStrict(String event, int count, long timeout, TimeUnit unit)
         throws InterruptedException, TimeoutException {
      if (!await(event, count, timeout, unit)) {
         throw new TimeoutException("Timed out waiting for event " + event);
      }
   }

   public boolean await(String event, int count, long timeout, TimeUnit unit) throws InterruptedException {
      lock.lock();
      try {
         long waitNanos = unit.toNanos(timeout);
         while (waitNanos > 0) {
            Integer currentCount = events.get(event);
            if (currentCount != null && currentCount >= count) {
               events.put(event, currentCount - count);
               break;
            }
            waitNanos = unblockCondition.awaitNanos(waitNanos);
         }

         if (waitNanos <= 0) {
            // let the triggering thread know that we timed out
            events.put(event, -1);
            return false;
         }

         return true;
      } finally {
         lock.unlock();
      }
   }

   public void trigger(String event) {
      trigger(event, 1);
   }

   public void triggerForever(String event) {
      trigger(event, Integer.MAX_VALUE);
   }

   public void trigger(String event, int count) {
      lock.lock();
      try {
         Integer currentCount = events.get(event);
         if (currentCount == null) {
            currentCount = 0;
         } else if (currentCount < 0) {
            throw new IllegalStateException("Thread already timed out waiting for event " + event);
         }

         // If triggerForever is called more than once, it will cause an overflow and the waiters will fail.
         events.put(event, currentCount + count);
         unblockCondition.signalAll();
      } finally {
         lock.unlock();
      }
   }
}
