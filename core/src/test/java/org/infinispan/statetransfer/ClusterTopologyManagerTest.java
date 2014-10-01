package org.infinispan.statetransfer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partionhandling.impl.AvailabilityMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;
import org.jgroups.protocols.DISCARD;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anySet;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

@Test(groups = "functional", testName = "statetransfer.ClusterTopologyManagerTest")
@CleanupAfterMethod
public class ClusterTopologyManagerTest extends MultipleCacheManagersTest {

   public static final String CACHE_NAME = "testCache";
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
      TestingUtil.blockUntilViewsReceived(60000, c1, c2, c3);
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

   @Test(groups = "unstable")
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
            checkpoint.trigger("rebalance_" + viewId);
            log.debugf("Blocking the REBALANCE_START command on the merge coordinator");
            checkpoint.awaitStrict("merge", 30, TimeUnit.SECONDS);
            return invocation.callRealMethod();
         }
      }).when(spyLocalTopologyManager).handleRebalance(eq(CACHE_NAME), any(CacheTopology.class), anyInt());
      TestingUtil.replaceComponent(mergeCoordManager, LocalTopologyManager.class, spyLocalTopologyManager, true);

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
      final LocalTopologyManager localTopologyManager2 = TestingUtil.extractGlobalComponent(manager(1),
            LocalTopologyManager.class);
      final CheckPoint checkpoint = new CheckPoint();
      LocalTopologyManager spyLocalTopologyManager2 = spy(localTopologyManager2);
      final CacheTopology initialTopology = localTopologyManager2.getCacheTopology(CACHE_NAME);
      doAnswer(new Answer<Object>() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            int viewId = (Integer) invocation.getArguments()[0];
            checkpoint.trigger("GET_STATUS_" + viewId);
            log.debugf("Blocking the GET_STATUS command on the new coordinator");
            checkpoint.awaitStrict("3 left", 10, TimeUnit.SECONDS);
            return invocation.callRealMethod();
         }
      }).when(spyLocalTopologyManager2).handleStatusRequest(anyInt());
      doAnswer(new Answer<Object>() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            CacheTopology topology = (CacheTopology) invocation.getArguments()[1];
            if (topology.getRebalanceId() == initialTopology.getRebalanceId() + 1) {
               log.debugf("Discarding CH update command %s", topology);
               return null;
            }
            return invocation.callRealMethod();
         }
      }).when(spyLocalTopologyManager2).handleTopologyUpdate(eq(CACHE_NAME), any(CacheTopology.class),
            any(AvailabilityMode.class), anyInt());
      doAnswer(new Answer<Object>() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            CacheTopology topology = (CacheTopology) invocation.getArguments()[1];
            if (topology.getRebalanceId() == initialTopology.getRebalanceId() + 2) {
               log.debugf("Discarding rebalance command %s", topology);
               return null;
            }
            return invocation.callRealMethod();
         }
      }).when(spyLocalTopologyManager2).handleRebalance(eq(CACHE_NAME), any(CacheTopology.class), anyInt());
      TestingUtil.replaceComponent(manager(1), LocalTopologyManager.class, spyLocalTopologyManager2, true);

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
      checkpoint.triggerForever("3 left");

      // Wait for node 2 to install a view with only itself and unblock the GET_STATUS command
      TestingUtil.waitForRehashToComplete(c2);
   }

   public void testAbruptLeaveAfterGetStatus2() throws TimeoutException, InterruptedException {
      // Similar to testAbruptLeaveAfterGetStatus, but also test that delayed CacheTopologyControlCommands
      // are handled properly.
      // After node 2 becomes the coordinator and the GET_STATUS command is unblocked, it normally installs these topologies:
      // 1. The recovered topology with 2 and 3 as members (topologyId = initial topologyId + 1, rebalanceId = initial rebalanceId + 1)
      // 2. A topology starting the rebalance with 2 and 3 (topologyId = initial topologyId + 2, rebalanceId = initial rebalanceId + 2)
      // 3. A topology with 2 as the only member, but still with a pending CH (topologyId = initialTopologyId + 3, rebalanceId = initial rebalanceId + 2)
      // 4. A topology ending the rebalance (topologyId = initialTopologyId + 4, rebalanceId = initial rebalanceId + 2)
      // Sometimes node 2 can confirm the rebalance before receiving the topology in step 3, in which case step 3 is skipped.
      // We discard the topologies from steps 1 and 2, to test that the topology update in step 3 is enough to start and finish the rebalance.

      // Block the GET_STATUS command on node 2
      final LocalTopologyManager localTopologyManager2 = TestingUtil.extractGlobalComponent(manager(1),
            LocalTopologyManager.class);
      final CheckPoint checkpoint = new CheckPoint();
      LocalTopologyManager spyLocalTopologyManager2 = spy(localTopologyManager2);
      final CacheTopology initialTopology = localTopologyManager2.getCacheTopology(CACHE_NAME);
      doAnswer(new Answer<Object>() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            int viewId = (Integer) invocation.getArguments()[0];
            checkpoint.trigger("GET_STATUS_" + viewId);
            log.debugf("Blocking the GET_STATUS command on the new coordinator");
            checkpoint.awaitStrict("3 left", 10, TimeUnit.SECONDS);
            return invocation.callRealMethod();
         }
      }).when(spyLocalTopologyManager2).handleStatusRequest(anyInt());

      // Delay the first CH_UPDATE after the merge so that it arrives after
      doAnswer(new Answer<Object>() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            CacheTopology topology = (CacheTopology) invocation.getArguments()[1];
            if (topology.getRebalanceId() == initialTopology.getRebalanceId() + 1) {
               log.debugf("Discarding CH update command %s", topology);
               return null;
            }
            return invocation.callRealMethod();
         }
      }).when(spyLocalTopologyManager2).handleTopologyUpdate(eq(CACHE_NAME), any(CacheTopology.class),
            any(AvailabilityMode.class), anyInt());
      doAnswer(new Answer<Object>() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            CacheTopology topology = (CacheTopology) invocation.getArguments()[1];
            if (topology.getRebalanceId() == initialTopology.getRebalanceId() + 2) {
               log.debugf("Discarding rebalance command %s", topology);
               return null;
            }
            return invocation.callRealMethod();
         }
      }).when(spyLocalTopologyManager2).handleRebalance(eq(CACHE_NAME), any(CacheTopology.class), anyInt());
      TestingUtil.replaceComponent(manager(1), LocalTopologyManager.class, spyLocalTopologyManager2, true);

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
      checkpoint.triggerForever("3 left");

      // Wait for node 2 to install a view with only itself and unblock the GET_STATUS command
      TestingUtil.waitForRehashToComplete(c2);
   }

   public void testLeaveDuringGetTransactions() throws InterruptedException, TimeoutException {
      final CheckPoint checkpoint = new CheckPoint();
      StateProvider stateProvider = TestingUtil.extractComponent(c2, StateProvider.class);
      StateProvider spyStateProvider = spy(stateProvider);
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
      TestingUtil.replaceComponent(c2, StateProvider.class, spyStateProvider, true);

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

