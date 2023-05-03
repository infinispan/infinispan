package org.infinispan.statetransfer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.infinispan.test.TestingUtil.sequence;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.globalstate.NoOpGlobalConfigurationManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.Mocks;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestException;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.InTransactionMode;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.transaction.TransactionMode;
import org.jgroups.protocols.DISCARD;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "statetransfer.ClusterTopologyManagerTest")
@CleanupAfterMethod
public class ClusterTopologyManagerTest extends MultipleCacheManagersTest {

   public static final String CACHE_NAME = "testCache";
   private static final String OTHER_CACHE_NAME = "other_cache";
   private ConfigurationBuilder defaultConfig;
   private Cache<?, ?> c1, c2, c3;
   private DISCARD d1, d2, d3;

   @Override
   public Object[] factory() {
      return new Object[] {
         new ClusterTopologyManagerTest().cacheMode(CacheMode.DIST_SYNC).transactional(true),
      };
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      defaultConfig = getDefaultClusteredCacheConfig(cacheMode, transactional);
      createClusteredCaches(3, defaultConfig, new TransportFlags().withFD(true).withMerge(true), CACHE_NAME);

      c1 = cache(0, CACHE_NAME);
      c2 = cache(1, CACHE_NAME);
      c3 = cache(2, CACHE_NAME);
      d1 = TestingUtil.getDiscardForCache(c1.getCacheManager());
      d2 = TestingUtil.getDiscardForCache(c2.getCacheManager());
      d3 = TestingUtil.getDiscardForCache(c3.getCacheManager());
   }

   @Override
   protected void amendCacheManagerBeforeStart(EmbeddedCacheManager cm) {
      NoOpGlobalConfigurationManager.amendCacheManager(cm);
   }

   public void testNodeAbruptLeave() {
      // Create some more caches to trigger ISPN-2572
      ConfigurationBuilder cfg = defaultConfig;
      defineConfigurationOnAllManagers("cache2", cfg);
      defineConfigurationOnAllManagers("cache3", cfg);
      defineConfigurationOnAllManagers("cache4", cfg);
      defineConfigurationOnAllManagers("cache5", cfg);
      cache(0, "cache2");
      cache(1, "cache2");
      cache(0, "cache3");
      cache(2, "cache3");
      cache(1, "cache4");
      cache(2, "cache4");
      cache(0, "cache5");
      cache(1, "cache5");

      // create the partitions
      log.debugf("Splitting cluster");
      d3.discardAll(true);
      TestingUtil.installNewView(manager(0), manager(1));
      TestingUtil.installNewView(manager(2));

      // wait for the partitions to form
      long startTime = System.currentTimeMillis();
      TestingUtil.blockUntilViewsReceived(30000, false, c1, c2);
      TestingUtil.blockUntilViewsReceived(30000, false, c3);
      TestingUtil.waitForNoRebalance(c1, c2);
      TestingUtil.waitForNoRebalance(c3);

      TestingUtil.waitForNoRebalance(cache(0, "cache2"), cache(1, "cache2"));
      TestingUtil.waitForNoRebalance(cache(0, "cache3"));
      TestingUtil.waitForNoRebalance(cache(1, "cache4"));
      TestingUtil.waitForNoRebalance(cache(0, "cache5"), cache(1, "cache5"));

      long endTime = System.currentTimeMillis();
      log.debugf("Recovery took %s", Util.prettyPrintTime(endTime - startTime));
      assert endTime - startTime < 30000 : "Recovery took too long: " + Util.prettyPrintTime(endTime - startTime);

      // Check that a new node can join
      EmbeddedCacheManager newCm = addClusterEnabledCacheManager(new TransportFlags().withFD(true).withMerge(true));
      newCm.defineConfiguration(CACHE_NAME, defaultConfig.build());
      Cache<Object, Object> c4 = cache(3, CACHE_NAME);
      TestingUtil.blockUntilViewsReceived(30000, true, c1, c2, c4);
      TestingUtil.waitForNoRebalance(c1, c2, c4);

      newCm.defineConfiguration("cache2", defaultConfig.build());
      newCm.defineConfiguration("cache3", defaultConfig.build());
      newCm.defineConfiguration("cache4", defaultConfig.build());
      newCm.defineConfiguration("cache5", defaultConfig.build());
      cache(3, "cache2");
      cache(3, "cache3");
      cache(3, "cache4");
      cache(3, "cache5");
      TestingUtil.waitForNoRebalance(cache(0, "cache2"), cache(1, "cache2"), cache(3, "cache2"));
      TestingUtil.waitForNoRebalance(cache(0, "cache3"), cache(3, "cache3"));
      TestingUtil.waitForNoRebalance(cache(1, "cache4"), cache(3, "cache4"));
      TestingUtil.waitForNoRebalance(cache(0, "cache5"), cache(1, "cache5"), cache(3, "cache5"));
   }

   public void testClusterRecoveryAfterCoordLeave() {
      // create the partitions
      log.debugf("Splitting cluster");
      d1.discardAll(true);
      TestingUtil.installNewView(manager(0));
      TestingUtil.installNewView(manager(1), manager(2));

      // wait for the partitions to form
      long startTime = System.currentTimeMillis();
      TestingUtil.blockUntilViewsReceived(30000, false, c1);
      TestingUtil.blockUntilViewsReceived(30000, false, c2, c3);
      TestingUtil.waitForNoRebalance(c1);
      TestingUtil.waitForNoRebalance(c2, c3);

      long endTime = System.currentTimeMillis();
      log.debugf("Recovery took %s", Util.prettyPrintTime(endTime - startTime));
      assert endTime - startTime < 30000 : "Recovery took too long: " + Util.prettyPrintTime(endTime - startTime);

      // Check that a new node can join
      addClusterEnabledCacheManager(new TransportFlags().withFD(true).withMerge(true));
      manager(3).defineConfiguration(CACHE_NAME, defaultConfig.build());
      Cache<Object, Object> c4 = cache(3, CACHE_NAME);
      TestingUtil.blockUntilViewsReceived(30000, true, c2, c3, c4);
      TestingUtil.waitForNoRebalance(c2, c3, c4);
   }

   public void testClusterRecoveryAfterThreeWaySplit() {
      // create the partitions
      log.debugf("Splitting the cluster in three");
      d1.discardAll(true);
      d2.discardAll(true);
      d3.discardAll(true);

      TestingUtil.installNewView(manager(0));
      TestingUtil.installNewView(manager(1));
      TestingUtil.installNewView(manager(2));

      // wait for the partitions to form
      TestingUtil.blockUntilViewsReceived(30000, false, c1);
      TestingUtil.blockUntilViewsReceived(30000, false, c2);
      TestingUtil.blockUntilViewsReceived(30000, false, c3);
      TestingUtil.waitForNoRebalance(c1);
      TestingUtil.waitForNoRebalance(c2);
      TestingUtil.waitForNoRebalance(c3);

      // merge the remaining partitions
      log.debugf("Merging the cluster partitions");
      d1.discardAll(false);
      d2.discardAll(false);
      d3.discardAll(false);

      // wait for the merged cluster to form
      long startTime = System.currentTimeMillis();
      TestingUtil.blockUntilViewsReceived(60000, c1, c2, c3);
      TestingUtil.waitForNoRebalance(c1, c2, c3);

      long endTime = System.currentTimeMillis();
      log.debugf("Merge took %s", Util.prettyPrintTime(endTime - startTime));
      assert endTime - startTime < 30000 : "Merge took too long: " + Util.prettyPrintTime(endTime - startTime);

      // Check that a new node can join
      addClusterEnabledCacheManager(new TransportFlags().withFD(true).withMerge(true));
      manager(3).defineConfiguration(CACHE_NAME, defaultConfig.build());
      Cache<Object, Object> c4 = cache(3, CACHE_NAME);
      TestingUtil.blockUntilViewsReceived(30000, true, c1, c2, c3, c4);
      TestingUtil.waitForNoRebalance(c1, c2, c3, c4);
   }

   public void testClusterRecoveryAfterSplitAndCoordLeave() {
      // create the partitions
      log.debugf("Splitting the cluster in three");
      d1.discardAll(true);
      d2.discardAll(true);
      d3.discardAll(true);

      TestingUtil.installNewView(manager(0));
      TestingUtil.installNewView(manager(1));
      TestingUtil.installNewView(manager(2));

      // wait for the partitions to form
      TestingUtil.blockUntilViewsReceived(30000, false, c1);
      TestingUtil.blockUntilViewsReceived(30000, false, c2);
      TestingUtil.blockUntilViewsReceived(30000, false, c3);
      TestingUtil.waitForNoRebalance(c1);
      TestingUtil.waitForNoRebalance(c2);
      TestingUtil.waitForNoRebalance(c3);

      // kill the coordinator
      manager(0).stop();

      // merge the two remaining partitions
      log.debugf("Merging the cluster partitions");
      d2.discardAll(false);
      d3.discardAll(false);

      // wait for the merged cluster to form
      long startTime = System.currentTimeMillis();
      TestingUtil.blockUntilViewsReceived(30000, c2, c3);
      TestingUtil.waitForNoRebalance(c2, c3);

      long endTime = System.currentTimeMillis();
      log.debugf("Merge took %s", Util.prettyPrintTime(endTime - startTime));
      assert endTime - startTime < 30000 : "Merge took too long: " + Util.prettyPrintTime(endTime - startTime);

      // Check that a new node can join
      addClusterEnabledCacheManager(new TransportFlags().withFD(true).withMerge(true));
      manager(3).defineConfiguration(CACHE_NAME, defaultConfig.build());
      Cache<Object, Object> c4 = cache(3, CACHE_NAME);
      TestingUtil.blockUntilViewsReceived(30000, true, c2, c3, c4);
      TestingUtil.waitForNoRebalance(c2, c3, c4);
   }

   public void testClusterRecoveryWithRebalance() throws Exception {
      // Compute the merge coordinator by sorting the JGroups addresses, the same way MERGE2/3 do
      List<Address> members = new ArrayList<>(manager(0).getMembers());
      Collections.sort(members);
      Address mergeCoordAddress = members.get(0);
      log.debugf("The merge coordinator will be %s", mergeCoordAddress);
      EmbeddedCacheManager mergeCoordManager = manager(mergeCoordAddress);
      int mergeCoordIndex = cacheManagers.indexOf(mergeCoordManager);

      // create the partitions
      log.debugf("Splitting the cluster in three");
      d1.discardAll(true);
      d2.discardAll(true);
      d3.discardAll(true);

      TestingUtil.installNewView(manager(0));
      TestingUtil.installNewView(manager(1));
      TestingUtil.installNewView(manager(2));

      // wait for the coordinator to be separated (don't care about the others)
      TestingUtil.blockUntilViewsReceived(30000, false, c1);
      TestingUtil.blockUntilViewsReceived(30000, false, c2);
      TestingUtil.blockUntilViewsReceived(30000, false, c3);
      TestingUtil.waitForNoRebalance(c1);
      TestingUtil.waitForNoRebalance(c2);
      TestingUtil.waitForNoRebalance(c3);

      // Disable DISCARD *only* on the merge coordinator
      if (mergeCoordIndex == 0) d1.discardAll(false);
      if (mergeCoordIndex == 1) d2.discardAll(false);
      if (mergeCoordIndex == 2) d3.discardAll(false);

      int viewIdAfterSplit = mergeCoordManager.getTransport().getViewId();
      final CheckPoint checkpoint = new CheckPoint();
      blockRebalanceStart(mergeCoordManager, checkpoint, 2);

      EmbeddedCacheManager cm4 = addClusterEnabledCacheManager(new TransportFlags().withFD(true).withMerge(true));
      blockRebalanceStart(cm4, checkpoint, 2);
      // Force the initialization of the transport
      cm4.defineConfiguration(CACHE_NAME, defaultConfig.build());
      cm4.defineConfiguration(OTHER_CACHE_NAME, defaultConfig.build());
      cm4.getCache(OTHER_CACHE_NAME);
      TestingUtil.blockUntilViewsReceived(30000, manager(mergeCoordIndex), cm4);
      Future<Cache<Object,Object>> cacheFuture = fork(() -> cm4.getCache(CACHE_NAME));

      log.debugf("Waiting for the REBALANCE_START command to reach the merge coordinator");
      checkpoint.awaitStrict("rebalance_" + Arrays.asList(mergeCoordAddress, cm4.getAddress()), 10, SECONDS);

      // merge the partitions
      log.debugf("Merging the cluster partitions");
      d1.discardAll(false);
      d2.discardAll(false);
      d3.discardAll(false);

      // wait for the JGroups merge
      long startTime = System.currentTimeMillis();
      TestingUtil.blockUntilViewsReceived(30000, cacheManagers);
      TestingUtil.waitForNoRebalance(caches(CACHE_NAME));

      // unblock the REBALANCE_START command
      log.debugf("Unblocking the REBALANCE_START command on the coordinator");
      checkpoint.triggerForever("merge");

      // wait for the 4th cache to finish joining
      Cache<Object, Object> c4 = cacheFuture.get(30, SECONDS);
      TestingUtil.waitForNoRebalance(c1, c2, c3, c4);

      long endTime = System.currentTimeMillis();
      log.debugf("Merge took %s", Util.prettyPrintTime(endTime - startTime));
      assert endTime - startTime < 30000 : "Merge took too long: " + Util.prettyPrintTime(endTime - startTime);

      // Check that another node can join
      EmbeddedCacheManager cm5 = addClusterEnabledCacheManager(new TransportFlags().withFD(true).withMerge(true));
      cm5.defineConfiguration(CACHE_NAME, defaultConfig.build());
      Cache<Object, Object> c5 = cm5.getCache(CACHE_NAME);
      TestingUtil.blockUntilViewsReceived(30000, true, c1, c2, c3, c4, c5);
      TestingUtil.waitForNoRebalance(c1, c2, c3, c4, c5);
   }

   protected void blockRebalanceStart(final EmbeddedCacheManager manager, final CheckPoint checkpoint, final int numMembers) {
      final LocalTopologyManager localTopologyManager = TestingUtil.extractGlobalComponent(manager,
            LocalTopologyManager.class);
      LocalTopologyManager spyLocalTopologyManager = spy(localTopologyManager);
      doAnswer(invocation -> {
         CacheTopology topology = (CacheTopology) invocation.getArguments()[1];
         List<Address> members = topology.getMembers();
         checkpoint.trigger("rebalance_" + members);
         if (members.size() == numMembers) {
            log.debugf("Blocking the REBALANCE_START command with members %s on %s", members, manager.getAddress());
            return sequence(checkpoint.future("merge", 30, SECONDS, testExecutor()),
                            () -> Mocks.callRealMethod(invocation));
         }
         return invocation.callRealMethod();
      }).when(spyLocalTopologyManager).handleRebalance(eq(CACHE_NAME), any(CacheTopology.class), anyInt(),
                                                          any(Address.class));
      TestingUtil.replaceComponent(manager, LocalTopologyManager.class, spyLocalTopologyManager, true);
   }

   /*
    * Test that cluster recovery can finish if one of the members leaves before sending the status response.
    */
   public void testAbruptLeaveAfterGetStatus() throws TimeoutException, InterruptedException {
      // Block the GET_STATUS command on node 2
      final LocalTopologyManager localTopologyManager2 = TestingUtil.extractGlobalComponent(manager(1),
            LocalTopologyManager.class);
      final CheckPoint checkpoint = new CheckPoint();
      LocalTopologyManager spyLocalTopologyManager2 = spy(localTopologyManager2);
      final CacheTopology initialTopology = localTopologyManager2.getCacheTopology(CACHE_NAME);
      log.debugf("Starting with topology %d", initialTopology.getTopologyId());

      doAnswer(invocation -> {
         int viewId = (Integer) invocation.getArguments()[0];
         checkpoint.trigger("GET_STATUS_" + viewId);
         log.debugf("Blocking the GET_STATUS command on the new coordinator");
         checkpoint.awaitStrict("3 left", 10, SECONDS);
         return invocation.callRealMethod();
      }).when(spyLocalTopologyManager2).handleStatusRequest(anyInt());

      // There should be no topology update or rebalance with 2 members
      CompletableFuture<Void> update2MembersFuture = new CompletableFuture<>();
      doAnswer(invocation -> {
         CacheTopology topology = (CacheTopology) invocation.getArguments()[1];
         if (topology.getMembers().size() == 2) {
            log.debugf("Found CH update with 2 mem %s", topology);
            update2MembersFuture.completeExceptionally(new TestException());
         }
         return invocation.callRealMethod();
      }).when(spyLocalTopologyManager2).handleTopologyUpdate(eq(CACHE_NAME), any(CacheTopology.class),
                                                             any(AvailabilityMode.class), anyInt(), any(Address.class));
      doAnswer(invocation -> {
         CacheTopology topology = (CacheTopology) invocation.getArguments()[1];
         if (topology.getMembers().size() == 2) {
            log.debugf("Discarding rebalance command %s", topology);
            update2MembersFuture.completeExceptionally(new TestException());
         }
         return invocation.callRealMethod();
      }).when(spyLocalTopologyManager2).handleRebalance(eq(CACHE_NAME), any(CacheTopology.class), anyInt(),
                                                        any(Address.class));
      TestingUtil.replaceComponent(manager(1), LocalTopologyManager.class, spyLocalTopologyManager2, true);

      // Node 1 (the coordinator) dies. Node 2 becomes coordinator and tries to call GET_STATUS
      killNode(manager(0), new EmbeddedCacheManager[]{manager(1), manager(2)});

      // Wait for the GET_STATUS command and stop node 3 abruptly
      int viewId = manager(1).getTransport().getViewId();
      checkpoint.awaitStrict("GET_STATUS_" + viewId, 10, SECONDS);
      killNode(manager(2), new EmbeddedCacheManager[]{manager(1)});
      checkpoint.triggerForever("3 left");

      // Wait for node 2 to install a view with only itself and unblock the GET_STATUS command
      TestingUtil.waitForNoRebalance(c2);

      // Check there was no topology update or rebalance with 2 members
      update2MembersFuture.complete(null);
      update2MembersFuture.join();
   }

   private void killNode(EmbeddedCacheManager nodeToKill, EmbeddedCacheManager[] nodesToKeep) {
      log.debugf("Killing node %s", nodeToKill);
      d1.discardAll(true);
      TestingUtil.installNewView(nodeToKill);
      nodeToKill.stop();
      TestingUtil.installNewView(nodesToKeep);
      TestingUtil.blockUntilViewsReceived(30000, false, nodesToKeep);
   }

   @InTransactionMode(TransactionMode.TRANSACTIONAL)
   public void testLeaveDuringGetTransactions() throws InterruptedException, TimeoutException {
      final CheckPoint checkpoint = new CheckPoint();
      StateProvider stateProvider = TestingUtil.extractComponent(c2, StateProvider.class);
      StateProvider spyStateProvider = spy(stateProvider);
      doAnswer(invocation -> {
         int topologyId = (Integer) invocation.getArguments()[1];
         checkpoint.trigger("GET_TRANSACTIONS");
         log.debugf("Blocking the GET_TRANSACTIONS(%d) command on the %s", topologyId, c2);
         checkpoint.awaitStrict("LEAVE", 10, SECONDS);
         return invocation.callRealMethod();
      }).when(spyStateProvider).getTransactionsForSegments(any(Address.class), anyInt(), any());
      TestingUtil.replaceComponent(c2, StateProvider.class, spyStateProvider, true);

      long startTime = System.currentTimeMillis();
      manager(2).stop();

      checkpoint.awaitStrict("GET_TRANSACTIONS", 10, SECONDS);
      manager(1).stop();
      checkpoint.trigger("LEAVE");

      TestingUtil.blockUntilViewsReceived(30000, false, c1);
      TestingUtil.waitForNoRebalance(c1);
      long endTime = System.currentTimeMillis();
      log.debugf("Recovery took %s", Util.prettyPrintTime(endTime - startTime));
      assert endTime - startTime < 30000 : "Recovery took too long: " + Util.prettyPrintTime(endTime - startTime);
   }
}
