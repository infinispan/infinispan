package org.infinispan.statetransfer;

import static java.util.stream.IntStream.range;
import static org.infinispan.test.TestingUtil.waitForNoRebalance;
import static org.infinispan.util.BlockingLocalTopologyManager.confirmTopologyUpdate;
import static org.infinispan.util.BlockingLocalTopologyManager.finishRebalance;
import static org.infinispan.util.BlockingLocalTopologyManager.replaceTopologyManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.globalstate.NoOpGlobalConfigurationManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.InCacheMode;
import org.infinispan.topology.CacheTopology;
import org.testng.annotations.Test;

// for JIRA ISPN-15299
@Test(groups = "functional", testName = "statetransfer.PutAllDuringJoinTest")
@InCacheMode({CacheMode.DIST_SYNC, CacheMode.REPL_SYNC})
public class PutAllDuringJoinTest extends MultipleCacheManagersTest {

   private static final int NUM_SEGMENTS = 6;
   private static final int NUM_KEYS = NUM_SEGMENTS * 3;

   @Override
   protected void createCacheManagers() throws Throwable {
      createCluster(2);
   }

   @Override
   protected void amendCacheManagerBeforeStart(EmbeddedCacheManager cm) {
      NoOpGlobalConfigurationManager.amendCacheManager(cm);
   }

   public void testPutMapDuringStateTransferWithEmptyCache() throws InterruptedException, ExecutionException, TimeoutException {
      doPutAllDuringStateTransfer(false);
   }

   public void testPutMapDuringStateTransferWithPopulatedCache() throws InterruptedException, ExecutionException, TimeoutException {
      doPutAllDuringStateTransfer(true);
   }

   @SuppressWarnings("resource")
   private void doPutAllDuringStateTransfer(boolean populateCache) throws InterruptedException, ExecutionException, TimeoutException {
      var cacheName = "c1-" + (!populateCache ? "empty-" : "") + cacheMode.toString().toLowerCase();
      var config = getDefaultClusteredCacheConfig(cacheMode)
            .clustering().hash().numOwners(2).numSegments(NUM_SEGMENTS)
            .build();

      // cache is created in node0 only
      manager(0).defineConfiguration(cacheName, config);
      waitForNoRebalance(manager(0).getCache(cacheName));
      assertFalse(manager(1).cacheExists(cacheName));

      var dataMap = new TreeMap<>();
      if (populateCache) {
         range(0, NUM_KEYS).forEach(num -> {
            dataMap.put(num, "value" + num);
            cache(0, cacheName).put(num, "value");
         });
      } else {
         range(0, NUM_KEYS).forEach(num -> dataMap.put(num, "value" + num));
      }

      // let's block the topology change
      var localTopologyManager0 = replaceTopologyManager(manager(0), cacheName);
      var localTopologyManager1 = replaceTopologyManager(manager(1), cacheName);

      // let's create the cache in node1, topology changes are blocked at this point
      manager(1).defineConfiguration(cacheName, config);
      // using a fork because it will block
      var cacheStarted = fork(() -> manager(1).getCache(cacheName));

      // let's READ_OLD_WRITE_ALL be installed, it is in this topology that we want to be to perform the putAll
      confirmTopologyUpdate(CacheTopology.Phase.READ_OLD_WRITE_ALL, localTopologyManager0, localTopologyManager1);

      try {
         // at this point, read-ch should be node0 only, write-ch must have both node0 (primary) and node1 (backup)
         var topology = localTopologyManager0.getCacheTopology(cacheName);
         for (var i = 0; i < NUM_SEGMENTS; ++i) {
            assertEquals(1, topology.getCurrentCH().locateOwnersForSegment(i).size());
            assertEquals(2, topology.getPendingCH().locateOwnersForSegment(i).size());
         }

         // perform putAll
         // put all will be sent to the backup owner, node1, which is not allow to read, it does not even try the DataContainer
         // It will fetch the value from the primary owner.
         cache(0, cacheName).putAll(dataMap);

         assertEquals(NUM_KEYS, cache(0, cacheName).size());
      } finally {
         finishRebalance(CacheTopology.Phase.READ_ALL_WRITE_ALL, localTopologyManager0, localTopologyManager1);
      }
      cacheStarted.get(10, TimeUnit.SECONDS);
   }
}
