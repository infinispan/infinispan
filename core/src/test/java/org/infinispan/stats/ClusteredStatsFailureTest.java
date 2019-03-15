package org.infinispan.stats;


import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.stats.impl.AbstractClusterStats;
import org.infinispan.test.Mocks;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.util.ControlledTimeService;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "stats.ClusteredStatsFailureTest")
public class ClusteredStatsFailureTest extends MultipleCacheManagersTest {

   protected final int CLUSTER_SIZE = 3;

   ControlledTimeService controlledTimeService = new ControlledTimeService();
   ClusterCacheStats clusterStats;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      // Have to actually enable stats
      cfg.jmxStatistics().enable();
      createCluster(cfg, CLUSTER_SIZE);
      waitForClusterToForm();

      // Put in controlled time service so we can force refresh to happen
      TestingUtil.replaceComponent(manager(0), TimeService.class, controlledTimeService, true);

      clusterStats = TestingUtil.extractComponent(cache(0), ClusterCacheStats.class);
   }

   protected void refreshClusterStats() {
      // Force cluster stats to be refreshed
      controlledTimeService.advance(AbstractClusterStats.DEFAULT_STALE_STATS_THRESHOLD + 1);
      // Force retrieval of stats
      // Make sure read time isn't compiled away
      if (clusterStats.getAverageReadTime() == 128787) {
         System.out.println("Magic happened");
      }
   }

   public void testNodeDiesDuringProcessing() throws InterruptedException, TimeoutException, BrokenBarrierException, ExecutionException {
      Cache<?, ?> cache2 = cache(2);
      CheckPoint checkPoint = new CheckPoint();

      checkPoint.triggerForever(Mocks.AFTER_RELEASE);

      Mocks.blockingMock(checkPoint, LockManager.class, cache2, (stub, m) ->
         stub.when(m).getNumberOfLocksHeld());

      Future<Void> future = fork(this::refreshClusterStats);

      assertTrue(checkPoint.await(Mocks.BEFORE_INVOCATION, 10, TimeUnit.SECONDS));

      TestingUtil.killCacheManagers(manager(2));

      checkPoint.triggerForever(Mocks.BEFORE_RELEASE);

      future.get(10, TimeUnit.SECONDS);
   }

   public void testNodeShuttingDownDuringProcessing() throws InterruptedException, TimeoutException, ExecutionException {
      Cache<Object, Object> cache1 = cache(1);

      int countBefore = clusterStats.getCurrentNumberOfEntries();

      cache1.put("foo", "bar");

      // Puts cache in terminated state
      cache1.stop();

      // Stopped cached should not cause stats to fail
      refreshClusterStats();

      // This ensures that stats were actually invoked
      assertEquals(countBefore + 1, clusterStats.getCurrentNumberOfEntries());
   }
}
