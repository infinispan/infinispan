package org.infinispan.notifications.cachelistener.cluster;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.BlockingInterceptor;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.distribution.TriangleDistributionInterceptor;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Cluster listener test having a configuration of non tx and dist
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.cluster.ClusterListenerDistTest")
public class ClusterListenerDistTest extends AbstractClusterListenerNonTxTest {
   public ClusterListenerDistTest() {
      super(false, CacheMode.DIST_SYNC);
   }

   @Test
   public void testPrimaryOwnerGoesDownBeforeSendingEvent() throws InterruptedException, TimeoutException,
                                                                   ExecutionException, BrokenBarrierException {
      final Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      ClusterListener clusterListener = new ClusterListener();
      cache0.addListener(clusterListener);

      CyclicBarrier barrier = new CyclicBarrier(2);
      BlockingInterceptor blockingInterceptor = new BlockingInterceptor<>(barrier, PutKeyValueCommand.class, true, false);
      cache1.getAdvancedCache().getAsyncInterceptorChain().addInterceptorBefore(blockingInterceptor, TriangleDistributionInterceptor.class);

      final MagicKey key = new MagicKey(cache1, cache2);
      Future<String> future = fork(() -> cache0.put(key, FIRST_VALUE));

      // Wait until the primary owner has sent the put command successfully to  backup
      barrier.await(10, TimeUnit.SECONDS);
      awaitForBackups(cache0);

      // Kill the cache now - note this will automatically unblock the fork thread
      TestingUtil.killCacheManagers(cache1.getCacheManager());

      // This should return null normally, but since it was retried it returns it's own value :(
      // Maybe some day this can work properly
      assertEquals(future.get(10, TimeUnit.SECONDS), FIRST_VALUE);

      TestingUtil.waitForNoRebalance(cache0, cache2);

      // The command is retried during rebalance, but there are 5 topology updates (thus up to 5 retries)
      // 1: top - 1 NO_REBALANCE
      // 2: top     READ_OLD_WRITE_ALL
      // 3: top     READ_ALL_WRITE_ALL
      // 4: top     READ_NEW_WRITE_ALL
      // 5: top     NO_REBALANCE
      assertTrue("Expected 1 - 5 events, but received " + clusterListener.events,
            clusterListener.events.size() >= 1 && clusterListener.events.size() <= 5);

      Address cache0primary = cache0.getAdvancedCache().getDistributionManager().getCacheTopology().getDistribution(key).primary();
      Address cache2primary = cache2.getAdvancedCache().getDistributionManager().getCacheTopology().getDistribution(key).primary();
      // we expect that now both nodes have the same topology
      assertEquals(cache0primary, cache2primary);

      clusterListener.events.forEach(e -> checkEvent(e, key, false, true));
   }
}
