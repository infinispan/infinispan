package org.infinispan.notifications.cachelistener.cluster;

import static org.infinispan.test.TestingUtil.extractInterceptorChain;
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
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.ControlledRpcManager;
import org.testng.annotations.Test;

/**
 * Cluster listener test having a configuration of non tx and replication
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.cluster.ClusterListenerReplTest")
public class ClusterListenerReplTest extends AbstractClusterListenerNonTxTest {
   public ClusterListenerReplTest() {
      super(false, CacheMode.REPL_SYNC);
   }

   public void testPrimaryOwnerGoesDownBeforeBackupRaisesEvent() throws InterruptedException, TimeoutException,
                                                                   ExecutionException {
      final Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      ClusterListener clusterListener = new ClusterListener();
      cache0.addListener(clusterListener);

      // Now we want to block the outgoing put to the backup owner
      ControlledRpcManager controlledRpcManager = ControlledRpcManager.replaceRpcManager(cache1);

      final MagicKey key = new MagicKey(cache1, cache2);
      Future<String> future = fork(() -> cache0.put(key, FIRST_VALUE));

      // Wait until the primary owner has sent the put command successfully to the backups
      ControlledRpcManager.BlockedRequest<?> blockedPut = controlledRpcManager.expectCommand(PutKeyValueCommand.class);
      // And discard the request
      blockedPut.skipSend();

      // Kill the cache now
      TestingUtil.killCacheManagers(cache1.getCacheManager());

      // This should return null normally, but when it was retried it returns its own value
      String returnValue = future.get(10, TimeUnit.SECONDS);
      assertTrue(returnValue == null || returnValue.equals(FIRST_VALUE));

      // We should have received an event that was marked as retried
      assertTrue(clusterListener.events.size() >= 1);
      // Because a rebalance has 4 phases, the command may be retried 4 times
      assertTrue(clusterListener.events.size() <= 4);
      for (CacheEntryEvent<Object, String> event : clusterListener.events) {
         checkEvent(event, key, true, true);
      }
   }

   public void testPrimaryOwnerGoesDownAfterBackupRaisesEvent() throws InterruptedException, TimeoutException,
                                                                   ExecutionException, BrokenBarrierException {
      final Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      ClusterListener clusterListener = new ClusterListener();
      cache0.addListener(clusterListener);

      CyclicBarrier barrier = new CyclicBarrier(3);
      BlockingInterceptor<?> blockingInterceptor0 = new BlockingInterceptor<>(barrier, PutKeyValueCommand.class,
            true, false);
      extractInterceptorChain(cache0).addInterceptorBefore(blockingInterceptor0, EntryWrappingInterceptor.class);
      BlockingInterceptor<?> blockingInterceptor2 = new BlockingInterceptor<>(barrier, PutKeyValueCommand.class,
            true, false);
      extractInterceptorChain(cache2).addInterceptorBefore(blockingInterceptor2, EntryWrappingInterceptor.class);

      // this is a replicated cache, all other nodes are backup owners
      final MagicKey key = new MagicKey(cache1);
      Future<String> future = fork(() -> cache0.put(key, FIRST_VALUE));

      // Wait until the primary owner has sent the put command successfully to both backups
      barrier.await(10, TimeUnit.SECONDS);

      // Remove the interceptor so the next command can proceed properly
      extractInterceptorChain(cache0).removeInterceptor(BlockingInterceptor.class);
      extractInterceptorChain(cache2).removeInterceptor(BlockingInterceptor.class);
      blockingInterceptor0.suspend(true);
      blockingInterceptor2.suspend(true);

      // Kill the cache now - note this will automatically unblock the fork thread
      TestingUtil.killCacheManagers(cache1.getCacheManager());

      // Unblock the command
      barrier.await(10, TimeUnit.SECONDS);

      // This should return null normally, but since it was retried it returns it's own value :(
      // Maybe some day this can work properly
      String returnValue = future.get(10, TimeUnit.SECONDS);
      assertEquals(FIRST_VALUE, returnValue);

      // We should have received an event that was marked as retried
      assertTrue(clusterListener.events.size() >= 2);
      // Because a rebalance has 4 phases, the command may be retried 4 times
      assertTrue(clusterListener.events.size() <= 4);

      // First create should not be retried since it was sent before node failure.
      checkEvent(clusterListener.events.get(0), key, true, false);

      // Events from retried commands are a MODIFY since CREATE was already done
      for (int i = 1; i < clusterListener.events.size(); i++) {
         CacheEntryEvent<Object, String> event = clusterListener.events.get(i);
         checkEvent(event, key, false, true);
      }

      checkEvent(clusterListener.events.get(1), key, false, true);
   }
}
