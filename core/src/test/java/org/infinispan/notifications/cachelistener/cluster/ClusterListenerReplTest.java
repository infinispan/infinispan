package org.infinispan.notifications.cachelistener.cluster;

import org.infinispan.Cache;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.BlockingInterceptor;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.EntryWrappingInterceptor;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.tx.dld.ControlledRpcManager;
import org.testng.annotations.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.testng.AssertJUnit.*;

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
                                                                   ExecutionException, BrokenBarrierException {
      final Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      ClusterListener clusterListener = new ClusterListener();
      cache0.addListener(clusterListener);

      // Now we want to block the outgoing put to the backup owner
      RpcManager rpcManager = TestingUtil.extractComponent(cache1, RpcManager.class);
      ControlledRpcManager controlledRpcManager = new ControlledRpcManager(rpcManager);
      controlledRpcManager.blockBefore(PutKeyValueCommand.class);
      TestingUtil.replaceComponent(cache1, RpcManager.class, controlledRpcManager, true);

      final MagicKey key = new MagicKey(cache1, cache2);
      Future<String> future = fork(new Callable<String>() {
         @Override
         public String call() throws Exception {
            return cache0.put(key, FIRST_VALUE);
         }
      });

      // Wait until the primary owner has sent the put command successfully to  backup
      controlledRpcManager.waitForCommandToBlock(10, TimeUnit.SECONDS);

      // Kill the cache now
      TestingUtil.killCacheManagers(cache1.getCacheManager());

      // This should return null normally, but since it was retried it returns it's own value :(
      // Maybe some day this can work properly
      assertNull(future.get(10, TimeUnit.SECONDS));

      // We should have received an event that was marked as retried
      assertEquals(clusterListener.events.size(), 1);
      CacheEntryEvent<Object, String> event = clusterListener.events.get(0);
      // Since it was a retry but the backup got the write the event isn't a CREATE!!
      assertEquals(event.getType(), Event.Type.CACHE_ENTRY_CREATED);
      CacheEntryCreatedEvent<Object, String> modEvent = (CacheEntryCreatedEvent<Object, String>)event;
      assertTrue(modEvent.isCommandRetried());
      assertEquals(modEvent.getKey(), key);
      assertEquals(modEvent.getValue(), FIRST_VALUE);
   }

   public void testPrimaryOwnerGoesDownAfterBackupRaisesEvent() throws InterruptedException, TimeoutException,
                                                                   ExecutionException, BrokenBarrierException {
      final Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      ClusterListener clusterListener = new ClusterListener();
      cache0.addListener(clusterListener);

      CyclicBarrier barrier = new CyclicBarrier(3);
      BlockingInterceptor blockingInterceptor0 = new BlockingInterceptor(barrier, PutKeyValueCommand.class,
            true, false);
      cache0.getAdvancedCache().addInterceptorBefore(blockingInterceptor0, EntryWrappingInterceptor.class);
      BlockingInterceptor blockingInterceptor2 = new BlockingInterceptor(barrier, PutKeyValueCommand.class,
            true, false);
      cache2.getAdvancedCache().addInterceptorBefore(blockingInterceptor2, EntryWrappingInterceptor.class);

      final MagicKey key = new MagicKey(cache1, cache2);
      Future<String> future = fork(new Callable<String>() {
         @Override
         public String call() throws Exception {
            return cache0.put(key, FIRST_VALUE);
         }
      });

      // Wait until the primary owner has sent the put command successfully to both backups
      barrier.await(10, TimeUnit.SECONDS);

      // Remove the interceptor so the next command can proceed properly
      cache2.getAdvancedCache().removeInterceptor(BlockingInterceptor.class);

      // Kill the cache now - note this will automatically unblock the fork thread
      TestingUtil.killCacheManagers(cache1.getCacheManager());

      // Unblock the command
      barrier.await(10, TimeUnit.SECONDS);

      // This should return null normally, but since it was retried it returns it's own value :(
      // Maybe some day this can work properly
      assertEquals(future.get(10, TimeUnit.SECONDS), FIRST_VALUE);

      assertEquals(clusterListener.events.size(), 2);
      CacheEntryEvent<Object, String> event = clusterListener.events.get(0);
      assertEquals(event.getType(), Event.Type.CACHE_ENTRY_CREATED);
      CacheEntryCreatedEvent<Object, String> createEvent = (CacheEntryCreatedEvent<Object, String>)event;
      assertFalse(createEvent.isCommandRetried());
      assertEquals(createEvent.getKey(), key);
      assertEquals(createEvent.getValue(), FIRST_VALUE);

      // We should have received an event that was marked as retried
      event = clusterListener.events.get(1);
      // Since it was a retry but the backup got the write the event isn't a CREATE!!
      assertEquals(event.getType(), Event.Type.CACHE_ENTRY_MODIFIED);
      CacheEntryModifiedEvent<Object, String> modEvent = (CacheEntryModifiedEvent<Object, String>)event;
      assertTrue(modEvent.isCommandRetried());
      assertEquals(modEvent.getKey(), key);
      assertEquals(modEvent.getValue(), FIRST_VALUE);
   }
}
