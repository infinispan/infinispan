package org.infinispan.notifications.cachelistener.cluster;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.commands.CommandInvocationId;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.MagicKey;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.util.concurrent.CommandAckCollector;
import org.testng.annotations.Test;

/**
 * Tests for cluster listeners that are specific to non tx
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional")
public abstract class AbstractClusterListenerNonTxTest extends AbstractClusterListenerTest {
   protected AbstractClusterListenerNonTxTest(boolean tx, CacheMode cacheMode) {
      super(tx, cacheMode);
   }

   @Test
   public void testPrimaryOwnerGoesDownAfterSendingEvent() throws InterruptedException, ExecutionException,
                                                                  TimeoutException {
      final Cache<Object, String> cache0 = cache(0, CACHE_NAME);
      Cache<Object, String> cache1 = cache(1, CACHE_NAME);
      Cache<Object, String> cache2 = cache(2, CACHE_NAME);

      ClusterListener clusterListener = listener();
      cache0.addListener(clusterListener);

      CheckPoint checkPoint = new CheckPoint();
      waitUntilNotificationRaised(cache1, checkPoint);
      checkPoint.triggerForever("pre_raise_notification_release");

      final MagicKey key = new MagicKey(cache1, cache2);
      Future<String> future = fork(new Callable<String>() {
         @Override
         public String call() throws Exception {
            return cache0.put(key, FIRST_VALUE);
         }
      });

      checkPoint.awaitStrict("post_raise_notification_invoked", 10, TimeUnit.SECONDS);
      awaitForBackups(cache0);

      // Kill the cache now - note this will automatically unblock the fork thread
      TestingUtil.killCacheManagers(cache1.getCacheManager());

      future.get(10, TimeUnit.SECONDS);

      if (TestingUtil.isTriangleAlgorithm(cacheMode, tx)) {
         //because of the triangle, it is possible to put to be retried multiple times.
         assertTrue(clusterListener.events.size() == 2 || clusterListener.events.size() == 3);
      } else {
         // We should have received 2 events
         assertEquals(clusterListener.events.size(), 2);
      }
      CacheEntryEvent<Object, String> event = clusterListener.events.remove(0);
      assertEquals(event.getType(), Event.Type.CACHE_ENTRY_CREATED);
      CacheEntryCreatedEvent<Object, String> createEvent = (CacheEntryCreatedEvent<Object, String>)event;
      assertFalse(createEvent.isCommandRetried());
      assertEquals(createEvent.getKey(), key);
      assertEquals(createEvent.getValue(), FIRST_VALUE);

      while (!clusterListener.events.isEmpty()) {
         event = clusterListener.events.remove(0);
         // Since it was a retry but the backup got the write the event isn't a CREATE!!
         assertEquals(event.getType(), Event.Type.CACHE_ENTRY_MODIFIED);
         CacheEntryModifiedEvent<Object, String> modEvent = (CacheEntryModifiedEvent<Object, String>) event;
         assertTrue(modEvent.isCommandRetried());
         assertEquals(modEvent.getKey(), key);
         assertEquals(modEvent.getValue(), FIRST_VALUE);
      }
   }

   protected void awaitForBackups(Cache<?, ?> cache) {
      if (TestingUtil.isTriangleAlgorithm(cacheMode, tx)) {
         CommandAckCollector collector = TestingUtil.extractComponent(cache, CommandAckCollector.class);
         List<CommandInvocationId> pendingCommands = collector.getPendingCommands();
         //only 1 put is waiting (it may receive the backup ack, but not the primary ack since it is blocked!)
         assertEquals(1, pendingCommands.size());
         //make sure that the backup received the update
         eventually(() -> !collector.hasPendingBackupAcks(pendingCommands.get(0)));
      }
   }
}
