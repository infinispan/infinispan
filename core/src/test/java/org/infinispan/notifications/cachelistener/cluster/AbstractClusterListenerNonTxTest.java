package org.infinispan.notifications.cachelistener.cluster;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.MagicKey;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.remoting.transport.Address;
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
      Future<String> future = fork(() -> cache0.put(key, FIRST_VALUE));

      checkPoint.awaitStrict("post_raise_notification_invoked", 10, TimeUnit.SECONDS);
      awaitForBackups(cache0);

      // Kill the cache now - note this will automatically unblock the fork thread
      TestingUtil.killCacheManagers(cache1.getCacheManager());

      future.get(10, TimeUnit.SECONDS);

      TestingUtil.waitForNoRebalance(cache0, cache2);

      // The command is retried during rebalance, but there are 5 topology updates (thus up to 5 retries)
      // 1: top - 1 NO_REBALANCE
      // 2: top     READ_OLD_WRITE_ALL
      // 3: top     READ_ALL_WRITE_ALL
      // 4: top     READ_NEW_WRITE_ALL
      // 5: top     NO_REBALANCE
      assertTrue("Expected 2 - 6 events, but received " + clusterListener.events,
            clusterListener.events.size() >= 2 && clusterListener.events.size() <= 6);
      // Since the first event was generated properly it is a create without retry
      checkEvent(clusterListener.events.get(0), key, true, false);

      Address cache0primary = cache0.getAdvancedCache().getDistributionManager().getPrimaryLocation(key);
      Address cache2primary = cache2.getAdvancedCache().getDistributionManager().getPrimaryLocation(key);
      // we expect that now both nodes have the same topology
      assertEquals(cache0primary, cache2primary);

      // Any extra events would be retries as modifications
      clusterListener.events.stream().skip(1).forEach(e -> checkEvent(e, key, false, true));
   }

   protected void checkEvent(CacheEntryEvent<Object, String> event, MagicKey key, boolean isCreated, boolean isRetried) {
      CacheEntryCreatedEvent<Object, String> createEvent;
      if (isCreated) {
         assertEquals(event.getType(), Event.Type.CACHE_ENTRY_CREATED);
         createEvent = (CacheEntryCreatedEvent<Object, String>)event;
         assertEquals(createEvent.isCommandRetried(), isRetried);
      } else {
         assertEquals(event.getType(), Event.Type.CACHE_ENTRY_MODIFIED);
         CacheEntryModifiedEvent<Object, String> modEvent = (CacheEntryModifiedEvent<Object, String>) event;
         assertTrue(modEvent.isCommandRetried());
      }
      assertEquals(event.getKey(), key);
      assertEquals(event.getValue(), FIRST_VALUE);
   }

   protected void awaitForBackups(Cache<?, ?> cache) {
      if (TestingUtil.isTriangleAlgorithm(cacheMode, tx)) {
         CommandAckCollector collector = TestingUtil.extractComponent(cache, CommandAckCollector.class);
         List<Long> pendingCommands = collector.getPendingCommands();
         //only 1 put is waiting (it may receive the backup ack, but not the primary ack since it is blocked!)
         assertEquals(1, pendingCommands.size());
         //make sure that the backup received the update
         eventually(() -> !collector.hasPendingBackupAcks(pendingCommands.get(0)));
      }
   }
}
