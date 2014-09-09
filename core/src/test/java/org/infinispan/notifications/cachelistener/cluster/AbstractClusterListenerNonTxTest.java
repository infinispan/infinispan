package org.infinispan.notifications.cachelistener.cluster;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.MagicKey;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.testng.Assert.*;

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

      // Kill the cache now - note this will automatically unblock the fork thread
      TestingUtil.killCacheManagers(cache1.getCacheManager());

      future.get(10, TimeUnit.SECONDS);

      // We should have received 2 events
      assertEquals(clusterListener.events.size(), 2);
      CacheEntryEvent<Object, String> event = clusterListener.events.get(0);
      assertEquals(event.getType(), Event.Type.CACHE_ENTRY_CREATED);
      CacheEntryCreatedEvent<Object, String> createEvent = (CacheEntryCreatedEvent<Object, String>)event;
      assertFalse(createEvent.isCommandRetried());
      assertEquals(createEvent.getKey(), key);
      assertEquals(createEvent.getValue(), FIRST_VALUE);


      event = clusterListener.events.get(1);
      // Since it was a retry but the backup got the write the event isn't a CREATE!!
      assertEquals(event.getType(), Event.Type.CACHE_ENTRY_MODIFIED);
      CacheEntryModifiedEvent<Object, String> modEvent = (CacheEntryModifiedEvent<Object, String>)event;
      assertTrue(modEvent.isCommandRetried());
      assertEquals(modEvent.getKey(), key);
      assertEquals(modEvent.getValue(), FIRST_VALUE);
   }
}
