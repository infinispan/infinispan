package org.infinispan.notifications.cachelistener.cluster;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

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
import org.infinispan.test.eventually.Eventually;
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

      // The command is retried during rebalance, but there are two topologies - in the first (rebalancing) topology
      // one node can be primary owner and in the second (rebalanced) the other. In this case, it's possible that
      // the listener is fired both in the first topology and then after the response from primary owner arrives
      // and the originator now has become the new primary owner.
      // Similar situation is possible with triangle algorithm (TODO pruivo: elaborate)
      assertTrue(clusterListener.events.size() >= 2);
      assertTrue(clusterListener.events.size() <= 3);
      checkEvent(clusterListener.events.get(0), key, true, false);

      Address cache0primary = cache0.getAdvancedCache().getDistributionManager().getPrimaryLocation(key);
      Address cache2primary = cache2.getAdvancedCache().getDistributionManager().getPrimaryLocation(key);
      // we expect that now both nodes have the same topology
      assertEquals(cache0primary, cache2primary);
      // This is possible after rebalance; when rebalancing, primary owner is always the old backup

      checkEvent(clusterListener.events.get(1), key, false, true);
      if (clusterListener.events.size() == 3) {
         checkEvent(clusterListener.events.get(2), key, false, true);
      }
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
         Eventually.eventually(() -> !collector.hasPendingBackupAcks(pendingCommands.get(0)));
      }
   }
}
