package org.infinispan.notifications.cachelistener.cluster;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.MagicKey;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryExpired;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryExpiredEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * Cluster listener test which confirms that Listener is able to receive an event according to the type of the method
 *
 * @author kazuhira-r
 * @since 9.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.cluster.ClusterListenerDistEventTypeTest")
public class ClusterListenerDistEventTypeTest extends AbstractClusterListenerTest {
    public ClusterListenerDistEventTypeTest() {
        super(false, CacheMode.DIST_SYNC);
    }

    @Test
    public void testReceiveCreatedEventType() {
        Cache<Object, String> cache0 = cache(0, CACHE_NAME);
        Cache<Object, String> cache1 = cache(1, CACHE_NAME);
        Cache<Object, String> cache2 = cache(2, CACHE_NAME);

        ClusterEventTypeReceiveListener listener0 = new ClusterEventTypeReceiveListener();
        ClusterEventTypeReceiveListener listener1 = new ClusterEventTypeReceiveListener();
        ClusterEventTypeReceiveListener listener2 = new ClusterEventTypeReceiveListener();

        cache0.addListener(listener0);
        cache1.addListener(listener1);
        cache2.addListener(listener2);

        MagicKey key = new MagicKey(cache0);
        String expectedValue = key + "-clustered";
        cache0.put(key, key + "-clustered");

        int expectCount = 1;
        int eventsIndex = 0;

        assertEquals(expectCount, listener0.createdEvents.size());
        assertEquals(Event.Type.CACHE_ENTRY_CREATED, listener0.createdEvents.get(eventsIndex).getType());
        assertEquals(key, listener0.createdEvents.get(eventsIndex).getKey());
        assertEquals(expectedValue, listener0.createdEvents.get(eventsIndex).getValue());

        assertEquals(expectCount, listener1.createdEvents.size());
        assertEquals(Event.Type.CACHE_ENTRY_CREATED, listener1.createdEvents.get(eventsIndex).getType());
        assertEquals(key, listener1.createdEvents.get(eventsIndex).getKey());
        assertEquals(expectedValue, listener1.createdEvents.get(eventsIndex).getValue());

        assertEquals(expectCount, listener2.createdEvents.size());
        assertEquals(Event.Type.CACHE_ENTRY_CREATED, listener2.createdEvents.get(eventsIndex).getType());
        assertEquals(key, listener2.createdEvents.get(eventsIndex).getKey());
        assertEquals(expectedValue, listener2.createdEvents.get(eventsIndex).getValue());
    }

    @Test
    public void testReceiveModifiedEventType() {
        Cache<Object, String> cache0 = cache(0, CACHE_NAME);
        Cache<Object, String> cache1 = cache(1, CACHE_NAME);
        Cache<Object, String> cache2 = cache(2, CACHE_NAME);

        ClusterEventTypeReceiveListener listener0 = new ClusterEventTypeReceiveListener();
        ClusterEventTypeReceiveListener listener1 = new ClusterEventTypeReceiveListener();
        ClusterEventTypeReceiveListener listener2 = new ClusterEventTypeReceiveListener();

        cache0.addListener(listener0);
        cache1.addListener(listener1);
        cache2.addListener(listener2);

        MagicKey key = new MagicKey(cache0);
        cache0.put(key, key + "-clustered");

        String expectedValue = key + "-clustered-modified";
        cache0.put(key, key + "-clustered-modified");

        int expectCount = 1;
        int eventsIndex = 0;

        assertEquals(expectCount, listener0.modifiedEvents.size());
        assertEquals(Event.Type.CACHE_ENTRY_MODIFIED, listener0.modifiedEvents.get(eventsIndex).getType());
        assertEquals(key, listener0.modifiedEvents.get(eventsIndex).getKey());
        assertEquals(expectedValue, listener0.modifiedEvents.get(eventsIndex).getValue());

        assertEquals(expectCount, listener1.modifiedEvents.size());
        assertEquals(Event.Type.CACHE_ENTRY_MODIFIED, listener1.modifiedEvents.get(eventsIndex).getType());
        assertEquals(key, listener1.modifiedEvents.get(eventsIndex).getKey());
        assertEquals(expectedValue, listener1.modifiedEvents.get(eventsIndex).getValue());

        assertEquals(expectCount, listener2.modifiedEvents.size());
        assertEquals(Event.Type.CACHE_ENTRY_MODIFIED, listener2.modifiedEvents.get(eventsIndex).getType());
        assertEquals(key, listener2.modifiedEvents.get(eventsIndex).getKey());
        assertEquals(expectedValue, listener2.modifiedEvents.get(eventsIndex).getValue());
    }

    @Test
    public void testReceiveRemovedEventType() {
        Cache<Object, String> cache0 = cache(0, CACHE_NAME);
        Cache<Object, String> cache1 = cache(1, CACHE_NAME);
        Cache<Object, String> cache2 = cache(2, CACHE_NAME);

        ClusterEventTypeReceiveListener listener0 = new ClusterEventTypeReceiveListener();
        ClusterEventTypeReceiveListener listener1 = new ClusterEventTypeReceiveListener();
        ClusterEventTypeReceiveListener listener2 = new ClusterEventTypeReceiveListener();

        cache0.addListener(listener0);
        cache1.addListener(listener1);
        cache2.addListener(listener2);

        MagicKey key = new MagicKey(cache0);
        cache0.put(key, key + "-clustered");
        cache0.remove(key);

        int expectCount = 1;
        int eventsIndex = 0;

        assertEquals(expectCount, listener0.createdEvents.size());
        assertEquals(Event.Type.CACHE_ENTRY_REMOVED, listener0.removedEvents.get(eventsIndex).getType());
        assertEquals(key, listener0.removedEvents.get(eventsIndex).getKey());

        assertEquals(expectCount, listener1.removedEvents.size());
        assertEquals(Event.Type.CACHE_ENTRY_REMOVED, listener1.removedEvents.get(eventsIndex).getType());
        assertEquals(key, listener1.removedEvents.get(eventsIndex).getKey());

        assertEquals(expectCount, listener2.removedEvents.size());
        assertEquals(Event.Type.CACHE_ENTRY_REMOVED, listener2.removedEvents.get(eventsIndex).getType());
        assertEquals(key, listener2.removedEvents.get(eventsIndex).getKey());
    }

    @Test
    public void testReceiveExpiredEventType() {
        Cache<Object, String> cache0 = cache(0, CACHE_NAME);
        Cache<Object, String> cache1 = cache(1, CACHE_NAME);
        Cache<Object, String> cache2 = cache(2, CACHE_NAME);

        ClusterEventTypeReceiveListener listener0 = new ClusterEventTypeReceiveListener();
        ClusterEventTypeReceiveListener listener1 = new ClusterEventTypeReceiveListener();
        ClusterEventTypeReceiveListener listener2 = new ClusterEventTypeReceiveListener();

        cache0.addListener(listener0);
        cache1.addListener(listener1);
        cache2.addListener(listener2);

        MagicKey key = new MagicKey(cache0);
        String expectedValue = key + "-clustered-expiring";
        cache0.put(key, key + "-clustered-expiring", 1000, TimeUnit.MILLISECONDS);

        ts0.advance(1001);

        assertNull(cache0.get(key));

        int expectCount = 1;

        eventually(() -> listener0.expiredEvents.size() >= expectCount, 200000);
        eventually(() -> listener1.expiredEvents.size() >= expectCount, 200000);
        eventually(() -> listener2.expiredEvents.size() >= expectCount, 200000);

        int eventsIndex = 0;

        assertEquals(expectCount, listener0.expiredEvents.size());
        assertEquals(Event.Type.CACHE_ENTRY_EXPIRED, listener0.expiredEvents.get(eventsIndex).getType());
        assertEquals(key, listener0.expiredEvents.get(eventsIndex).getKey());
        assertEquals(expectedValue, listener0.expiredEvents.get(eventsIndex).getValue());

        assertEquals(expectCount, listener1.expiredEvents.size());
        assertEquals(Event.Type.CACHE_ENTRY_EXPIRED, listener1.expiredEvents.get(eventsIndex).getType());
        assertEquals(key, listener1.expiredEvents.get(eventsIndex).getKey());
        assertEquals(expectedValue, listener1.expiredEvents.get(eventsIndex).getValue());

        assertEquals(expectCount, listener2.expiredEvents.size());
        assertEquals(Event.Type.CACHE_ENTRY_EXPIRED, listener2.expiredEvents.get(eventsIndex).getType());
        assertEquals(key, listener2.expiredEvents.get(eventsIndex).getKey());
        assertEquals(expectedValue, listener2.expiredEvents.get(eventsIndex).getValue());
    }

    @Listener(clustered = true)
    class ClusterEventTypeReceiveListener {
        List<CacheEntryCreatedEvent> createdEvents = Collections.synchronizedList(new ArrayList<>());
        List<CacheEntryModifiedEvent> modifiedEvents = Collections.synchronizedList(new ArrayList<>());
        List<CacheEntryRemovedEvent> removedEvents = Collections.synchronizedList(new ArrayList<>());
        List<CacheEntryExpiredEvent> expiredEvents = Collections.synchronizedList(new ArrayList<>());

        @CacheEntryCreated
        public void onCacheCreatedEvent(CacheEntryCreatedEvent event) {
            log.debugf("Adding new cluster created event %s", event);
            createdEvents.add(event);
        }

        @CacheEntryModified
        public void onCacheModifiedEvent(CacheEntryModifiedEvent event) {
            log.debugf("Adding new cluster modified event %s", event);
            modifiedEvents.add(event);
        }

        @CacheEntryRemoved
        public void onCacheRemovedEvent(CacheEntryRemovedEvent event) {
            log.debugf("Adding new cluster removed event %s", event);
            removedEvents.add(event);
        }

        @CacheEntryExpired
        public void onCacheExpiredEvent(CacheEntryExpiredEvent event) {
            log.debugf("Adding new cluster expired event %s", event);
            expiredEvents.add(event);
        }
    }
}
