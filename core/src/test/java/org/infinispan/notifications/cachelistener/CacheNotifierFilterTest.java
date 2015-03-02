package org.infinispan.notifications.cachelistener;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.filter.CollectionKeyFilter;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryInvalidated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryPassivated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryVisitedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Simple test class that tests to make sure other events are properly handled for filters
 *
 * @author wburns
 * @since 4.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.CacheNotifierFilterTest")
public class CacheNotifierFilterTest extends MultipleCacheManagersTest {
   protected final String CACHE_NAME = getClass().getName();
   protected ConfigurationBuilder builderUsed;

   @Override
   protected void createCacheManagers() throws Throwable {
      builderUsed = new ConfigurationBuilder();
      builderUsed.clustering().cacheMode(CacheMode.REPL_SYNC);
      createClusteredCaches(3, CACHE_NAME, builderUsed);
   }

   private static class VisitedFilter implements CacheEventFilter<String, String> {

      @Override
      public boolean accept(String key, String oldValue, Metadata oldMetadata, String newValue, Metadata newMetadata, EventType eventType) {
         return eventType.getType() != Event.Type.CACHE_ENTRY_VISITED;
      }
   }

   @Listener
   private static class TestListener {
      private final List<CacheEntryVisitedEvent> visitedEvents = Collections.synchronizedList(
            new ArrayList<CacheEntryVisitedEvent>());
      private final List<TopologyChangedEvent> topologyEvents = Collections.synchronizedList(
            new ArrayList<TopologyChangedEvent>());
      @CacheEntryVisited
      public void entryVisited(CacheEntryVisitedEvent event) {
         visitedEvents.add(event);
      }

      @TopologyChanged
      public void topologyChanged(TopologyChangedEvent event) {
         topologyEvents.add(event);
      }
   }

   @Listener
   private static class AllCacheEntryListener {
      private final List<CacheEntryEvent> events = Collections.synchronizedList(
            new ArrayList<CacheEntryEvent>());

      @CacheEntryVisited
      @CacheEntryActivated
      @CacheEntryModified
      @CacheEntryRemoved
      @CacheEntryCreated
      @CacheEntryInvalidated
      @CacheEntryPassivated
      public void listenEvent(CacheEntryEvent event) {
         events.add(event);
      }
   }

   /**
    * Basic test to ensure that non modification events are properly filtered
    */
   @Test
   public void testCacheEntryVisitedEventFiltered() {
      String key = "key";
      String value = "value";
      Cache<String, String> cache0 = cache(0, CACHE_NAME);
      cache0.put(key, value);

      TestListener listener = new TestListener();
      cache0.addListener(listener, new CollectionKeyFilter<>(Collections.singletonList(key)));

      assertEquals(value, cache0.get(key));

      assertEquals(0, listener.visitedEvents.size());

      // Verify others work still as well
      String notKey = "not" + key;
      cache0.put(notKey, value);
      cache0.get("not" + key);

      assertEquals(2, listener.visitedEvents.size());
   }

   @Test
   public void testNonCacheEventsNotFiltered() {
      Cache<String, String> cache0 = cache(0, CACHE_NAME);

      TestListener listener = new TestListener();
      // This would block all cache events
      cache0.addListener(listener, new CollectionKeyFilter<>(Collections.emptyList(), true));

      addClusterEnabledCacheManager(builderUsed);

      waitForClusterToForm(CACHE_NAME);

      // Adding a node requires 3 topologies x2 for pre/post = 6
      assertEquals(6, listener.topologyEvents.size());
   }

   @Test
   public void testVisitationsBlocked() {
      String key = "key";
      String value = "value";
      Cache<String, String> cache0 = cache(0, CACHE_NAME);
      cache0.put(key, value);

      AllCacheEntryListener listener = new AllCacheEntryListener();
      cache0.addListener(listener, new VisitedFilter(), null);

      assertEquals(value, cache0.get(key));

      assertEquals(0, listener.events.size());

      // Verify others don't work as well
      String notKey = "not" + key;
      cache0.put(notKey, value);
      cache0.get("not" + key);

      // We should have 2 create events
      assertEquals(2, listener.events.size());
      assertEquals(Event.Type.CACHE_ENTRY_CREATED, listener.events.get(0).getType());
      assertEquals(Event.Type.CACHE_ENTRY_CREATED, listener.events.get(1).getType());
   }
}
