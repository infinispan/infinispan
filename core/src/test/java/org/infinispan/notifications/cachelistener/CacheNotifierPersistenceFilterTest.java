package org.infinispan.notifications.cachelistener;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.eviction.PassivationManager;
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
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

/**
 * Simple test class that tests to make sure other events are properly handled for filters
 *
 * @author wburns
 * @since 4.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.CacheNotifierFilterTest")
public class CacheNotifierPersistenceFilterTest extends MultipleCacheManagersTest {
   protected final String CACHE_NAME = getClass().getName();
   protected ConfigurationBuilder builderUsed;

   @Override
   protected void createCacheManagers() throws Throwable {
      builderUsed = new ConfigurationBuilder();
      builderUsed.clustering().cacheMode(CacheMode.REPL_SYNC);
      builderUsed.persistence().passivation(true).addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .storeName(CACHE_NAME);
      createClusteredCaches(3, CACHE_NAME, builderUsed);
   }

   private static class EventKeyFilter implements CacheEventFilter<String, String> {

      private final Event.Type type;
      private final Object key;

      public EventKeyFilter(Event.Type type, Object key) {
         this.type = type;
         this.key = key;
      }

      @Override
      public boolean accept(String key, String oldValue, Metadata oldMetadata, String newValue, Metadata newMetadata, EventType eventType) {
         boolean accept = type == eventType.getType() && this.key.equals(key);
         return accept;
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

   @Test
   public void testPassivationBlocked() {
      String key = "key";
      String value = "value";
      Cache<String, String> cache0 = cache(0, CACHE_NAME);

      AllCacheEntryListener listener = new AllCacheEntryListener();
      cache0.addListener(listener, new EventKeyFilter(Event.Type.CACHE_ENTRY_PASSIVATED, key), null);

      PassivationManager passivationManager = cache0.getAdvancedCache().getComponentRegistry().getComponent(
            PassivationManager.class);

      passivationManager.passivate(new ImmortalCacheEntry(key, value));

      assertEquals(2, listener.events.size());
      assertEquals(Event.Type.CACHE_ENTRY_PASSIVATED, listener.events.get(0).getType());
      assertEquals(key, listener.events.get(0).getKey());
      assertEquals(value, listener.events.get(0).getValue());
      assertEquals(Event.Type.CACHE_ENTRY_PASSIVATED, listener.events.get(0).getType());
      assertEquals(key, listener.events.get(1).getKey());
      assertNull(listener.events.get(1).getValue());

      passivationManager.passivate(new ImmortalCacheEntry("not" + key, value));

      // We shouldn't have received any additional events
      assertEquals(2, listener.events.size());
   }

   @Test
   public void testActivationBlocked() {
      String key = "key";
      String value = "value";
      Cache<String, String> cache0 = cache(0, CACHE_NAME);

      PassivationManager passivationManager = cache0.getAdvancedCache().getComponentRegistry().getComponent(
            PassivationManager.class);

      // Passivate 2 entries to resurrect
      passivationManager.passivate(new ImmortalCacheEntry(key, value));
      passivationManager.passivate(new ImmortalCacheEntry("not" + key, value));

      AllCacheEntryListener listener = new AllCacheEntryListener();
      cache0.addListener(listener, new EventKeyFilter(Event.Type.CACHE_ENTRY_ACTIVATED, key), null);

      assertEquals(value, cache0.get("not" + key));

      // We shouldn't have received any events
      assertEquals(0, listener.events.size());

      assertEquals(value, cache0.get(key));

      assertEquals(2, listener.events.size());
      assertEquals(Event.Type.CACHE_ENTRY_ACTIVATED, listener.events.get(0).getType());
      assertEquals(key, listener.events.get(0).getKey());
      assertEquals(value, listener.events.get(0).getValue());
      assertEquals(Event.Type.CACHE_ENTRY_ACTIVATED, listener.events.get(0).getType());
      assertEquals(key, listener.events.get(1).getKey());
      assertEquals(value, listener.events.get(1).getValue());
   }
}
