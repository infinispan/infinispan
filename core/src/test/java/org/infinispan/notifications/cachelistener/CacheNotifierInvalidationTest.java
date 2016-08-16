package org.infinispan.notifications.cachelistener;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryInvalidated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryPassivated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.Event.Type;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * Simple test class that tests to make sure invalidation events are raised on remote 
 * nodes
 *
 * @author wburns
 * @since 4.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.CacheNotifierInvalidationTest")
public class CacheNotifierInvalidationTest extends MultipleCacheManagersTest {
   protected final String CACHE_NAME = getClass().getName();
   protected ConfigurationBuilder builderUsed;

   @Override
   protected void createCacheManagers() throws Throwable {
      builderUsed = new ConfigurationBuilder();
      builderUsed.clustering().cacheMode(CacheMode.INVALIDATION_SYNC);
      createClusteredCaches(3, CACHE_NAME, builderUsed);
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
    * Basic test to ensure that a remote node's is notified of invalidation
    */
   @Test
   public void testRemoteNodeValueInvalidated() {
      String key = "key";
      String value = "value";
      Cache<String, String> cache0 = cache(0, CACHE_NAME);
      cache0.put(key, value);

      AllCacheEntryListener listener = new AllCacheEntryListener();
      cache0.addListener(listener);

      String value2 = "value2";
      // Now update the key which will invalidate cache0's key
      cache(1, CACHE_NAME).put(key, value2);

      assertEquals(2, listener.events.size());

      CacheEntryEvent event = listener.events.get(0);
      assertEquals(Type.CACHE_ENTRY_INVALIDATED, event.getType());
      assertEquals(key, event.getKey());
      assertEquals(value, event.getValue());
      assertTrue(event.isPre());
      assertFalse(event.isOriginLocal());
      
      event = listener.events.get(1);
      assertEquals(Type.CACHE_ENTRY_INVALIDATED, event.getType());
      assertEquals(key, event.getKey());
      assertEquals(value, event.getValue());
      assertFalse(event.isPre());
      assertFalse(event.isOriginLocal());
   }
}
