package org.infinispan.notifications.cachelistener;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;

import static org.testng.AssertJUnit.*;

/**
 * ISPN-3354
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "notifications.cachelistener.DuplicatedEventsTest")
public class DuplicatedEventsTest extends MultipleCacheManagersTest {

   public void testNonDuplicate() {
      final Cache<String, String> cacheA = cache(0);
      final Cache<String, String> cacheB = cache(1);
      final MyCacheListener listenerA = new MyCacheListener();
      cacheA.addListener(listenerA);

      final MyCacheListener listenerB = new MyCacheListener();
      cacheB.addListener(listenerB);

      cacheA.put("a", "a");

		/*
       * We expect 4 events on both nodes: pre-create, pre-modified, post-modified, post-create
		 */
      assertEquals(4, listenerA.events.size());
      assertEquals(4, listenerB.events.size());

      checkEvents(listenerA, "a");
      checkEvents(listenerB, "a");

		/*
       * So far so good, let's try another key, say "b"
		 */

      listenerA.events.clear();
      listenerB.events.clear();

      cacheA.put("b", "b");

		/*
       * We expect 4 events again
		 */
      assertEquals(4, listenerA.events.size());
      assertEquals(4, listenerB.events.size());

      checkEvents(listenerA, "b");
      checkEvents(listenerB, "b");

		/*
       * Let's try another one, say "a0"
		 */

      listenerA.events.clear();
      listenerB.events.clear();

      cacheA.put("a0", "a0");

		/*
       * We expect another 4 events, but on the local node (A in this case) we get 8
		 */
      assertEquals(4, listenerA.events.size());
      assertEquals(4, listenerB.events.size());

      checkEvents(listenerA, "a0");
      checkEvents(listenerB, "a0");
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      builder.clustering().hash().numSegments(60);
      createClusteredCaches(2, builder);
   }

   private void checkEvents(MyCacheListener listener, String expectedKey) {
      assertTrue(listener.events.get(0) instanceof CacheEntryCreatedEvent);
      assertEquals(expectedKey, listener.events.get(0).getKey());
      assertTrue(listener.events.get(0).isPre());

      assertTrue(listener.events.get(1) instanceof CacheEntryModifiedEvent);
      assertEquals(expectedKey, listener.events.get(1).getKey());
      assertTrue(listener.events.get(1).isPre());

      assertTrue(listener.events.get(2) instanceof CacheEntryModifiedEvent);
      assertEquals(expectedKey, listener.events.get(2).getKey());
      assertFalse(listener.events.get(2).isPre());

      assertTrue(listener.events.get(3) instanceof CacheEntryCreatedEvent);
      assertEquals(expectedKey, listener.events.get(3).getKey());
      assertFalse(listener.events.get(3).isPre());
   }

   @Listener
   public class MyCacheListener {

      private List<CacheEntryEvent<String, String>> events = new LinkedList<CacheEntryEvent<String, String>>();

      @CacheEntryCreated
      public void created(CacheEntryCreatedEvent<String, String> event) {
         events.add(event);
      }

      @CacheEntryModified
      public void modified(CacheEntryModifiedEvent<String, String> event) {
         events.add(event);
      }

      @CacheEntryRemoved
      public void removed(CacheEntryRemovedEvent<String, String> event) {
         events.add(event);
      }
   }

}
