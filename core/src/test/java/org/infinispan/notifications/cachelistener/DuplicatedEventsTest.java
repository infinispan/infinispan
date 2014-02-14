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
       * We expect 2 events on both nodes: pre-create, post-create
		 */
      assertEquals(2, listenerA.events.size());
      assertEquals(2, listenerB.events.size());

      checkEvents(listenerA, "a");
      checkEvents(listenerB, "a");

		/*
       * So far so good, let's try another key, say "b"
		 */

      listenerA.events.clear();
      listenerB.events.clear();

      cacheA.put("b", "b");

		/*
       * We expect 2 events again
		 */
      assertEquals(2, listenerA.events.size());
      assertEquals(2, listenerB.events.size());

      checkEvents(listenerA, "b");
      checkEvents(listenerB, "b");

		/*
       * Let's try another one, say "a0"
		 */

      listenerA.events.clear();
      listenerB.events.clear();

      cacheA.put("a0", "a0");

		/*
       * We expect another 2 events
		 */
      assertEquals(2, listenerA.events.size());
      assertEquals(2, listenerB.events.size());

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

      assertTrue(listener.events.get(1) instanceof CacheEntryCreatedEvent);
      assertEquals(expectedKey, listener.events.get(1).getKey());
      assertFalse(listener.events.get(1).isPre());
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
