package org.infinispan.it.compatibility;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.event.CustomEventLogListener.DynamicConverterFactory;
import org.infinispan.client.hotrod.event.CustomEventLogListener.DynamicCustomEventLogListener;
import org.infinispan.client.hotrod.event.CustomEventLogListener.StaticConverterFactory;
import org.infinispan.client.hotrod.event.CustomEventLogListener.StaticCustomEventLogListener;
import org.infinispan.client.hotrod.event.EventLogListener;
import org.infinispan.client.hotrod.event.EventLogListener.DynamicFilteredEventLogListener;
import org.infinispan.client.hotrod.event.EventLogListener.DynamicCacheEventFilterFactory;
import org.infinispan.client.hotrod.event.EventLogListener.StaticFilteredEventLogListener;
import org.infinispan.client.hotrod.event.EventLogListener.StaticCacheEventFilterFactory;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;

/**
 * Test compatibility between embedded caches and Hot Rod endpoints.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@Test(groups = "functional", testName = "it.compatibility.CompatibilityTest")
public class EmbeddedHotRodTest extends AbstractInfinispanTest {

   CompatibilityCacheFactory<Integer, String> cacheFactory;

   @BeforeClass
   protected void setup() throws Exception {
      cacheFactory = new CompatibilityCacheFactory<Integer, String>(CacheMode.LOCAL).setup();
      HotRodServer hotrod = cacheFactory.getHotrodServer();
      hotrod.addCacheEventFilterFactory("static-filter-factory", new StaticCacheEventFilterFactory());
      hotrod.addCacheEventFilterFactory("dynamic-filter-factory", new DynamicCacheEventFilterFactory());
      hotrod.addCacheEventConverterFactory("static-converter-factory", new StaticConverterFactory());
      hotrod.addCacheEventConverterFactory("dynamic-converter-factory", new DynamicConverterFactory());
   }

   @AfterClass
   protected void teardown() {
      CompatibilityCacheFactory.killCacheFactories(cacheFactory);
   }

   public void testEmbeddedPutHotRodGet() {
      final Integer key = 1;
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertEquals(null, embedded.put(key, "v1"));
      assertEquals("v1", remote.get(key));
      assertEquals("v1", embedded.put(key, "v2"));
      assertEquals("v2", remote.get(key));
      assertEquals("v2", embedded.remove(key));
   }

   public void testHotRodPutEmbeddedGet() {
      final Integer key = 2;
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      assertEquals(null, remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "v1"));
      assertEquals("v1", embedded.get(key));
      assertEquals(null, remote.put(key, "v2"));
      assertEquals("v2", remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "v3"));
      assertEquals("v3", embedded.get(key));
      assertEquals("v3", remote.withFlags(Flag.FORCE_RETURN_VALUE).remove(key));
   }

   public void testEmbeddedPutIfAbsentHotRodGet() {
      final Integer key = 3;
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertEquals(null, embedded.putIfAbsent(key, "v1"));
      assertEquals("v1", remote.get(key));
      assertEquals("v1", embedded.putIfAbsent(key, "v2"));
      assertEquals("v1", remote.get(key));
      assertEquals("v1", embedded.remove(key));
   }

   public void testHotRodPutIfAbsentEmbeddedGet() {
      final Integer key = 4;
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertEquals(null, remote.withFlags(Flag.FORCE_RETURN_VALUE).putIfAbsent(key, "v1"));
      assertEquals("v1", embedded.get(key));
      assertEquals(null, remote.putIfAbsent(key, "v2"));
      assertEquals("v1", remote.withFlags(Flag.FORCE_RETURN_VALUE).putIfAbsent(key, "v2"));
      assertEquals("v1", embedded.get(key));
      assertEquals("v1", remote.withFlags(Flag.FORCE_RETURN_VALUE).remove(key));
   }

   public void testEmbeddedReplaceHotRodGet() {
      final Integer key = 5;
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertEquals(null, embedded.replace(key, "v1"));
      assertEquals(null, embedded.put(key, "v1"));
      assertEquals("v1", embedded.replace(key, "v2"));
      assertEquals("v2", remote.get(key));
      assertEquals("v2", embedded.remove(key));
   }

   public void testHotRodReplaceEmbeddedGet() {
      final Integer key = 6;
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertEquals(null, remote.withFlags(Flag.FORCE_RETURN_VALUE).replace(key, "v1"));
      assertEquals(null, remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "v1"));
      assertEquals("v1", remote.withFlags(Flag.FORCE_RETURN_VALUE).replace(key, "v2"));
      assertEquals("v2", embedded.get(key));
   }

   public void testEmbeddedReplaceConditionalHotRodGet() {
      final Integer key = 7;
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertEquals(null, embedded.put(key, "v1"));
      assertTrue(embedded.replace(key, "v1", "v2"));
      assertEquals("v2", remote.get(key));
      assertEquals("v2", embedded.remove(key));
   }

   public void testHotRodReplaceConditionalEmbeddedGet() {
      final Integer key = 8;
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertEquals(null, remote.put(key, "v1"));
      VersionedValue<String> versioned = remote.getVersioned(key);
      assertEquals("v1", versioned.getValue());
      assertTrue(0 != versioned.getVersion());
      assertFalse(remote.replaceWithVersion(key, "v2", Long.MAX_VALUE));
      assertTrue(remote.replaceWithVersion(key, "v2", versioned.getVersion()));
      assertEquals("v2", embedded.get(key));
      assertEquals("v2", remote.withFlags(Flag.FORCE_RETURN_VALUE).remove(key));
   }

   public void testEmbeddedRemoveHotRodGet() {
      final Integer key = 9;
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertEquals(null, embedded.put(key, "v1"));
      assertEquals("v1", embedded.remove(key));
      assertEquals(null, remote.get(key));
   }

   public void testHotRodRemoveEmbeddedGet() {
      final Integer key = 10;
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertEquals(null, remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "v1"));
      assertEquals("v1", remote.withFlags(Flag.FORCE_RETURN_VALUE).remove(key));
      assertEquals(null, embedded.get(key));
   }

   public void testEmbeddedRemoveConditionalHotRodGet() {
      final Integer key = 11;
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertEquals(null, embedded.put(key, "v1"));
      assertFalse(embedded.remove(key, "vX"));
      assertTrue(embedded.remove(key, "v1"));
      assertEquals(null, remote.get(key));
   }

   public void testHotRodRemoveConditionalEmbeddedGet() {
      final Integer key = 12;
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertEquals(null, remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "v1"));
      VersionedValue<String> versioned = remote.getVersioned(key);
      assertFalse(remote.withFlags(Flag.FORCE_RETURN_VALUE).removeWithVersion(key, Long.MAX_VALUE));
      assertTrue(remote.withFlags(Flag.FORCE_RETURN_VALUE).removeWithVersion(key, versioned.getVersion()));
      assertEquals(null, embedded.get(key));
   }

   public void testEventReceiveBasic() {
      EventLogListener<Integer> eventListener = new EventLogListener<>(true);
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      remote.addClientListener(eventListener);
      try {
         eventListener.expectNoEvents();
         remote.remove(1);
         eventListener.expectNoEvents();
         remote.put(1, "one");
         assertEquals("one", embedded.get(1));
         eventListener.expectOnlyCreatedEvent(1, embedded);
         remote.put(1, "new-one");
         assertEquals("new-one", embedded.get(1));
         eventListener.expectOnlyModifiedEvent(1, embedded);
         remote.remove(1);
         eventListener.expectOnlyRemovedEvent(1, embedded);
      } finally {
         remote.removeClientListener(eventListener);
      }
   }

   public void testEventReceiveConditional() {
      EventLogListener<Integer> eventListener = new EventLogListener<>(true);
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      remote.addClientListener(eventListener);
      try {
         eventListener.expectNoEvents();
         // Put if absent
         remote.putIfAbsent(1, "one");
         eventListener.expectOnlyCreatedEvent(1, embedded);
         remote.putIfAbsent(1, "again");
         eventListener.expectNoEvents();
         // Replace
         remote.replace(1, "newone");
         eventListener.expectOnlyModifiedEvent(1, embedded);
         // Replace with version
         remote.replaceWithVersion(1, "one", 0);
         eventListener.expectNoEvents();
         VersionedValue<String> versioned = remote.getVersioned(1);
         remote.replaceWithVersion(1, "one", versioned.getVersion());
         eventListener.expectOnlyModifiedEvent(1, embedded);
         // Remove with version
         remote.removeWithVersion(1, 0);
         eventListener.expectNoEvents();
         versioned = remote.getVersioned(1);
         remote.removeWithVersion(1, versioned.getVersion());
         eventListener.expectOnlyRemovedEvent(1, embedded);
      } finally {
         remote.removeClientListener(eventListener);
      }
   }

   public void testEventReplayAfterAddingListener() {
      EventLogWithStateListener<Integer> eventListener = new EventLogWithStateListener<>(true);
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      remote.put(1, "one");
      assertEquals("one", embedded.get(1));
      remote.put(2, "two");
      assertEquals("two", embedded.get(2));
      remote.put(3, "three");
      assertEquals("three", embedded.get(3));
      remote.remove(3);
      assertNull(embedded.get(3));
      eventListener.expectNoEvents();
      remote.addClientListener(eventListener);
      try {
         eventListener.expectUnorderedEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED, 1, 2);
         remote.remove(1);
         eventListener.expectOnlyRemovedEvent(1, embedded);
         remote.remove(2);
         eventListener.expectOnlyRemovedEvent(2, embedded);
      } finally {
         remote.removeClientListener(eventListener);
      }
   }

   public void testEventNoReplayAfterAddingListener() {
      EventLogListener<Integer> eventListener = new EventLogListener<>(true);
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      remote.put(1, "one");
      assertEquals("one", embedded.get(1));
      remote.put(2, "two");
      assertEquals("two", embedded.get(2));
      remote.put(3, "three");
      assertEquals("three", embedded.get(3));
      remote.remove(3);
      assertNull(embedded.get(3));
      eventListener.expectNoEvents();
      remote.addClientListener(eventListener);
      try {
         eventListener.expectNoEvents();
         remote.remove(1);
         eventListener.expectOnlyRemovedEvent(1, embedded);
         remote.remove(2);
         eventListener.expectOnlyRemovedEvent(2, embedded);
      } finally {
         remote.removeClientListener(eventListener);
      }
   }

   public void testEventFilteringStatic() {
      StaticFilteredEventLogListener<Integer> eventListener = new StaticFilteredEventLogListener<>(true);
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      remote.addClientListener(eventListener);
      try {
         eventListener.expectNoEvents();
         remote.put(1, "one");
         assertEquals("one", embedded.get(1));
         eventListener.expectNoEvents();
         remote.put(2, "two");
         assertEquals("two", embedded.get(2));
         eventListener.expectOnlyCreatedEvent(2, embedded);
         remote.remove(1);
         assertNull(embedded.get(1));
         eventListener.expectNoEvents();
         remote.remove(2);
         assertNull(embedded.get(2));
         eventListener.expectOnlyRemovedEvent(2, embedded);
      } finally {
         remote.removeClientListener(eventListener);
      }
   }

   public void testEventFilteringDynamic() {
      DynamicFilteredEventLogListener<Integer> eventListener = new DynamicFilteredEventLogListener<>(true);
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      remote.addClientListener(eventListener, new Object[]{3}, null);
      try {
         eventListener.expectNoEvents();
         remote.put(1, "one");
         assertEquals("one", embedded.get(1));
         eventListener.expectNoEvents();
         remote.put(2, "two");
         assertEquals("two", embedded.get(2));
         eventListener.expectNoEvents();
         remote.put(3, "three");
         assertEquals("three", embedded.get(3));
         eventListener.expectOnlyCreatedEvent(3, embedded);
         remote.replace(1, "new-one");
         assertEquals("new-one", embedded.get(1));
         eventListener.expectNoEvents();
         remote.replace(2, "new-two");
         assertEquals("new-two", embedded.get(2));
         eventListener.expectNoEvents();
         remote.replace(3, "new-three");
         assertEquals("new-three", embedded.get(3));
         eventListener.expectOnlyModifiedEvent(3, embedded);
         remote.remove(1);
         assertNull(embedded.get(1));
         eventListener.expectNoEvents();
         remote.remove(2);
         assertNull(embedded.get(2));
         eventListener.expectNoEvents();
         remote.remove(3);
         assertNull(embedded.get(3));
         eventListener.expectOnlyRemovedEvent(3, embedded);
      } finally {
         remote.removeClientListener(eventListener);
      }
   }

   public void testCustomEvents() {
      StaticCustomEventLogListener eventListener = new StaticCustomEventLogListener();
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      remote.addClientListener(eventListener);
      try {
         eventListener.expectNoEvents();
         remote.put(1, "one");
         assertEquals("one", embedded.get(1));
         eventListener.expectOnlyCreatedCustomEvent(1, "one");
         remote.put(1, "new-one");
         assertEquals("new-one", embedded.get(1));
         eventListener.expectOnlyModifiedCustomEvent(1, "new-one");
         remote.remove(1);
         assertNull(embedded.get(1));
         eventListener.expectOnlyRemovedCustomEvent(1, null);
      } finally {
         remote.removeClientListener(eventListener);
      }
   }

   public void testCustomEventsDynamic() {
      DynamicCustomEventLogListener eventListener = new DynamicCustomEventLogListener();
      Cache<Integer, String> embedded = cacheFactory.getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      remote.addClientListener(eventListener, null, new Object[]{2});
      try {
         eventListener.expectNoEvents();
         remote.put(1, "one");
         assertEquals("one", embedded.get(1));
         eventListener.expectOnlyCreatedCustomEvent(1, "one");
         remote.put(2, "two");
         assertEquals("two", embedded.get(2));
         eventListener.expectOnlyCreatedCustomEvent(2, null);
         remote.remove(1);
         assertNull(embedded.get(1));
         eventListener.expectOnlyRemovedCustomEvent(1, null);
         remote.remove(2);
         assertNull(embedded.get(2));
         eventListener.expectOnlyRemovedCustomEvent(2, null);
      } finally {
         remote.removeClientListener(eventListener);
      }
   }

   @ClientListener(includeCurrentState = true)
   public static class EventLogWithStateListener<K> extends EventLogListener<K> {
      public EventLogWithStateListener(boolean compatibility) {
         super(compatibility);
      }
   }

}
