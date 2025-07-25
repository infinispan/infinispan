package org.infinispan.it.endpoints;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withClientListener;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.client.hotrod.event.CustomEventLogListener.CustomEvent;
import org.infinispan.client.hotrod.event.CustomEventLogListener.DynamicConverterFactory;
import org.infinispan.client.hotrod.event.CustomEventLogListener.DynamicCustomEventLogListener;
import org.infinispan.client.hotrod.event.CustomEventLogListener.StaticConverterFactory;
import org.infinispan.client.hotrod.event.CustomEventLogListener.StaticCustomEventLogListener;
import org.infinispan.client.hotrod.event.EventLogListener;
import org.infinispan.client.hotrod.event.EventLogListener.DynamicCacheEventFilterFactory;
import org.infinispan.client.hotrod.event.EventLogListener.DynamicFilteredEventLogListener;
import org.infinispan.client.hotrod.event.EventLogListener.StaticCacheEventFilterFactory;
import org.infinispan.client.hotrod.event.EventLogListener.StaticFilteredEventLogListener;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.AbstractInfinispanTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test embedded caches and Hot Rod endpoints.
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
@Test(groups = "functional", testName = "it.endpoints.EmbeddedHotRodTest")
public class EmbeddedHotRodTest extends AbstractInfinispanTest {

   EndpointsCacheFactory<Integer, String> cacheFactory;

   @BeforeClass
   protected void setup() throws Exception {
      cacheFactory = new EndpointsCacheFactory.Builder<Integer, String>().withCacheMode(CacheMode.LOCAL)
            .withContextInitializer(EndpointITSCI.INSTANCE).build();
      HotRodServer hotrod = cacheFactory.getHotrodServer();
      hotrod.addCacheEventFilterFactory("static-filter-factory", new StaticCacheEventFilterFactory(2));
      hotrod.addCacheEventFilterFactory("dynamic-filter-factory", new DynamicCacheEventFilterFactory());
      hotrod.addCacheEventConverterFactory("static-converter-factory", new StaticConverterFactory());
      hotrod.addCacheEventConverterFactory("dynamic-converter-factory", new DynamicConverterFactory());
   }

   @AfterClass
   protected void teardown() {
      EndpointsCacheFactory.killCacheFactories(cacheFactory);
   }

   private Cache<Integer, String> getEmbeddedCache() {
      return cacheFactory.getEmbeddedCache().getAdvancedCache();
   }

   public void testEmbeddedPutHotRodGet() {
      final Integer key = 1;
      Cache<Integer, String> embedded = getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertNull(embedded.put(key, "v1"));
      assertEquals("v1", remote.get(key));
      assertEquals("v1", embedded.put(key, "v2"));
      assertEquals("v2", remote.get(key));
      assertEquals("v2", embedded.remove(key));
   }

   public void testHotRodPutEmbeddedGet() {
      final Integer key = 2;
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      Cache<Integer, String> embedded = getEmbeddedCache();
      assertNull(remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "v1"));
      assertEquals("v1", embedded.get(key));
      assertNull(remote.put(key, "v2"));
      assertEquals("v2", remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "v3"));
      assertEquals("v3", embedded.get(key));
      assertEquals("v3", remote.withFlags(Flag.FORCE_RETURN_VALUE).remove(key));
   }

   public void testEmbeddedPutIfAbsentHotRodGet() {
      final Integer key = 3;
      Cache<Integer, String> embedded = getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertNull(embedded.putIfAbsent(key, "v1"));
      assertEquals("v1", remote.get(key));
      assertEquals("v1", embedded.putIfAbsent(key, "v2"));
      assertEquals("v1", remote.get(key));
      assertEquals("v1", embedded.remove(key));
   }

   public void testHotRodPutIfAbsentEmbeddedGet() {
      final Integer key = 4;
      Cache<Integer, String> embedded = getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertNull(remote.withFlags(Flag.FORCE_RETURN_VALUE).putIfAbsent(key, "v1"));
      assertEquals("v1", embedded.get(key));
      assertNull(remote.putIfAbsent(key, "v2"));
      assertEquals("v1", remote.withFlags(Flag.FORCE_RETURN_VALUE).putIfAbsent(key, "v2"));
      assertEquals("v1", embedded.get(key));
      assertEquals("v1", remote.withFlags(Flag.FORCE_RETURN_VALUE).remove(key));
   }

   public void testEmbeddedReplaceHotRodGet() {
      final Integer key = 5;
      Cache<Integer, String> embedded = getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertNull(embedded.replace(key, "v1"));
      assertNull(embedded.put(key, "v1"));
      assertEquals("v1", embedded.replace(key, "v2"));
      assertEquals("v2", remote.get(key));
      assertEquals("v2", embedded.remove(key));
   }

   public void testHotRodReplaceEmbeddedGet() {
      final Integer key = 6;
      Cache<Integer, String> embedded = getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertNull(remote.withFlags(Flag.FORCE_RETURN_VALUE).replace(key, "v1"));
      assertNull(remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "v1"));
      assertEquals("v1", remote.withFlags(Flag.FORCE_RETURN_VALUE).replace(key, "v2"));
      assertEquals("v2", embedded.get(key));
   }

   public void testEmbeddedReplaceConditionalHotRodGet() {
      final Integer key = 7;
      Cache<Integer, String> embedded = getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertNull(embedded.put(key, "v1"));
      assertTrue(embedded.replace(key, "v1", "v2"));
      assertEquals("v2", remote.get(key));
      assertEquals("v2", embedded.remove(key));
   }

   public void testHotRodReplaceConditionalEmbeddedGet() {
      final Integer key = 8;
      Cache<Integer, String> embedded = getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertNull(remote.put(key, "v1"));
      VersionedValue<String> versioned = remote.getWithMetadata(key);
      assertEquals("v1", versioned.getValue());
      assertTrue(0 != versioned.getVersion());
      assertFalse(remote.replaceWithVersion(key, "v2", Long.MAX_VALUE));
      assertTrue(remote.replaceWithVersion(key, "v2", versioned.getVersion()));
      assertEquals("v2", embedded.get(key));
      assertEquals("v2", remote.withFlags(Flag.FORCE_RETURN_VALUE).remove(key));
   }

   public void testEmbeddedRemoveHotRodGet() {
      final Integer key = 9;
      Cache<Integer, String> embedded = getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertNull(embedded.put(key, "v1"));
      assertEquals("v1", embedded.remove(key));
      assertNull(remote.get(key));
   }

   public void testHotRodRemoveEmbeddedGet() {
      final Integer key = 10;
      Cache<Integer, String> embedded = getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertNull(remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "v1"));
      assertEquals("v1", remote.withFlags(Flag.FORCE_RETURN_VALUE).remove(key));
      assertNull(embedded.get(key));
   }

   public void testEmbeddedRemoveConditionalHotRodGet() {
      final Integer key = 11;
      Cache<Integer, String> embedded = getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertNull(embedded.put(key, "v1"));
      assertFalse(embedded.remove(key, "vX"));
      assertTrue(embedded.remove(key, "v1"));
      assertNull(remote.get(key));
   }

   public void testHotRodRemoveConditionalEmbeddedGet() {
      final Integer key = 12;
      Cache<Integer, String> embedded = getEmbeddedCache();
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      assertNull(remote.withFlags(Flag.FORCE_RETURN_VALUE).put(key, "v1"));
      VersionedValue<String> versioned = remote.getWithMetadata(key);
      assertFalse(remote.withFlags(Flag.FORCE_RETURN_VALUE).removeWithVersion(key, Long.MAX_VALUE));
      assertTrue(remote.withFlags(Flag.FORCE_RETURN_VALUE).removeWithVersion(key, versioned.getVersion()));
      assertNull(embedded.get(key));
   }

   public void testEventReceiveBasic() {
      EventLogListener<Integer> l = new EventLogListener<>(cacheFactory.getHotRodCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.remove(1);
         l.expectOnlyRemovedEvent(1);
         remote.put(1, "one");
         assertEquals("one", getEmbeddedCache().get(1));
         l.expectOnlyCreatedEvent(1);
         remote.put(1, "new-one");
         assertEquals("new-one", getEmbeddedCache().get(1));
         l.expectOnlyModifiedEvent(1);
         remote.remove(1);
         l.expectOnlyRemovedEvent(1);
      });
   }

   public void testEventReceiveConditional() {
      EventLogListener<Integer> l = new EventLogListener<>(cacheFactory.getHotRodCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         // Put if absent
         remote.putIfAbsent(1, "one");
         l.expectOnlyCreatedEvent(1);
         remote.putIfAbsent(1, "again");
         l.expectNoEvents();
         // Replace
         remote.replace(1, "newone");
         l.expectOnlyModifiedEvent(1);
         // Replace with version
         remote.replaceWithVersion(1, "one", 0);
         l.expectNoEvents();
         VersionedValue<?> versioned = remote.getWithMetadata(1);
         remote.replaceWithVersion(1, "one", versioned.getVersion());
         l.expectOnlyModifiedEvent(1);
         // Remove with version
         remote.removeWithVersion(1, 0);
         l.expectNoEvents();
         versioned = remote.getWithMetadata(1);
         remote.removeWithVersion(1, versioned.getVersion());
         l.expectOnlyRemovedEvent(1);
      });
   }

   public void testEventReplayAfterAddingListener() {
      EventLogWithStateListener<Integer> l = new EventLogWithStateListener<>(cacheFactory.getHotRodCache());
      createRemove();
      l.expectNoEvents();
      withClientListener(l, remote -> {
         l.expectUnorderedEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED, 1, 2);
         remote.remove(1);
         l.expectOnlyRemovedEvent(1);
         remote.remove(2);
         l.expectOnlyRemovedEvent(2);
      });
   }

   public void testEventNoReplayAfterAddingListener() {
      createRemove();
      EventLogListener<Integer> l = new EventLogListener<>(cacheFactory.getHotRodCache());
      l.expectNoEvents();
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.remove(1);
         l.expectOnlyRemovedEvent(1);
         remote.remove(2);
         l.expectOnlyRemovedEvent(2);
      });
   }

   private void createRemove() {
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      Cache<Integer, String> embedded = getEmbeddedCache();
      remote.put(1, "one");
      assertEquals("one", embedded.get(1));
      remote.put(2, "two");
      assertEquals("two", embedded.get(2));
      remote.put(3, "three");
      assertEquals("three", embedded.get(3));
      remote.remove(3);
      assertNull(embedded.get(3));
   }

   public void testEventFilteringStatic() {
      StaticFilteredEventLogListener<Integer> l =
            new StaticFilteredEventLogListener<>(cacheFactory.getHotRodCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.put(1, "one");
         Cache<Integer, String> embedded = getEmbeddedCache();
         assertEquals("one", embedded.get(1));
         l.expectNoEvents();
         remote.put(2, "two");
         assertEquals("two", embedded.get(2));
         l.expectOnlyCreatedEvent(2);
         remote.remove(1);
         assertNull(embedded.get(1));
         l.expectNoEvents();
         remote.remove(2);
         assertNull(embedded.get(2));
         l.expectOnlyRemovedEvent(2);
      });
   }

   public void testEventFilteringDynamic() {
      RemoteCache<Integer, String> remote = cacheFactory.getHotRodCache();
      DynamicFilteredEventLogListener<Integer> eventListener = new DynamicFilteredEventLogListener<>(remote);
      Cache<Integer, String> embedded = getEmbeddedCache();
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
         eventListener.expectOnlyCreatedEvent(3);
         remote.replace(1, "new-one");
         assertEquals("new-one", embedded.get(1));
         eventListener.expectNoEvents();
         remote.replace(2, "new-two");
         assertEquals("new-two", embedded.get(2));
         eventListener.expectNoEvents();
         remote.replace(3, "new-three");
         assertEquals("new-three", embedded.get(3));
         eventListener.expectOnlyModifiedEvent(3);
         remote.remove(1);
         assertNull(embedded.get(1));
         eventListener.expectNoEvents();
         remote.remove(2);
         assertNull(embedded.get(2));
         eventListener.expectNoEvents();
         remote.remove(3);
         assertNull(embedded.get(3));
         eventListener.expectOnlyRemovedEvent(3);
      } finally {
         remote.removeClientListener(eventListener);
      }
   }

   public void testCustomEvents() {
      StaticCustomEventLogListener<Integer> l =
            new StaticCustomEventLogListener<>(cacheFactory.getHotRodCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.put(1, "one");
         Cache<Integer, String> embedded = getEmbeddedCache();
         assertEquals("one", embedded.get(1));
         l.expectCreatedEvent(new CustomEvent(1, "one", 0));
         remote.put(1, "new-one");
         assertEquals("new-one", embedded.get(1));
         l.expectModifiedEvent(new CustomEvent(1, "new-one", 0));
         remote.remove(1);
         assertNull(embedded.get(1));
         l.expectRemovedEvent(new CustomEvent(1, null, 0));
      });
   }

   public void testCustomEventsDynamic() {
      DynamicCustomEventLogListener<Integer> l = new DynamicCustomEventLogListener<>(cacheFactory.getHotRodCache());
      withClientListener(l, null, new Object[]{2}, remote -> {
         l.expectNoEvents();
         remote.put(1, "one");
         Cache<Integer, String> embedded = getEmbeddedCache();
         assertEquals("one", embedded.get(1));
         l.expectCreatedEvent(new CustomEvent(1, "one", 0));
         remote.put(2, "two");
         assertEquals("two", embedded.get(2));
         l.expectCreatedEvent(new CustomEvent(2, null, 0));
         remote.remove(1);
         assertNull(embedded.get(1));
         l.expectRemovedEvent(new CustomEvent(1, null, 0));
         remote.remove(2);
         assertNull(embedded.get(2));
         l.expectRemovedEvent(new CustomEvent(2, null, 0));
      });
   }

   @ClientListener(includeCurrentState = true)
   public static class EventLogWithStateListener<K> extends EventLogListener<K> {
      public EventLogWithStateListener(RemoteCache<K, ?> r) {
         super(r);
      }
   }

}
