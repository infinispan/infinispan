package org.infinispan.client.hotrod.event;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withClientListener;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.EventLogListener.DynamicCacheEventFilterFactory;
import org.infinispan.client.hotrod.event.EventLogListener.DynamicFilteredEventLogListener;
import org.infinispan.client.hotrod.event.EventLogListener.DynamicFilteredEventLogWithStateListener;
import org.infinispan.client.hotrod.event.EventLogListener.RawStaticCacheEventFilterFactory;
import org.infinispan.client.hotrod.event.EventLogListener.RawStaticFilteredEventLogListener;
import org.infinispan.client.hotrod.event.EventLogListener.StaticCacheEventFilterFactory;
import org.infinispan.client.hotrod.event.EventLogListener.StaticFilteredEventLogListener;
import org.infinispan.client.hotrod.event.EventLogListener.StaticFilteredEventLogWithStateListener;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.event.ClientFilterEventsTest")
public class ClientFilterEventsTest extends SingleHotRodServerTest {

   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      HotRodServer server = HotRodClientTestingUtil.startHotRodServer(cacheManager, builder);
      server.addCacheEventFilterFactory("static-filter-factory", new StaticCacheEventFilterFactory(2));
      server.addCacheEventFilterFactory("dynamic-filter-factory", new DynamicCacheEventFilterFactory());
      server.addCacheEventFilterFactory("raw-static-filter-factory", new RawStaticCacheEventFilterFactory());
      return server;
   }

   public void testFilteredEvents() {
      final StaticFilteredEventLogListener<Integer> l = new StaticFilteredEventLogListener<>(remoteCacheManager.getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.put(1, "one");
         l.expectNoEvents();
         remote.put(2, "two");
         l.expectOnlyCreatedEvent(2);
         remote.remove(1);
         l.expectNoEvents();
         remote.remove(2);
         l.expectOnlyRemovedEvent(2);
      });
   }

   public void testParameterBasedFiltering() {
      final DynamicFilteredEventLogListener<Integer> l = new DynamicFilteredEventLogListener<>(remoteCacheManager.getCache());
      withClientListener(l, new Object[]{3}, null, remote -> {
         l.expectNoEvents();
         remote.put(1, "one");
         l.expectNoEvents();
         remote.put(2, "two");
         l.expectNoEvents();
         remote.put(3, "three");
         l.expectOnlyCreatedEvent(3);
      });
   }

   public void testFilteredEventsReplay() {
      RemoteCache<Integer, String> cache = remoteCacheManager.getCache();
      final StaticFilteredEventLogWithStateListener<Integer> staticEventListener =
            new StaticFilteredEventLogWithStateListener<>(cache);
      cache.put(1, "one");
      cache.put(2, "two");
      withClientListener(staticEventListener, remote -> {
         staticEventListener.expectOnlyCreatedEvent(2);
         remote.remove(1);
         remote.remove(2);
         staticEventListener.expectOnlyRemovedEvent(2);
      });
      final DynamicFilteredEventLogWithStateListener<Integer> dynamicEventListener =
            new DynamicFilteredEventLogWithStateListener<>(cache);
      cache.put(1, "one");
      cache.put(2, "two");
      cache.put(3, "three");
      withClientListener(dynamicEventListener, new Object[]{3}, null, remote -> {
         dynamicEventListener.expectOnlyCreatedEvent(3);
         remote.remove(1);
         remote.remove(2);
         remote.remove(3);
         dynamicEventListener.expectOnlyRemovedEvent(3);
      });
   }

   public void testFilteredNoEventsReplay() {
      RemoteCache<Integer, String> cache = remoteCacheManager.getCache();
      final StaticFilteredEventLogListener<Integer> staticEventListener =
            new StaticFilteredEventLogListener<>(cache);
      cache.put(1, "one");
      cache.put(2, "two");
      withClientListener(staticEventListener, remote -> {
         staticEventListener.expectNoEvents();
         remote.remove(1);
         remote.remove(2);
         staticEventListener.expectOnlyRemovedEvent(2);
      });
      final DynamicFilteredEventLogListener<Integer> dynamicEventListener =
            new DynamicFilteredEventLogListener<>(cache);
      cache.put(1, "one");
      cache.put(2, "two");
      cache.put(3, "three");
      withClientListener(dynamicEventListener, new Object[]{3}, null, remote -> {
         staticEventListener.expectNoEvents();
         remote.remove(1);
         remote.remove(2);
         remote.remove(3);
         dynamicEventListener.expectOnlyRemovedEvent(3);
      });
   }

   /**
    * Test that the HotRod server returns an error when a ClientListener is
    * registered with a non-existing 'filterFactoryName'.
    */
   @Test(expectedExceptions = HotRodClientException.class)
   public void testNonExistingConverterFactoryCustomEvents() {
      NonExistingFilterFactoryListener l = new NonExistingFilterFactoryListener<>(remoteCacheManager.getCache());
      withClientListener(l, remote -> {});
   }

   public void testRawFilteredEvents() {
      final RawStaticFilteredEventLogListener<Integer> l =
            new RawStaticFilteredEventLogListener<>(remoteCacheManager.getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.put(1, "one");
         l.expectNoEvents();
         remote.put(2, "two");
         l.expectOnlyCreatedEvent(2);
         remote.remove(1);
         l.expectNoEvents();
         remote.remove(2);
         l.expectOnlyRemovedEvent(2);
      });
   }

   @ClientListener(filterFactoryName = "non-existing-test-filter-factory")
   public static class NonExistingFilterFactoryListener<K> extends CustomEventLogListener<K, Object> {
      public NonExistingFilterFactoryListener(RemoteCache<K, ?> r) { super(r); }
   }

}
