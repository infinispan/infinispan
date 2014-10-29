package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.EventLogListener.DynamicFilteredEventLogListener;
import org.infinispan.client.hotrod.event.EventLogListener.DynamicFilteredEventLogWithStateListener;
import org.infinispan.client.hotrod.event.EventLogListener.DynamicCacheEventFilterFactory;
import org.infinispan.client.hotrod.event.EventLogListener.StaticFilteredEventLogListener;
import org.infinispan.client.hotrod.event.EventLogListener.StaticFilteredEventLogWithStateListener;
import org.infinispan.client.hotrod.event.EventLogListener.StaticCacheEventFilterFactory;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.test.RemoteCacheManagerCallable;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.Test;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withClientListener;

@Test(groups = "functional", testName = "client.hotrod.event.ClientFilterEventsTest")
public class ClientFilterEventsTest extends SingleHotRodServerTest {

   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      HotRodServer server = TestHelper.startHotRodServer(cacheManager, builder);
      server.addCacheEventFilterFactory("static-filter-factory", new StaticCacheEventFilterFactory());
      server.addCacheEventFilterFactory("dynamic-filter-factory", new DynamicCacheEventFilterFactory());
      return server;
   }

   public void testFilteredEvents() {
      final StaticFilteredEventLogListener<Integer> eventListener = new StaticFilteredEventLogListener<>();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<Integer, String> cache = rcm.getCache();
            eventListener.expectNoEvents();
            cache.put(1, "one");
            eventListener.expectNoEvents();
            cache.put(2, "two");
            eventListener.expectOnlyCreatedEvent(2, cache());
            cache.remove(1);
            eventListener.expectNoEvents();
            cache.remove(2);
            eventListener.expectOnlyRemovedEvent(2, cache());
         }
      });
   }

   public void testParameterBasedFiltering() {
      final DynamicFilteredEventLogListener<Integer> eventListener = new DynamicFilteredEventLogListener<>();
      withClientListener(eventListener, new Object[]{3}, null, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<Integer, String> cache = rcm.getCache();
            eventListener.expectNoEvents();
            cache.put(1, "one");
            eventListener.expectNoEvents();
            cache.put(2, "two");
            eventListener.expectNoEvents();
            cache.put(3, "three");
            eventListener.expectOnlyCreatedEvent(3, cache());
         }
      });
   }

   public void testFilteredEventsReplay() {
      final StaticFilteredEventLogWithStateListener<Integer> staticEventListener =
            new StaticFilteredEventLogWithStateListener<>();
      RemoteCache<Integer, String> cache = remoteCacheManager.getCache();
      cache.put(1, "one");
      cache.put(2, "two");
      withClientListener(staticEventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            staticEventListener.expectOnlyCreatedEvent(2, cache());
            RemoteCache<Integer, String> cache = rcm.getCache();
            cache.remove(1);
            cache.remove(2);
            staticEventListener.expectOnlyRemovedEvent(2, cache());
         }
      });
      final DynamicFilteredEventLogWithStateListener<Integer> dynamicEventListener =
            new DynamicFilteredEventLogWithStateListener<>();
      cache.put(1, "one");
      cache.put(2, "two");
      cache.put(3, "three");
      withClientListener(dynamicEventListener, new Object[]{3}, null, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            dynamicEventListener.expectOnlyCreatedEvent(3, cache());
            RemoteCache<Integer, String> cache = rcm.getCache();
            cache.remove(1);
            cache.remove(2);
            cache.remove(3);
            dynamicEventListener.expectOnlyRemovedEvent(3, cache());
         }
      });
   }

   public void testFilteredNoEventsReplay() {
      final StaticFilteredEventLogListener<Integer> staticEventListener = new StaticFilteredEventLogListener<>();
      RemoteCache<Integer, String> cache = remoteCacheManager.getCache();
      cache.put(1, "one");
      cache.put(2, "two");
      withClientListener(staticEventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            staticEventListener.expectNoEvents();
            RemoteCache<Integer, String> cache = rcm.getCache();
            cache.remove(1);
            cache.remove(2);
            staticEventListener.expectOnlyRemovedEvent(2, cache());
         }
      });
      final DynamicFilteredEventLogListener<Integer> dynamicEventListener = new DynamicFilteredEventLogListener<>();
      cache.put(1, "one");
      cache.put(2, "two");
      cache.put(3, "three");
      withClientListener(dynamicEventListener, new Object[]{3}, null, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            staticEventListener.expectNoEvents();
            RemoteCache<Integer, String> cache = rcm.getCache();
            cache.remove(1);
            cache.remove(2);
            cache.remove(3);
            dynamicEventListener.expectOnlyRemovedEvent(3, cache());
         }
      });
   }

   /**
    * Test that the HotRod server returns an error when a ClientListener is
    * registered with a non-existing 'filterFactoryName'.
    */
   @Test(expectedExceptions = HotRodClientException.class)
   public void testNonExistingConverterFactoryCustomEvents() {
      NonExistingFilterFactoryListener eventListener = new NonExistingFilterFactoryListener();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager));
   }

   @ClientListener(filterFactoryName = "non-existing-test-filter-factory")
   public static class NonExistingFilterFactoryListener extends CustomEventLogListener {}

}
