package org.infinispan.client.hotrod.event;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withClientListener;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.CustomEventLogListener.*;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.RemoteCacheManagerCallable;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.event.ClientCustomEventsTest")
public class ClientCustomEventsTest extends SingleHotRodServerTest {

   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      HotRodServer server = HotRodClientTestingUtil.startHotRodServer(cacheManager, builder);
      server.addCacheEventConverterFactory("static-converter-factory", new StaticConverterFactory());
      server.addCacheEventConverterFactory("dynamic-converter-factory", new DynamicConverterFactory());
      server.addCacheEventConverterFactory("raw-static-converter-factory", new RawStaticConverterFactory());
      return server;
   }

   public void testCustomEvents() {
      final StaticCustomEventLogListener eventListener = new StaticCustomEventLogListener();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<Integer, String> cache = rcm.getCache();
            eventListener.expectNoEvents();
            cache.put(1, "one");
            eventListener.expectCreatedEvent(new CustomEvent(1, "one", 0));
            cache.put(1, "newone");
            eventListener.expectModifiedEvent(new CustomEvent(1, "newone", 0));
            cache.remove(1);
            eventListener.expectRemovedEvent(new CustomEvent(1, null, 0));
         }
      });
   }
   
   public void testTimeOrderedEvents() {
      final StaticCustomEventLogListener eventListener = new StaticCustomEventLogListener();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<Integer, String> cache = rcm.getCache();
            eventListener.expectNoEvents();
            cache.put(1, "one");
            cache.replace(1, "newone");
            cache.replace(1, "newnewone");
            cache.replace(1, "newnewnewone");
            cache.replace(1, "newnewnewnewone");
            cache.replace(1, "newnewnewnewnewone");
            eventListener.expectOrderedEventQueue(ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED);
         }
      });
   }

   /**
    * Test that the HotRod server returns an error when a ClientListener is
    * registered with a non-existing 'converterFactoryName'.
    */
   @Test(expectedExceptions = HotRodClientException.class)
   public void testNonExistingConverterFactoryCustomEvents() {
      NonExistingConverterFactoryListener eventListener = new NonExistingConverterFactoryListener();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager));
   }

   public void testParameterBasedConversion() {
      final DynamicCustomEventLogListener eventListener = new DynamicCustomEventLogListener();
      withClientListener(eventListener, null, new Object[]{2}, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<Integer, String> cache = rcm.getCache();
            eventListener.expectNoEvents();
            cache.put(1, "one");
            eventListener.expectCreatedEvent(new CustomEvent(1, "one", 0));
            cache.put(2, "two");
            eventListener.expectCreatedEvent(new CustomEvent(2, null, 0));
         }
      });
   }

   public void testConvertedEventsReplay() {
      final StaticCustomEventLogWithStateListener staticEventListener = new StaticCustomEventLogWithStateListener();
      RemoteCache<Integer, String> cache = remoteCacheManager.getCache();
      cache.put(1, "one");
      withClientListener(staticEventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            staticEventListener.expectCreatedEvent(new CustomEvent(1, "one", 0));
         }
      });
      final DynamicCustomEventWithStateLogListener dynamicEventListener = new DynamicCustomEventWithStateLogListener();
      cache.put(2, "two");
      withClientListener(dynamicEventListener, null, new Object[]{2}, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            dynamicEventListener.expectCreatedEvent(new CustomEvent(2, null, 0));
         }
      });
   }

   public void testConvertedNoEventsReplay() {
      final StaticCustomEventLogListener staticEventListener = new StaticCustomEventLogListener();
      RemoteCache<Integer, String> cache = remoteCacheManager.getCache();
      cache.put(1, "one");
      withClientListener(staticEventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            staticEventListener.expectNoEvents();
         }
      });
      final DynamicCustomEventLogListener dynamicEventListener = new DynamicCustomEventLogListener();
      cache.put(2, "two");
      withClientListener(dynamicEventListener, null, new Object[]{2}, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            staticEventListener.expectNoEvents();
         }
      });
   }

   public void testRawCustomEvents() {
      final RawStaticCustomEventLogListener eventListener = new RawStaticCustomEventLogListener();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<Integer, String> cache = rcm.getCache();
            eventListener.expectNoEvents();
            cache.put(1, "one");
            // 1 = [3,75,0,0,0,1], "one" = [3,62,3,111,110,101]
            eventListener.expectCreatedEvent(new byte[]{3, 75, 0, 0, 0, 1, 3, 62, 3, 111, 110, 101});
            cache.put(1, "newone");
            // "newone" = [3,62,6,110,101,119,111,110,101]
            eventListener.expectModifiedEvent(new byte[]{3, 75, 0, 0, 0, 1, 3, 62, 6, 110, 101, 119, 111, 110, 101});
            cache.remove(1);
            eventListener.expectRemovedEvent(new byte[]{3, 75, 0, 0, 0, 1});
         }
      });
   }


   @ClientListener(converterFactoryName = "non-existing-test-converter-factory")
   public static class NonExistingConverterFactoryListener extends CustomEventLogListener {}

}
