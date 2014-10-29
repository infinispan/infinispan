package org.infinispan.client.hotrod.event;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withClientListener;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.CustomEventLogListener.DynamicConverterFactory;
import org.infinispan.client.hotrod.event.CustomEventLogListener.DynamicCustomEventLogListener;
import org.infinispan.client.hotrod.event.CustomEventLogListener.DynamicCustomEventWithStateLogListener;
import org.infinispan.client.hotrod.event.CustomEventLogListener.StaticConverterFactory;
import org.infinispan.client.hotrod.event.CustomEventLogListener.StaticCustomEventLogListener;
import org.infinispan.client.hotrod.event.CustomEventLogListener.StaticCustomEventLogWithStateListener;
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
      HotRodServer server = TestHelper.startHotRodServer(cacheManager, builder);
      server.addCacheEventConverterFactory("static-converter-factory", new StaticConverterFactory());
      server.addCacheEventConverterFactory("dynamic-converter-factory", new DynamicConverterFactory());
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
            eventListener.expectOnlyCreatedCustomEvent(1, "one");
            cache.put(1, "newone");
            eventListener.expectOnlyModifiedCustomEvent(1, "newone");
            cache.remove(1);
            eventListener.expectOnlyRemovedCustomEvent(1, null);
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
            eventListener.expectOnlyCreatedCustomEvent(1, "one");
            cache.put(2, "two");
            eventListener.expectOnlyCreatedCustomEvent(2, null);
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
            staticEventListener.expectOnlyCreatedCustomEvent(1, "one");
         }
      });
      final DynamicCustomEventWithStateLogListener dynamicEventListener = new DynamicCustomEventWithStateLogListener();
      cache.put(2, "two");
      withClientListener(dynamicEventListener, null, new Object[]{2}, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            dynamicEventListener.expectOnlyCreatedCustomEvent(2, null);
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

   @ClientListener(converterFactoryName = "non-existing-test-converter-factory")
   public static class NonExistingConverterFactoryListener extends CustomEventLogListener {}

}
