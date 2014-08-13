package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.event.CustomEventLogListener.DynamicConverterFactory;
import org.infinispan.client.hotrod.event.CustomEventLogListener.DynamicCustomEventLogListener;
import org.infinispan.client.hotrod.event.CustomEventLogListener.StaticConverterFactory;
import org.infinispan.client.hotrod.event.CustomEventLogListener.StaticCustomEventLogListener;
import org.infinispan.client.hotrod.test.RemoteCacheManagerCallable;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.Test;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withClientListener;

@Test(groups = "functional", testName = "client.hotrod.event.ClientCustomEventsTest")
public class ClientCustomEventsTest extends SingleHotRodServerTest {

   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      HotRodServer server = TestHelper.startHotRodServer(cacheManager, builder);
      server.addConverterFactory("static-converter-factory", new StaticConverterFactory());
      server.addConverterFactory("dynamic-converter-factory", new DynamicConverterFactory());
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
            eventListener.expectSingleCustomEvent(1, "one");
            cache.put(1, "newone");
            eventListener.expectSingleCustomEvent(1, "newone");
            cache.remove(1);
            eventListener.expectSingleCustomEvent(1, null);
         }
      });
   }

   public void testParameterBasedConversion() {
      final DynamicCustomEventLogListener eventListener = new DynamicCustomEventLogListener();
      withClientListener(eventListener, null, new Object[]{2}, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<Integer, String> cache = rcm.getCache();
            eventListener.expectNoEvents();
            cache.put(1, "one");
            eventListener.expectSingleCustomEvent(1, "one");
            cache.put(2, "two");
            eventListener.expectSingleCustomEvent(2, null);
         }
      });
   }

   public void testConvertedEventsReplay() {
      final StaticCustomEventLogListener staticEventListener = new StaticCustomEventLogListener();
      RemoteCache<Integer, String> cache = remoteCacheManager.getCache();
      cache.put(1, "one");
      withClientListener(staticEventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            staticEventListener.expectSingleCustomEvent(1, "one");
         }
      });
      final DynamicCustomEventLogListener dynamicEventListener = new DynamicCustomEventLogListener();
      cache.put(2, "two");
      withClientListener(dynamicEventListener, null, new Object[]{2}, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            dynamicEventListener.expectSingleCustomEvent(2, null);
         }
      });
   }

}
