package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.event.CustomEventListener.CustomEvent;
import org.infinispan.client.hotrod.test.RemoteCacheManagerCallable;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.metadata.Metadata;
import org.infinispan.filter.ConverterFactory;
import org.infinispan.filter.Converter;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.Test;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withClientListener;

@Test(groups = "functional", testName = "client.hotrod.event.ClientCustomEventsTest")
public class ClientCustomEventsTest extends SingleHotRodServerTest {

   TestConverterFactory converterFactory = new TestConverterFactory();

   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      builder.converterFactory("test-converter-factory", converterFactory);
      return TestHelper.startHotRodServer(cacheManager, builder);
   }

   public void testCustomEvents() {
      final CustomEventListener eventListener = new CustomEventListener();
      converterFactory.dynamic = false;
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
      final CustomEventListener eventListener = new CustomEventListener();
      converterFactory.dynamic = true;
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

//   @ClientListener(converterFactoryName = "test-converter-factory")
//   static class CustomEventListener {
//      BlockingQueue<CustomEvent> customEvents = new ArrayBlockingQueue<CustomEvent>(128);
//
//      CustomEvent pollEvent() {
//         try {
//            CustomEvent event = customEvents.poll(10, TimeUnit.SECONDS);
//            assertNotNull(event);
//            return event;
//         } catch (InterruptedException e) {
//            throw new AssertionError(e);
//         }
//      }
//
//      @ClientCacheEntryCreated
//      @ClientCacheEntryModified
//      @ClientCacheEntryRemoved
//      @SuppressWarnings("unused")
//      public void handleCustomEvent(ClientCacheEntryCustomEvent<CustomEvent> e) {
//         customEvents.add(e.getEventData());
//      }
//   }

   static class TestConverterFactory implements ConverterFactory {
      boolean dynamic;
      @Override
      public Converter<Integer, String, CustomEvent> getConverter(final Object[] params) {
         return new Converter<Integer, String, CustomEvent>() {
            @Override
            public CustomEvent convert(Integer key, String value, Metadata metadata) {
               if (dynamic && params[0].equals(key))
                  return new CustomEvent(key, null);

               return new CustomEvent(key, value);
            }
         };
      }
   }

}
