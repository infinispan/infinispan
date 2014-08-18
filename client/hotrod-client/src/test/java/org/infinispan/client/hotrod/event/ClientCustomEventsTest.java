package org.infinispan.client.hotrod.event;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withClientListener;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.event.CustomEventListener.CustomEvent;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.test.RemoteCacheManagerCallable;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.filter.Converter;
import org.infinispan.filter.ConverterFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

@Test(groups = "functional", testName = "client.hotrod.event.ClientCustomEventsTest")
public class ClientCustomEventsTest extends SingleHotRodServerTest {

   TestConverterFactory converterFactory = new TestConverterFactory();

   @Override
   protected HotRodServer createHotRodServer() {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      HotRodServer server = TestHelper.startHotRodServer(cacheManager, builder);
      server.addConverterFactory("test-converter-factory", converterFactory);
      return server;
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
   
   /**
    * Test that the HotRod server returns an error when a ClientListener is registered with a non-existing 'converterFactoryName'.
    */
   public void testNonExistingConverterFactoryCustomEvents() {
      final NonExistingConverterFactoryCustomEventListener eventListener = new NonExistingConverterFactoryCustomEventListener();
	  converterFactory.dynamic = false;
      boolean caughtException = false;
      try {
    	  withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
    		  @Override
    		  public void call() {
                   //don't need to do anything as we expect a HotRodClientException when the listener is registered.
    		  }
    	  	});
      } catch (HotRodClientException hrce) {
    	  caughtException = true;
      }
      assertEquals(true, caughtException);      
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

   public void testConvertedEventsReplay() {
      final CustomEventListener eventListener = new CustomEventListener();
      converterFactory.dynamic = false;
      RemoteCache<Integer, String> cache = remoteCacheManager.getCache();
      cache.put(1, "one");
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            eventListener.expectSingleCustomEvent(1, "one");
         }
      });
      converterFactory.dynamic = true;
      cache.put(2, "two");
      withClientListener(eventListener, null, new Object[]{2}, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            eventListener.expectSingleCustomEvent(2, null);
         }
      });
   }

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
