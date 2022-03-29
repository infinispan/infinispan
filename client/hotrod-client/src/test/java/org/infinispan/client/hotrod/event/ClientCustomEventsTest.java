package org.infinispan.client.hotrod.event;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withClientListener;
import static org.testng.AssertJUnit.fail;

import java.io.IOException;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.CustomEventLogListener.CustomEvent;
import org.infinispan.client.hotrod.event.CustomEventLogListener.DynamicConverterFactory;
import org.infinispan.client.hotrod.event.CustomEventLogListener.DynamicCustomEventLogListener;
import org.infinispan.client.hotrod.event.CustomEventLogListener.DynamicCustomEventWithStateLogListener;
import org.infinispan.client.hotrod.event.CustomEventLogListener.RawStaticConverterFactory;
import org.infinispan.client.hotrod.event.CustomEventLogListener.RawStaticCustomEventLogListener;
import org.infinispan.client.hotrod.event.CustomEventLogListener.SimpleConverterFactory;
import org.infinispan.client.hotrod.event.CustomEventLogListener.SimpleListener;
import org.infinispan.client.hotrod.event.CustomEventLogListener.StaticConverterFactory;
import org.infinispan.client.hotrod.event.CustomEventLogListener.StaticCustomEventLogListener;
import org.infinispan.client.hotrod.event.CustomEventLogListener.StaticCustomEventLogWithStateListener;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.protostream.SerializationContextInitializer;
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
      server.addCacheEventConverterFactory("simple-converter-factory", new SimpleConverterFactory<>());
      return server;
   }

   @Override
   protected SerializationContextInitializer contextInitializer() {
      return ClientEventSCI.INSTANCE;
   }

   public void testCustomEvents() {
      final StaticCustomEventLogListener<Integer> l =
            new StaticCustomEventLogListener<>(remoteCacheManager.getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.put(1, "one");
         l.expectCreatedEvent(new CustomEvent(1, "one", 0));
         remote.put(1, "newone");
         l.expectModifiedEvent(new CustomEvent(1, "newone", 0));
         remote.remove(1);
         l.expectRemovedEvent(new CustomEvent(1, null, 0));
      });
   }

   public void testCustomEvents2() {
      final SimpleListener<String> l = new SimpleListener<>(remoteCacheManager.getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.put("1", "one");
         l.expectCreatedEvent("one");
      });
   }

   public void testTimeOrderedEvents() {
      final StaticCustomEventLogListener<Integer> l =
            new StaticCustomEventLogListener<>(remoteCacheManager.getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.put(1, "one");
         remote.replace(1, "newone");
         remote.replace(1, "newnewone");
         remote.replace(1, "newnewnewone");
         remote.replace(1, "newnewnewnewone");
         remote.replace(1, "newnewnewnewnewone");
         l.expectOrderedEventQueue(ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED);
      });
   }

   /**
    * Test that the HotRod server returns an error when a ClientListener is
    * registered with a non-existing 'converterFactoryName'.
    */
   @Test(expectedExceptions = HotRodClientException.class)
   public void testNonExistingConverterFactoryCustomEvents() {
      NonExistingConverterFactoryListener l = new NonExistingConverterFactoryListener<>(remoteCacheManager.getCache());
      withClientListener(l, remote -> {});
   }

   public void testNoConverterFactoryCustomEvents() {
      NoConverterFactoryListener l = new NoConverterFactoryListener<>(remoteCacheManager.getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.put(1, "one");
         // We don't get an event, since we don't have a converter and we only allow custom events
         l.expectNoEvents();
      });
   }

   public void testParameterBasedConversion() {
      final DynamicCustomEventLogListener<Integer> l =
            new DynamicCustomEventLogListener<>(remoteCacheManager.getCache());
      withClientListener(l, null, new Object[]{2}, remote -> {
         l.expectNoEvents();
         remote.put(1, "one");
         l.expectCreatedEvent(new CustomEvent(1, "one", 0));
         remote.put(2, "two");
         l.expectCreatedEvent(new CustomEvent(2, null, 0));
      });
   }

   public void testConvertedEventsReplay() {
      RemoteCache<Integer, String> cache = remoteCacheManager.getCache();
      cache.put(1, "one");
      StaticCustomEventLogWithStateListener<Integer> staticEventListener =
            new StaticCustomEventLogWithStateListener<>(cache);
      withClientListener(staticEventListener, remote ->
            staticEventListener.expectCreatedEvent(new CustomEvent(1, "one", 0)));
      DynamicCustomEventWithStateLogListener<Integer> dynamicEventListener =
            new DynamicCustomEventWithStateLogListener<>(cache);
      withClientListener(dynamicEventListener, null, new Object[]{2}, remote -> {
         dynamicEventListener.expectCreatedEvent(new CustomEvent(1, "one", 0));
         cache.put(2, "two");
         dynamicEventListener.expectCreatedEvent(new CustomEvent(2, null, 0));
      });
   }

   public void testConvertedNoEventsReplay() {
      RemoteCache<Integer, String> cache = remoteCacheManager.getCache();
      StaticCustomEventLogListener staticEventListener = new StaticCustomEventLogListener<>(cache);
      cache.put(1, "one");
      withClientListener(staticEventListener, remote ->
            staticEventListener.expectNoEvents());
      DynamicCustomEventLogListener dynamicEventListener = new DynamicCustomEventLogListener<>(cache);
      cache.put(2, "two");
      withClientListener(dynamicEventListener, null, new Object[]{2}, remote ->
            staticEventListener.expectNoEvents());
   }

   public void testRawCustomEvents() {
      RawStaticCustomEventLogListener<Integer> eventListener =
            new RawStaticCustomEventLogListener<>(remoteCacheManager.getCache());
      withClientListener(eventListener, remote -> {
         eventListener.expectNoEvents();
         Marshaller marshaller = remote.getRemoteCacheContainer().getMarshaller();
         Integer key = 1;
         Object value = "one";
         try {
            byte[] keyBytes = marshaller.objectToByteBuffer(key);
            byte[] valBytes = marshaller.objectToByteBuffer(value);

            // Put initial value and assert converter creates a byte[] of the key + value bytes
            remote.put(key, value);
            eventListener.expectCreatedEvent(CustomEventLogListener.concat(keyBytes, valBytes));
            value = "newone";

            // Repeat with new value
            valBytes = marshaller.objectToByteBuffer(value);
            remote.put(key, value);
            eventListener.expectModifiedEvent(CustomEventLogListener.concat(keyBytes, valBytes));

            // Only keyBytes should be returned as no value in remove event
            remote.remove(key);
            eventListener.expectRemovedEvent(keyBytes);
         } catch (InterruptedException | IOException e) {
            fail(e.getMessage());
         }
      });
   }

   @ClientListener(converterFactoryName = "non-existing-test-converter-factory")
   public static class NonExistingConverterFactoryListener<K> extends CustomEventLogListener<K, Object> {
      public NonExistingConverterFactoryListener(RemoteCache<K, ?> r) { super(r); }
   }

   @ClientListener
   public static class NoConverterFactoryListener<K> extends CustomEventLogListener<K, Object> {
      public NoConverterFactoryListener(RemoteCache<K, ?> r) { super(r); }
   }
}
