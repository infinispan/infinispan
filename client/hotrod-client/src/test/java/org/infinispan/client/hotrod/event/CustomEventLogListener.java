package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.filter.Converter;
import org.infinispan.filter.ConverterFactory;
import org.infinispan.filter.NamedFactory;
import org.infinispan.metadata.Metadata;

import java.io.Serializable;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

@ClientListener(converterFactoryName = "test-converter-factory")
public abstract class CustomEventLogListener {
   BlockingQueue<CustomEvent> customEvents = new ArrayBlockingQueue<CustomEvent>(128);

   CustomEvent pollEvent() {
      try {
         CustomEvent event = customEvents.poll(10, TimeUnit.SECONDS);
         assertNotNull(event);
         return event;
      } catch (InterruptedException e) {
         throw new AssertionError(e);
      }
   }

   public void expectNoEvents() {
      assertEquals(0, customEvents.size());
   }

   public void expectSingleCustomEvent(Integer key, String value) {
      CustomEvent event = pollEvent();
      assertEquals(key, event.key);
      assertEquals(value, event.value);
   }

   @ClientCacheEntryCreated
   @ClientCacheEntryModified
   @ClientCacheEntryRemoved
   @SuppressWarnings("unused")
   public void handleCustomEvent(ClientCacheEntryCustomEvent<CustomEvent> e) {
      customEvents.add(e.getEventData());
   }

   public static class CustomEvent implements Serializable {
      final Integer key;
      final String value;
      CustomEvent(Integer key, String value) {
         this.key = key;
         this.value = value;
      }
   }

   @ClientListener(converterFactoryName = "static-converter-factory")
   public static class StaticCustomEventLogListener extends CustomEventLogListener {}

   @ClientListener(converterFactoryName = "dynamic-converter-factory")
   public static class DynamicCustomEventLogListener extends CustomEventLogListener {}

   @NamedFactory(name = "static-converter-factory")
   public static class StaticConverterFactory implements ConverterFactory {
      @Override
      public Converter<Integer, String, CustomEvent> getConverter(Object[] params) {
         return new StaticConverter();
      }

      static class StaticConverter implements Converter<Integer, String, CustomEvent>, Serializable {
         @Override
         public CustomEvent convert(Integer key, String value, Metadata metadata) {
            return new CustomEvent(key, value);
         }
      }
   }

   @NamedFactory(name = "dynamic-converter-factory")
   public static class DynamicConverterFactory implements ConverterFactory {
      @Override
      public Converter<Integer, String, CustomEvent> getConverter(final Object[] params) {
         return new DynamicConverter(params);
      }

      static class DynamicConverter implements Converter<Integer, String, CustomEvent>, Serializable {
         private final Object[] params;

         public DynamicConverter(Object[] params) {
            this.params = params;
         }

         @Override
         public CustomEvent convert(Integer key, String value, Metadata metadata) {
            if (params[0].equals(key))
               return new CustomEvent(key, null);

            return new CustomEvent(key, value);
         }
      }
   }

}
