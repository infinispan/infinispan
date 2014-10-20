package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.notifications.cachelistener.filter.NamedFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.EventType;

import java.io.Serializable;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

@ClientListener(converterFactoryName = "test-converter-factory")
public abstract class CustomEventLogListener {
   BlockingQueue<CustomEvent> createdCustomEvents = new ArrayBlockingQueue<CustomEvent>(128);
   BlockingQueue<CustomEvent> modifiedCustomEvents = new ArrayBlockingQueue<CustomEvent>(128);
   BlockingQueue<CustomEvent> removedCustomEvents = new ArrayBlockingQueue<CustomEvent>(128);

   public CustomEvent pollEvent(ClientEvent.Type type) {
      try {
         CustomEvent event = queue(type).poll(10, TimeUnit.SECONDS);
         assertNotNull(event);
         return event;
      } catch (InterruptedException e) {
         throw new AssertionError(e);
      }
   }

   private BlockingQueue<CustomEvent> queue(ClientEvent.Type type) {
      switch (type) {
         case CLIENT_CACHE_ENTRY_CREATED: return createdCustomEvents;
         case CLIENT_CACHE_ENTRY_MODIFIED: return modifiedCustomEvents;
         case CLIENT_CACHE_ENTRY_REMOVED: return removedCustomEvents;
         default: throw new IllegalArgumentException("Unknown event type: " + type);
      }
   }

   public void expectNoEvents(ClientEvent.Type type) {
      assertEquals(0, queue(type).size());
   }

   public void expectNoEvents() {
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED);
   }

   public void expectSingleCustomEvent(ClientEvent.Type type, Integer key, String value) {
      CustomEvent event = pollEvent(type);
      assertEquals(key, event.key);
      assertEquals(value, event.value);
   }

   public void expectOnlyCreatedCustomEvent(Integer key, String value) {
      expectSingleCustomEvent(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED, key, value);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED);
   }

   public void expectOnlyModifiedCustomEvent(Integer key, String value) {
      expectSingleCustomEvent(ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED, key, value);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED);
   }

   public void expectOnlyRemovedCustomEvent(Integer key, String value) {
      expectSingleCustomEvent(ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED, key, value);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED);
   }

   @ClientCacheEntryCreated
   @SuppressWarnings("unused")
   public void handleCustomCreatedEvent(ClientCacheEntryCustomEvent<CustomEvent> e) {
      assertEquals(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED, e.getType());
      createdCustomEvents.add(e.getEventData());
   }

   @ClientCacheEntryModified
   @SuppressWarnings("unused")
   public void handleCustomModifiedEvent(ClientCacheEntryCustomEvent<CustomEvent> e) {
      assertEquals(ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED, e.getType());
      modifiedCustomEvents.add(e.getEventData());
   }

   @ClientCacheEntryRemoved
   @SuppressWarnings("unused")
   public void handleCustomRemovedEvent(ClientCacheEntryCustomEvent<CustomEvent> e) {
      assertEquals(ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED, e.getType());
      removedCustomEvents.add(e.getEventData());
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

   @ClientListener(converterFactoryName = "static-converter-factory", includeCurrentState = true)
   public static class StaticCustomEventLogWithStateListener extends CustomEventLogListener {}

   @ClientListener(converterFactoryName = "dynamic-converter-factory")
   public static class DynamicCustomEventLogListener extends CustomEventLogListener {}

   @ClientListener(converterFactoryName = "dynamic-converter-factory", includeCurrentState = true)
   public static class DynamicCustomEventWithStateLogListener extends CustomEventLogListener {}

   @NamedFactory(name = "static-converter-factory")
   public static class StaticConverterFactory implements CacheEventConverterFactory {
      @Override
      public CacheEventConverter<Integer, String, CustomEvent> getConverter(Object[] params) {
         return new StaticConverter();
      }

      static class StaticConverter implements CacheEventConverter<Integer, String, CustomEvent>, Serializable {
         @Override
         public CustomEvent convert(Integer key, String previousValue, Metadata previousMetadata, String value,
                                    Metadata metadata, EventType eventType) {
            return new CustomEvent(key, value);
         }
      }
   }

   @NamedFactory(name = "dynamic-converter-factory")
   public static class DynamicConverterFactory implements CacheEventConverterFactory {
      @Override
      public CacheEventConverter<Integer, String, CustomEvent> getConverter(final Object[] params) {
         return new DynamicConverter(params);
      }

      static class DynamicConverter implements CacheEventConverter<Integer, String, CustomEvent>, Serializable {
         private final Object[] params;

         public DynamicConverter(Object[] params) {
            this.params = params;
         }

         @Override
         public CustomEvent convert(Integer key, String previousValue, Metadata previousMetadata, String value,
                                    Metadata metadata, EventType eventType) {
            if (params[0].equals(key))
               return new CustomEvent(key, null);

            return new CustomEvent(key, value);
         }
      }
   }

}
