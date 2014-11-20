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

import static org.infinispan.test.TestingUtil.assertAnyEquals;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

@ClientListener(converterFactoryName = "test-converter-factory")
public abstract class CustomEventLogListener<E> {
   BlockingQueue<E> createdCustomEvents = new ArrayBlockingQueue<>(128);
   BlockingQueue<E> modifiedCustomEvents = new ArrayBlockingQueue<>(128);
   BlockingQueue<E> removedCustomEvents = new ArrayBlockingQueue<>(128);

   public E pollEvent(ClientEvent.Type type) {
      try {
         E event = queue(type).poll(10, TimeUnit.SECONDS);
         assertNotNull(event);
         return event;
      } catch (InterruptedException e) {
         throw new AssertionError(e);
      }
   }

   private BlockingQueue<E> queue(ClientEvent.Type type) {
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

   public void expectSingleCustomEvent(ClientEvent.Type type, E expected) {
      E event = pollEvent(type);
      assertAnyEquals(expected, event);
   }

   public void expectOnlyCreatedCustomEvent(E expected) {
      expectSingleCustomEvent(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED, expected);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED);
   }

   public void expectOnlyModifiedCustomEvent(E expected) {
      expectSingleCustomEvent(ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED, expected);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED);
   }

   public void expectOnlyRemovedCustomEvent(E expected) {
      expectSingleCustomEvent(ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED, expected);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED);
   }

   @ClientCacheEntryCreated
   @SuppressWarnings("unused")
   public void handleCustomCreatedEvent(ClientCacheEntryCustomEvent<E> e) {
      assertEquals(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED, e.getType());
      createdCustomEvents.add(e.getEventData());
   }

   @ClientCacheEntryModified
   @SuppressWarnings("unused")
   public void handleCustomModifiedEvent(ClientCacheEntryCustomEvent<E> e) {
      assertEquals(ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED, e.getType());
      modifiedCustomEvents.add(e.getEventData());
   }

   @ClientCacheEntryRemoved
   @SuppressWarnings("unused")
   public void handleCustomRemovedEvent(ClientCacheEntryCustomEvent<E> e) {
      assertEquals(ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED, e.getType());
      removedCustomEvents.add(e.getEventData());
   }

   public static final class CustomEvent implements Serializable {
      final Integer key;
      final String value;
      public CustomEvent(Integer key, String value) {
         this.key = key;
         this.value = value;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         CustomEvent that = (CustomEvent) o;

         if (!key.equals(that.key)) return false;
         if (value != null ? !value.equals(that.value) : that.value != null)
            return false;

         return true;
      }

      @Override
      public int hashCode() {
         int result = key.hashCode();
         result = 31 * result + (value != null ? value.hashCode() : 0);
         return result;
      }
   }

   @ClientListener(converterFactoryName = "static-converter-factory")
   public static class StaticCustomEventLogListener extends CustomEventLogListener<CustomEvent> {}

   @ClientListener(converterFactoryName = "raw-static-converter-factory", useRawData = true)
   public static class RawStaticCustomEventLogListener extends CustomEventLogListener<byte[]> {}

   @ClientListener(converterFactoryName = "static-converter-factory", includeCurrentState = true)
   public static class StaticCustomEventLogWithStateListener extends CustomEventLogListener<CustomEvent> {}

   @ClientListener(converterFactoryName = "dynamic-converter-factory")
   public static class DynamicCustomEventLogListener extends CustomEventLogListener<CustomEvent> {}

   @ClientListener(converterFactoryName = "dynamic-converter-factory", includeCurrentState = true)
   public static class DynamicCustomEventWithStateLogListener extends CustomEventLogListener<CustomEvent> {}

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

   @NamedFactory(name = "raw-static-converter-factory")
   public static class RawStaticConverterFactory implements CacheEventConverterFactory {
      @Override
      public CacheEventConverter<byte[], byte[], byte[]> getConverter(Object[] params) {
         return new RawStaticConverter();
      }

      static class RawStaticConverter implements CacheEventConverter<byte[], byte[], byte[]>, Serializable {
         @Override
         public byte[] convert(byte[] key, byte[] previousValue, Metadata previousMetadata, byte[] value,
               Metadata metadata, EventType eventType) {
            return value != null ? concat(key, value) : key;
         }
      }
   }

   static byte[] concat(byte[] a, byte[] b) {
      int aLen = a.length;
      int bLen = b.length;
      byte[] ret = new byte[aLen + bLen];
      System.arraycopy(a, 0, ret, 0, aLen);
      System.arraycopy(b, 0, ret, aLen, bLen);
      return ret;
   }

}
