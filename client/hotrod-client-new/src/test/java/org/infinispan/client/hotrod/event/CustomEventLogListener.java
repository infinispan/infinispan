package org.infinispan.client.hotrod.event;

import static org.infinispan.test.TestingUtil.assertAnyEquals;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryExpired;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.filter.NamedFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.AbstractCacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventConverterFactory;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterConverterFactory;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoName;

@ClientListener(converterFactoryName = "test-converter-factory")
public abstract class CustomEventLogListener<K, E> implements RemoteCacheSupplier<K> {
   BlockingQueue<E> createdCustomEvents = new ArrayBlockingQueue<>(128);
   BlockingQueue<E> modifiedCustomEvents = new ArrayBlockingQueue<>(128);
   BlockingQueue<E> removedCustomEvents = new ArrayBlockingQueue<>(128);
   BlockingQueue<E> expiredCustomEvents = new ArrayBlockingQueue<>(128);

   private final RemoteCache<K, ?> remote;

   protected CustomEventLogListener(RemoteCache<K, ?> remote) {
      this.remote = remote;
   }

   @Override
   @SuppressWarnings("unchecked")
   public <V> RemoteCache<K, V> get() {
      return (RemoteCache<K, V>) remote;
   }

   public E pollEvent(ClientEvent.Type type) {
      try {
         E event = queue(type).poll(10, TimeUnit.SECONDS);
         assertNotNull(event);
         return event;
      } catch (InterruptedException e) {
         throw new AssertionError(e);
      }
   }

   protected BlockingQueue<E> queue(ClientEvent.Type type) {
      switch (type) {
         case CLIENT_CACHE_ENTRY_CREATED: return createdCustomEvents;
         case CLIENT_CACHE_ENTRY_MODIFIED: return modifiedCustomEvents;
         case CLIENT_CACHE_ENTRY_REMOVED: return removedCustomEvents;
         case CLIENT_CACHE_ENTRY_EXPIRED: return expiredCustomEvents;
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
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_EXPIRED);
   }

   public void expectSingleCustomEvent(ClientEvent.Type type, E expected) {
      E event = pollEvent(type);
      assertAnyEquals(expected, event);
   }

   public void expectCreatedEvent(E expected) {
      expectSingleCustomEvent(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED, expected);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_EXPIRED);
   }

   public void expectModifiedEvent(E expected) {
      expectSingleCustomEvent(ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED, expected);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_EXPIRED);
   }

   public void expectRemovedEvent(E expected) {
      expectSingleCustomEvent(ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED, expected);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_EXPIRED);
   }

   public void expectExpiredEvent(E expected) {
      expectSingleCustomEvent(ClientEvent.Type.CLIENT_CACHE_ENTRY_EXPIRED, expected);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED);
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

   @ClientCacheEntryExpired
   @SuppressWarnings("unused")
   public void handleCustomExpiredEvent(ClientCacheEntryCustomEvent<E> e) {
      assertEquals(ClientEvent.Type.CLIENT_CACHE_ENTRY_EXPIRED, e.getType());
      expiredCustomEvents.add(e.getEventData());
   }

   public static final class CustomEvent<K> {
      @ProtoField(1)
      final WrappedMessage key;

      @ProtoField(2)
      final String value;

      @ProtoField(number = 3, defaultValue = "-1")
      final long timestamp;

      @ProtoField(number = 4, defaultValue = "0")
      final int counter;

      public CustomEvent(K key, String value, int counter) {
         this(new WrappedMessage(key), value, System.nanoTime(), counter);
      }

      @ProtoFactory
      CustomEvent(WrappedMessage key, String value, long timestamp, int counter) {
         this.key = key;
         this.value = value;
         this.timestamp = timestamp;
         this.counter = counter;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         CustomEvent<?> that = (CustomEvent<?>) o;

         if (counter != that.counter) return false;
         if (!key.getValue().equals(that.key.getValue())) return false;
         return Objects.equals(value, that.value);
      }

      @Override
      public int hashCode() {
         int result = key.getValue().hashCode();
         result = 31 * result + (value != null ? value.hashCode() : 0);
         result = 31 * result + counter;
         return result;
      }

      @Override
      public String toString() {
         return "CustomEvent{" +
               "key=" + key.getValue() +
               ", value='" + value + '\'' +
               ", timestamp=" + timestamp +
               ", counter=" + counter +
               '}';
      }
   }

   @ClientListener(converterFactoryName = "static-converter-factory")
   public static class StaticCustomEventLogListener<K> extends CustomEventLogListener<K, CustomEvent> {
      public StaticCustomEventLogListener(RemoteCache<K, ?> r) { super(r); }

      @Override
      public void expectSingleCustomEvent(ClientEvent.Type type, CustomEvent expected) {
         CustomEvent event = pollEvent(type);
         assertNotNull(event.key);
         assertNotNull(event.timestamp); // check only custom field, value can be null
         assertAnyEquals(expected, event);
      }

      public void expectOrderedEventQueue(ClientEvent.Type type) {
         BlockingQueue<CustomEvent> queue = queue(type);
         if (queue.size() < 2)
            return;

         try {
            CustomEvent before = queue.poll(10, TimeUnit.SECONDS);
            Iterator<CustomEvent> iter = queue.iterator();
            while (iter.hasNext()) {
               CustomEvent after = iter.next();
               expectTimeOrdered(before, after);
               before = after;
            }
         } catch (InterruptedException e) {
            throw new AssertionError(e);
         }
      }

      private void expectTimeOrdered(CustomEvent before, CustomEvent after) {
         assertTrue("Before timestamp=" + before.timestamp + ", after timestamp=" + after.timestamp,
            before.timestamp < after.timestamp);
      }
   }

   @ClientListener(converterFactoryName = "raw-static-converter-factory", useRawData = true)
   public static class RawStaticCustomEventLogListener<K> extends CustomEventLogListener<K, byte[]> {
      public RawStaticCustomEventLogListener(RemoteCache<K, ?> r) { super(r); }
   }

   @ClientListener(converterFactoryName = "static-converter-factory", includeCurrentState = true)
   public static class StaticCustomEventLogWithStateListener<K> extends CustomEventLogListener<K, CustomEvent> {
      public StaticCustomEventLogWithStateListener(RemoteCache<K, ?> r) { super(r); }
   }

   @ClientListener(converterFactoryName = "dynamic-converter-factory")
   public static class DynamicCustomEventLogListener<K> extends CustomEventLogListener<K, CustomEvent> {
      public DynamicCustomEventLogListener(RemoteCache<K, ?> r) { super(r); }
   }

   @ClientListener(converterFactoryName = "dynamic-converter-factory", includeCurrentState = true)
   public static class DynamicCustomEventWithStateLogListener<K> extends CustomEventLogListener<K, CustomEvent> {
      public DynamicCustomEventWithStateLogListener(RemoteCache<K, ?> r) { super(r); }
   }

   @ClientListener(converterFactoryName = "simple-converter-factory")
   public static class SimpleListener<K> extends CustomEventLogListener<K, String> {
      public SimpleListener(RemoteCache<K, ?> r) {
         super(r);
      }
   }

   @NamedFactory(name = "simple-converter-factory")
   static class SimpleConverterFactory<K> implements CacheEventConverterFactory {
      @Override
      @SuppressWarnings("unchecked")
      public CacheEventConverter<String, String, CustomEvent> getConverter(Object[] params) {
         return new SimpleConverter();
      }

      @ProtoName("SimpleConverter")
      static class SimpleConverter<K> implements CacheEventConverter<K, String, String>, Serializable {
         @Override
         public String convert(K key, String oldValue, Metadata oldMetadata, String newValue, Metadata newMetadata, EventType eventType) {
            if (newValue != null) return newValue;
            return oldValue;
         }
      }
   }

   @NamedFactory(name = "static-converter-factory")
   public static class StaticConverterFactory<K> implements CacheEventConverterFactory {
      @Override
      public CacheEventConverter<K, String, CustomEvent> getConverter(Object[] params) {
         return new StaticConverter<>();
      }

      @ProtoName("StaticConverter")
      static class StaticConverter<K> implements CacheEventConverter<K, String, CustomEvent>, Serializable {
         @Override
         public CustomEvent convert(K key, String previousValue, Metadata previousMetadata, String value,
                                    Metadata metadata, EventType eventType) {
            return new CustomEvent<>(key, value, 0);
         }
      }
   }

   @NamedFactory(name = "dynamic-converter-factory")
   public static class DynamicConverterFactory<K> implements CacheEventConverterFactory {
      @Override
      public CacheEventConverter<K, String, CustomEvent> getConverter(final Object[] params) {
         return new DynamicConverter<>(params);
      }

      static class DynamicConverter<K> implements CacheEventConverter<K, String, CustomEvent>, Serializable {
         private final Object[] params;

         public DynamicConverter(Object[] params) {
            this.params = params;
         }

         @ProtoFactory
         DynamicConverter(ArrayList<WrappedMessage> wrappedParams) {
            this.params = wrappedParams == null ? null : wrappedParams.stream().map(WrappedMessage::getValue).toArray();
         }

         @ProtoField(number = 1, collectionImplementation = ArrayList.class)
         List<WrappedMessage> getWrappedParams() {
            return Arrays.stream(params).map(WrappedMessage::new).collect(Collectors.toList());
         }

         @Override
         public CustomEvent convert(K key, String previousValue, Metadata previousMetadata, String value,
                                    Metadata metadata, EventType eventType) {
            if (params[0].equals(key))
               return new CustomEvent<>(key, null, 0);

            return new CustomEvent<>(key, value, 0);
         }
      }
   }

   @NamedFactory(name = "raw-static-converter-factory")
   public static class RawStaticConverterFactory implements CacheEventConverterFactory {
      @Override
      public CacheEventConverter<byte[], byte[], byte[]> getConverter(Object[] params) {
         return new RawStaticConverter();
      }

      @ProtoName("RawStaticConverter")
      static class RawStaticConverter implements CacheEventConverter<byte[], byte[], byte[]>, Serializable {
         @Override
         public byte[] convert(byte[] key, byte[] previousValue, Metadata previousMetadata, byte[] value,
               Metadata metadata, EventType eventType) {
            return value != null ? concat(key, value) : key;
         }
      }
   }

   @NamedFactory(name = "filter-converter-factory")
   public static class FilterConverterFactory implements CacheEventFilterConverterFactory {

      @Override
      public CacheEventFilterConverter<Integer, String, CustomEvent> getFilterConverter(Object[] params) {
         return new FilterConverter(params);
      }

      static class FilterConverter extends AbstractCacheEventFilterConverter<Integer, String, CustomEvent> {
         private final Object[] params;

         @ProtoField(number = 1, defaultValue = "0")
         int count;

         FilterConverter(Object[] params) {
            this.params = params;
            this.count = 0;
         }

         @ProtoFactory
         FilterConverter(List<WrappedMessage> wrappedParams, int count) {
            this.params = wrappedParams == null ? null : wrappedParams.stream().map(WrappedMessage::getValue).toArray();
            this.count = count;
         }

         @ProtoField(number = 2, collectionImplementation = ArrayList.class)
         List<WrappedMessage> getWrappedParams() {
            return Arrays.stream(params).map(WrappedMessage::new).collect(Collectors.toList());
         }

         @Override
         public CustomEvent filterAndConvert(Integer key, String oldValue, Metadata oldMetadata,
            String newValue, Metadata newMetadata, EventType eventType) {
            count++;
            if (params[0].equals(key))
               return new CustomEvent<>(key, null, count);

            return new CustomEvent<>(key, newValue, count);
         }
      }
   }

   @ClientListener(filterFactoryName = "filter-converter-factory", converterFactoryName = "filter-converter-factory")
   public static class FilterCustomEventLogListener<K> extends CustomEventLogListener<K, CustomEvent> {
      public FilterCustomEventLogListener(RemoteCache<K, ?> r) { super(r); }
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
