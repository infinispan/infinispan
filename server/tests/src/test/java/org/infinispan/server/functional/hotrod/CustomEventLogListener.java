package org.infinispan.server.functional.hotrod;

import static org.infinispan.server.test.core.Common.assertAnyEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryExpired;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.ClientCacheEntryCustomEvent;
import org.infinispan.client.hotrod.event.ClientEvent;
import org.infinispan.server.functional.extensions.entities.Entities.CustomEvent;

@ClientListener(converterFactoryName = "test-converter-factory")
public abstract class CustomEventLogListener<K, V, E> {
   BlockingQueue<E> createdCustomEvents = new ArrayBlockingQueue<>(128);
   BlockingQueue<E> modifiedCustomEvents = new ArrayBlockingQueue<>(128);
   BlockingQueue<E> removedCustomEvents = new ArrayBlockingQueue<>(128);
   BlockingQueue<E> expiredCustomEvents = new ArrayBlockingQueue<>(128);

   private final RemoteCache<K, V> remote;

   protected CustomEventLogListener(RemoteCache<K, V> remote) {
      this.remote = remote;
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
         case CLIENT_CACHE_ENTRY_CREATED:
            return createdCustomEvents;
         case CLIENT_CACHE_ENTRY_MODIFIED:
            return modifiedCustomEvents;
         case CLIENT_CACHE_ENTRY_REMOVED:
            return removedCustomEvents;
         case CLIENT_CACHE_ENTRY_EXPIRED:
            return expiredCustomEvents;
         default:
            throw new IllegalArgumentException("Unknown event type: " + type);
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

   public void accept(BiConsumer<CustomEventLogListener<K, V, E>, RemoteCache<K, V>> cons) {
      remote.addClientListener(this);
      try {
         cons.accept(this, remote);
      } finally {
         remote.removeClientListener(this);
      }
   }

   public void accept(Object[] fparams, Object[] cparams, BiConsumer<CustomEventLogListener<K, V, E>, RemoteCache<K, V>> cons) {
      remote.addClientListener(this, fparams, cparams);
      try {
         cons.accept(this, remote);
      } finally {
         remote.removeClientListener(this);
      }
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

   public void expectOrderedEventQueue(ClientEvent.Type type) {
      // NO OP
   }

   @ClientListener(converterFactoryName = "static-converter-factory")
   public static class StaticCustomEventLogListener<K, V> extends CustomEventLogListener<K, V, CustomEvent<K>> {
      public StaticCustomEventLogListener(RemoteCache<K, V> r) {
         super(r);
      }

      @Override
      public void expectSingleCustomEvent(ClientEvent.Type type, CustomEvent<K> expected) {
         CustomEvent<K> event = pollEvent(type);
         assertNotNull(event.getKey());
         assertAnyEquals(expected, event);
      }

      @Override
      public void expectOrderedEventQueue(ClientEvent.Type type) {
         BlockingQueue<CustomEvent<K>> queue = queue(type);
         if (queue.size() < 2)
            return;

         try {
            CustomEvent<K> before = queue.poll(10, TimeUnit.SECONDS);
            for (CustomEvent<K> after : queue) {
               expectTimeOrdered(before, after);
               before = after;
            }
         } catch (InterruptedException e) {
            throw new AssertionError(e);
         }
      }

      private void expectTimeOrdered(CustomEvent<K> before, CustomEvent<K> after) {
         assertTrue(before.getTimestamp() < after.getTimestamp(),
               "Before timestamp=" + before.getTimestamp() + ", after timestamp=" + after.getTimestamp());
      }
   }

   @ClientListener(converterFactoryName = "raw-static-converter-factory", useRawData = true)
   public static class RawStaticCustomEventLogListener<K, V> extends CustomEventLogListener<K, V, byte[]> {
      public RawStaticCustomEventLogListener(RemoteCache<K, V> r) {
         super(r);
      }
   }

   @ClientListener(converterFactoryName = "static-converter-factory", includeCurrentState = true)
   public static class StaticCustomEventLogWithStateListener<K, V> extends CustomEventLogListener<K, V, CustomEvent<K>> {
      public StaticCustomEventLogWithStateListener(RemoteCache<K, V> r) {
         super(r);
      }
   }

   @ClientListener(converterFactoryName = "dynamic-converter-factory")
   public static class DynamicCustomEventLogListener<K, V> extends CustomEventLogListener<K, V, CustomEvent<K>> {
      public DynamicCustomEventLogListener(RemoteCache<K, V> r) {
         super(r);
      }
   }

   @ClientListener(converterFactoryName = "dynamic-converter-factory", includeCurrentState = true)
   public static class DynamicCustomEventWithStateLogListener<K, V> extends CustomEventLogListener<K, V, CustomEvent<K>> {
      public DynamicCustomEventWithStateLogListener(RemoteCache<K, V> r) {
         super(r);
      }
   }

   @ClientListener(converterFactoryName = "simple-converter-factory")
   public static class SimpleListener<K, V> extends CustomEventLogListener<K, V, String> {
      public SimpleListener(RemoteCache<K, V> r) {
         super(r);
      }
   }

   @ClientListener(filterFactoryName = "filter-converter-factory", converterFactoryName = "filter-converter-factory")
   public static class FilterCustomEventLogListener<K, V> extends CustomEventLogListener<K, V, CustomEvent> {
      public FilterCustomEventLogListener(RemoteCache<K, V> r) {
         super(r);
      }
   }

   @ClientListener
   public static class NoConverterFactoryListener<K, V> extends CustomEventLogListener<K, V, Object> {
      public NoConverterFactoryListener(RemoteCache<K, V> r) {
         super(r);
      }
   }
}
