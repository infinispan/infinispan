package org.infinispan.server.functional.hotrod;

import static org.infinispan.server.test.core.Common.assertAnyEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.ArrayList;
import java.util.List;
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
import org.infinispan.client.hotrod.event.ClientCacheEntryCreatedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryExpiredEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryModifiedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryRemovedEvent;
import org.infinispan.client.hotrod.event.ClientEvent;

@ClientListener
public class EventLogListener<K, V> {
   public BlockingQueue<ClientCacheEntryCreatedEvent<K>> createdEvents = new ArrayBlockingQueue<>(128);
   public BlockingQueue<ClientCacheEntryModifiedEvent<K>> modifiedEvents = new ArrayBlockingQueue<>(128);
   public BlockingQueue<ClientCacheEntryRemovedEvent<K>> removedEvents = new ArrayBlockingQueue<>(128);
   public BlockingQueue<ClientCacheEntryExpiredEvent<K>> expiredEvents = new ArrayBlockingQueue<>(128);

   private final RemoteCache<K, V> remote;

   public EventLogListener(RemoteCache<K, V> remote) {
      this.remote = remote;
   }

   @SuppressWarnings("unchecked")
   public <E extends ClientEvent> E pollEvent(ClientEvent.Type type) {
      try {
         E event = (E) queue(type).poll(10, TimeUnit.SECONDS);
         assertNotNull(event);
         return event;
      } catch (InterruptedException e) {
         throw new AssertionError(e);
      }
   }

   @SuppressWarnings("unchecked")
   public <E extends ClientEvent> BlockingQueue<E> queue(ClientEvent.Type type) {
      switch (type) {
         case CLIENT_CACHE_ENTRY_CREATED: return (BlockingQueue<E>) createdEvents;
         case CLIENT_CACHE_ENTRY_MODIFIED: return (BlockingQueue<E>) modifiedEvents;
         case CLIENT_CACHE_ENTRY_REMOVED: return (BlockingQueue<E>) removedEvents;
         case CLIENT_CACHE_ENTRY_EXPIRED: return (BlockingQueue<E>) expiredEvents;
         default: throw new IllegalArgumentException("Unknown event type: " + type);
      }
   }

   @ClientCacheEntryCreated
   @SuppressWarnings("unused")
   public void handleCreatedEvent(ClientCacheEntryCreatedEvent<K> e) {
      createdEvents.add(e);
   }

   @ClientCacheEntryModified @SuppressWarnings("unused")
   public void handleModifiedEvent(ClientCacheEntryModifiedEvent<K> e) {
      modifiedEvents.add(e);
   }

   @ClientCacheEntryRemoved @SuppressWarnings("unused")
   public void handleRemovedEvent(ClientCacheEntryRemovedEvent<K> e) {
      removedEvents.add(e);
   }

   @ClientCacheEntryExpired @SuppressWarnings("unused")
   public void handleExpiredEvent(ClientCacheEntryExpiredEvent<K> e) {
      expiredEvents.add(e);
   }

   public void expectNoEvents() {
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_EXPIRED);
   }

   public void expectNoEvents(ClientEvent.Type type) {
      switch (type) {
         case CLIENT_CACHE_ENTRY_CREATED:
            assertEquals(0, createdEvents.size(), createdEvents.toString());
            break;
         case CLIENT_CACHE_ENTRY_MODIFIED:
            assertEquals(0, modifiedEvents.size(), modifiedEvents.toString());
            break;
         case CLIENT_CACHE_ENTRY_REMOVED:
            assertEquals(0, removedEvents.size(), removedEvents.toString());
            break;
         case CLIENT_CACHE_ENTRY_EXPIRED:
            assertEquals(0, expiredEvents.size(), expiredEvents.toString());
            break;
      }
   }

   public void expectOnlyCreatedEvent(K key) {
      expectSingleEvent(key, ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_EXPIRED);
   }

   public void expectOnlyModifiedEvent(K key) {
      expectSingleEvent(key, ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_EXPIRED);
   }

   public void expectOnlyRemovedEvent(K key) {
      expectSingleEvent(key, ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_EXPIRED);
   }

   public void expectOnlyExpiredEvent(K key) {
      expectSingleEvent(key, ClientEvent.Type.CLIENT_CACHE_ENTRY_EXPIRED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED);
   }

   public void expectSingleEvent(K key, ClientEvent.Type type) {
      switch (type) {
         case CLIENT_CACHE_ENTRY_CREATED:
            ClientCacheEntryCreatedEvent<K> createdEvent = pollEvent(type);
            assertAnyEquals(key, createdEvent.getKey());
            assertAnyEquals(serverDataVersion(remote, key), createdEvent.getVersion());
            break;
         case CLIENT_CACHE_ENTRY_MODIFIED:
            ClientCacheEntryModifiedEvent<K> modifiedEvent = pollEvent(type);
            assertAnyEquals(key, modifiedEvent.getKey());
            assertAnyEquals(serverDataVersion(remote, key), modifiedEvent.getVersion());
            break;
         case CLIENT_CACHE_ENTRY_REMOVED:
            ClientCacheEntryRemovedEvent<K> removedEvent = pollEvent(type);
            assertAnyEquals(key, removedEvent.getKey());
            break;
         case CLIENT_CACHE_ENTRY_EXPIRED:
            ClientCacheEntryExpiredEvent<K> expiredEvent = pollEvent(type);
            assertAnyEquals(key, expiredEvent.getKey());
            break;
      }
      assertEquals(0, queue(type).size());
   }

   private long serverDataVersion(RemoteCache<K, ?> cache, K key) {
      return cache.getWithMetadata(key).getVersion();
   }

   @SafeVarargs
   public final void expectUnorderedEvents(ClientEvent.Type type, K... keys) {
      List<K> assertedKeys = new ArrayList<>();
      for (int i = 0; i < keys.length; i++) {
         ClientEvent event = pollEvent(type);
         int initialSize = assertedKeys.size();
         for (K key : keys) {
            K eventKey = null;
            switch (event.getType()) {
               case CLIENT_CACHE_ENTRY_CREATED:
                  eventKey = ((ClientCacheEntryCreatedEvent<K>) event).getKey();
                  break;
               case CLIENT_CACHE_ENTRY_MODIFIED:
                  eventKey = ((ClientCacheEntryModifiedEvent<K>) event).getKey();
                  break;
               case CLIENT_CACHE_ENTRY_REMOVED:
                  eventKey = ((ClientCacheEntryRemovedEvent<K>) event).getKey();
                  break;
               case CLIENT_CACHE_ENTRY_EXPIRED:
                  eventKey = ((ClientCacheEntryExpiredEvent<K>) event).getKey();
                  break;
            }
            checkUnorderedKeyEvent(assertedKeys, key, eventKey);
         }
         int finalSize = assertedKeys.size();
         assertEquals(initialSize + 1, finalSize);
      }
   }

   private boolean checkUnorderedKeyEvent(List<K> assertedKeys, K key, K eventKey) {
      if (key.equals(eventKey)) {
         assertFalse(assertedKeys.contains(key));
         assertedKeys.add(key);
         return true;
      }
      return false;
   }

   public void expectFailoverEvent() {
      pollEvent(ClientEvent.Type.CLIENT_CACHE_FAILOVER);
   }

   public void accept(BiConsumer<EventLogListener<K, V>, RemoteCache<K, V>> cons) {
      remote.addClientListener(this);
      try {
         cons.accept(this, remote);
      } finally {
         remote.removeClientListener(this);
      }
   }

   public void accept(Object[] fparams, Object[] cparams, BiConsumer<EventLogListener<K, V>, RemoteCache<K, V>> cons) {
      remote.addClientListener(this, fparams, cparams);
      try {
         cons.accept(this, remote);
      } finally {
         remote.removeClientListener(this);
      }
   }

   @ClientListener(filterFactoryName = "static-filter-factory")
   public static class StaticFilteredEventLogListener<K, V> extends EventLogListener<K, V> {
      public StaticFilteredEventLogListener(RemoteCache<K, V> r) { super(r); }
   }

   @ClientListener(filterFactoryName = "raw-static-filter-factory", useRawData = true)
   public static class RawStaticFilteredEventLogListener<K, V> extends EventLogListener<K, V> {
      public RawStaticFilteredEventLogListener(RemoteCache<K, V> r) { super(r); }
   }

   @ClientListener(filterFactoryName = "static-filter-factory", includeCurrentState = true)
   public static class StaticFilteredEventLogWithStateListener<K, V> extends EventLogListener<K, V> {
      public StaticFilteredEventLogWithStateListener(RemoteCache<K, V> r) { super(r); }
   }

   @ClientListener(filterFactoryName = "dynamic-filter-factory")
   public static class DynamicFilteredEventLogListener<K, V> extends EventLogListener<K, V> {
      public DynamicFilteredEventLogListener(RemoteCache<K, V> r) { super(r); }
   }

   @ClientListener(filterFactoryName = "dynamic-filter-factory", includeCurrentState = true)
   public static class DynamicFilteredEventLogWithStateListener<K, V> extends EventLogListener<K, V> {
      public DynamicFilteredEventLogWithStateListener(RemoteCache<K, V> r) { super(r); }
   }

   @ClientListener(includeCurrentState = true)
   public static class WithStateEventLogListener<K, V> extends EventLogListener<K, V> {
      public WithStateEventLogListener(RemoteCache<K, V> remote) {
         super(remote);
      }
   }

   @ClientListener(filterFactoryName = "non-existing-test-filter-factory")
   public static class NonExistingFilterFactoryListener<K, V> extends EventLogListener<K, V> {
      public NonExistingFilterFactoryListener(RemoteCache<K, V> r) {
         super(r);
      }
   }
}
