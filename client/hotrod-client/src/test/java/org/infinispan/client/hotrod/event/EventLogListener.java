package org.infinispan.client.hotrod.event;

import static org.infinispan.test.TestingUtil.assertAnyEquals;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryExpired;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.filter.NamedFactory;
import org.infinispan.marshall.core.ExternalPojo;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.infinispan.notifications.cachelistener.filter.EventType;

@ClientListener
public class EventLogListener<K> implements RemoteCacheSupplier<K> {
   public BlockingQueue<ClientCacheEntryCreatedEvent> createdEvents = new ArrayBlockingQueue<>(128);
   public BlockingQueue<ClientCacheEntryModifiedEvent> modifiedEvents = new ArrayBlockingQueue<>(128);
   public BlockingQueue<ClientCacheEntryRemovedEvent> removedEvents = new ArrayBlockingQueue<>(128);
   public BlockingQueue<ClientCacheEntryExpiredEvent> expiredEvents = new ArrayBlockingQueue<>(128);

   private final RemoteCache<K, ?> remote;

   public EventLogListener(RemoteCache<K, ?> remote) {
      this.remote = remote;
   }

   @Override
   @SuppressWarnings("unchecked")
   public <V> RemoteCache<K, V> get() {
      return (RemoteCache<K, V>) remote;
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
   public void handleCreatedEvent(ClientCacheEntryCreatedEvent e) {
      createdEvents.add(e);
   }

   @ClientCacheEntryModified @SuppressWarnings("unused")
   public void handleModifiedEvent(ClientCacheEntryModifiedEvent e) {
      modifiedEvents.add(e);
   }

   @ClientCacheEntryRemoved @SuppressWarnings("unused")
   public void handleRemovedEvent(ClientCacheEntryRemovedEvent e) {
      removedEvents.add(e);
   }

   @ClientCacheEntryExpired @SuppressWarnings("unused")
   public void handleExpiriedEvent(ClientCacheEntryExpiredEvent e) {
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
            assertEquals(createdEvents.toString(), 0, createdEvents.size());
            break;
         case CLIENT_CACHE_ENTRY_MODIFIED:
            assertEquals(modifiedEvents.toString(), 0, modifiedEvents.size());
            break;
         case CLIENT_CACHE_ENTRY_REMOVED:
            assertEquals(removedEvents.toString(), 0, removedEvents.size());
            break;
         case CLIENT_CACHE_ENTRY_EXPIRED:
            assertEquals(expiredEvents.toString(), 0, expiredEvents.size());
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
            ClientCacheEntryCreatedEvent createdEvent = pollEvent(type);
            assertAnyEquals(key, createdEvent.getKey());
            assertAnyEquals(serverDataVersion(remote, key), createdEvent.getVersion());
            break;
         case CLIENT_CACHE_ENTRY_MODIFIED:
            ClientCacheEntryModifiedEvent modifiedEvent = pollEvent(type);
            assertAnyEquals(key, modifiedEvent.getKey());
            assertAnyEquals(serverDataVersion(remote, key), modifiedEvent.getVersion());
            break;
         case CLIENT_CACHE_ENTRY_REMOVED:
            ClientCacheEntryRemovedEvent removedEvent = pollEvent(type);
            assertAnyEquals(key, removedEvent.getKey());
            break;
         case CLIENT_CACHE_ENTRY_EXPIRED:
            ClientCacheEntryExpiredEvent expiredEvent = pollEvent(type);
            assertAnyEquals(key, expiredEvent.getKey());
            break;
      }
      assertEquals(0, queue(type).size());
   }

   private long serverDataVersion(RemoteCache<K, ?> cache, K key) {
      long v1 = cache.getVersioned(key).getVersion();
      long v2 = cache.getWithMetadata(key).getVersion();
      assertEquals(v1, v2);
      return v1;
   }

   public void expectUnorderedEvents(ClientEvent.Type type, K... keys) {
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

   @ClientListener(filterFactoryName = "static-filter-factory")
   public static class StaticFilteredEventLogListener<K> extends EventLogListener<K> {
      public StaticFilteredEventLogListener(RemoteCache<K, ?> r) { super(r); }
   }

   @ClientListener(filterFactoryName = "raw-static-filter-factory", useRawData = true)
   public static class RawStaticFilteredEventLogListener<K> extends EventLogListener<K> {
      public RawStaticFilteredEventLogListener(RemoteCache<K, ?> r) { super(r); }
   }

   @ClientListener(filterFactoryName = "static-filter-factory", includeCurrentState = true)
   public static class StaticFilteredEventLogWithStateListener<K> extends EventLogListener<K> {
      public StaticFilteredEventLogWithStateListener(RemoteCache<K, ?> r) { super(r); }
   }

   @ClientListener(filterFactoryName = "dynamic-filter-factory")
   public static class DynamicFilteredEventLogListener<K> extends EventLogListener<K> {
      public DynamicFilteredEventLogListener(RemoteCache<K, ?> r) { super(r); }
   }

   @ClientListener(filterFactoryName = "dynamic-filter-factory", includeCurrentState = true)
   public static class DynamicFilteredEventLogWithStateListener<K> extends EventLogListener<K> {
      public DynamicFilteredEventLogWithStateListener(RemoteCache<K, ?> r) { super(r); }
   }

   @NamedFactory(name = "static-filter-factory")
   public static class StaticCacheEventFilterFactory implements CacheEventFilterFactory {
      private final int staticKey;

      public StaticCacheEventFilterFactory(int staticKey) {
         this.staticKey = staticKey;
      }

      @Override
      public CacheEventFilter<Integer, String> getFilter(final Object[] params) {
         return new StaticCacheEventFilter(staticKey);
      }

      static class StaticCacheEventFilter implements CacheEventFilter<Integer, String>, Serializable, ExternalPojo {
         final Integer staticKey;

         StaticCacheEventFilter(Integer staticKey) {
            this.staticKey = staticKey;
         }

         @Override
         public boolean accept(Integer key, String previousValue, Metadata previousMetadata, String value,
                               Metadata metadata, EventType eventType) {
            return staticKey.equals(key);
         }
      }
   }

   @NamedFactory(name = "dynamic-filter-factory")
   public static class DynamicCacheEventFilterFactory implements CacheEventFilterFactory {
      @Override
      public CacheEventFilter<Integer, String> getFilter(final Object[] params) {
         return new DynamicCacheEventFilter(params);
      }

      static class DynamicCacheEventFilter implements CacheEventFilter<Integer, String>, Serializable {
         private final Object[] params;

         public DynamicCacheEventFilter(Object[] params) {
            this.params = params;
         }

         @Override
         public boolean accept(Integer key, String previousValue, Metadata previousMetadata, String value,
                               Metadata metadata, EventType eventType) {
            return params[0].equals(key); // dynamic
         }
      }
   }

   @NamedFactory(name = "raw-static-filter-factory")
   public static class RawStaticCacheEventFilterFactory implements CacheEventFilterFactory {
      @Override
      public CacheEventFilter<byte[], byte[]> getFilter(final Object[] params) {
         return new RawStaticCacheEventFilter();
      }

      static class RawStaticCacheEventFilter implements CacheEventFilter<byte[], byte[]>, Serializable {
         final byte[] staticKey = new byte[]{3, 75, 0, 0, 0, 2}; // key integer `2`, as marshalled by GenericJBossMarshaller
         @Override
         public boolean accept(byte[] key, byte[] previousValue, Metadata previousMetadata, byte[] value,
               Metadata metadata, EventType eventType) {
            return Arrays.equals(key, staticKey);
         }
      }
   }

}
