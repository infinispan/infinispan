package org.infinispan.client.hotrod.event;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientCacheFailover;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.notifications.cachelistener.filter.NamedFactory;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilter;
import org.infinispan.notifications.cachelistener.filter.CacheEventFilterFactory;
import org.infinispan.notifications.cachelistener.filter.EventType;
import org.junit.Assert;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.*;

@ClientListener
public class EventLogListener<K> {
   public BlockingQueue<ClientCacheEntryCreatedEvent> createdEvents =
         new ArrayBlockingQueue<ClientCacheEntryCreatedEvent>(128);
   public BlockingQueue<ClientCacheEntryModifiedEvent> modifiedEvents =
         new ArrayBlockingQueue<ClientCacheEntryModifiedEvent>(128);
   public BlockingQueue<ClientCacheEntryRemovedEvent> removedEvents =
         new ArrayBlockingQueue<ClientCacheEntryRemovedEvent>(128);
   public BlockingQueue<ClientCacheFailoverEvent> failoverEvents =
         new ArrayBlockingQueue<ClientCacheFailoverEvent>(128);

   private final boolean compatibility;

   public EventLogListener(boolean compatibility) {
      this.compatibility = compatibility;
   }

   public EventLogListener() {
      this.compatibility = false;
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
         case CLIENT_CACHE_FAILOVER: return (BlockingQueue<E>) failoverEvents;
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

   @ClientCacheFailover @SuppressWarnings("unused")
   public void handleFailover(ClientCacheFailoverEvent e) {
      failoverEvents.add(e);
   }

   public void expectNoEvents() {
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED);
   }

   public void expectNoEvents(ClientEvent.Type type) {
      switch (type) {
         case CLIENT_CACHE_ENTRY_CREATED:
            assertEquals(0, createdEvents.size());
            break;
         case CLIENT_CACHE_ENTRY_MODIFIED:
            assertEquals(0, modifiedEvents.size());
            break;
         case CLIENT_CACHE_ENTRY_REMOVED:
            assertEquals(0, removedEvents.size());
            break;
      }
   }

   public void expectOnlyCreatedEvent(K key, Cache cache) {
      expectSingleEvent(key, ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED, cache);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED);
   }

   public void expectOnlyModifiedEvent(K key, Cache cache) {
      expectSingleEvent(key, ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED, cache);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED);
   }

   public void expectOnlyRemovedEvent(K key, Cache cache) {
      expectSingleEvent(key, ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED, cache);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED);
      expectNoEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_MODIFIED);
   }

   public void expectSingleEvent(K key, ClientEvent.Type type, Cache cache) {
      switch (type) {
         case CLIENT_CACHE_ENTRY_CREATED:
            ClientCacheEntryCreatedEvent createdEvent = pollEvent(type);
            Assert.assertEquals(key, createdEvent.getKey());
            assertEquals(serverDataVersion(cache, key), createdEvent.getVersion());
            break;
         case CLIENT_CACHE_ENTRY_MODIFIED:
            ClientCacheEntryModifiedEvent modifiedEvent = pollEvent(type);
            Assert.assertEquals(key, modifiedEvent.getKey());
            assertEquals(serverDataVersion(cache, key), modifiedEvent.getVersion());
            break;
         case CLIENT_CACHE_ENTRY_REMOVED:
            ClientCacheEntryRemovedEvent removedEvent = pollEvent(type);
            Assert.assertEquals(key, removedEvent.getKey());
            break;
      }
      Assert.assertEquals(0, queue(type).size());
   }

   private long serverDataVersion(Cache<Object, Object> cache, K key) {
      Object lookupKey;
      try {
         lookupKey = compatibility ? key : new GenericJBossMarshaller().objectToByteBuffer(key);
      } catch (Exception e) {
         throw new AssertionError(e);
      }

      Metadata meta = cache.getAdvancedCache().getCacheEntry(lookupKey).getMetadata();
      return ((NumericVersion) meta.version()).getVersion();
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
      public StaticFilteredEventLogListener() {}
      public StaticFilteredEventLogListener(boolean compatibility) { super(compatibility); }
   }

   @ClientListener(filterFactoryName = "static-filter-factory", includeCurrentState = true)
   public static class StaticFilteredEventLogWithStateListener<K> extends EventLogListener<K> {
      public StaticFilteredEventLogWithStateListener() {}
      public StaticFilteredEventLogWithStateListener(boolean compatibility) { super(compatibility); }
   }

   @ClientListener(filterFactoryName = "dynamic-filter-factory")
   public static class DynamicFilteredEventLogListener<K> extends EventLogListener<K> {
      public DynamicFilteredEventLogListener() {}
      public DynamicFilteredEventLogListener(boolean compatibility) { super(compatibility); }
   }

   @ClientListener(filterFactoryName = "dynamic-filter-factory", includeCurrentState = true)
   public static class DynamicFilteredEventLogWithStateListener<K> extends EventLogListener<K> {
      public DynamicFilteredEventLogWithStateListener() {}
      public DynamicFilteredEventLogWithStateListener(boolean compatibility) { super(compatibility); }
   }

   @NamedFactory(name = "static-filter-factory")
   public static class StaticCacheEventFilterFactory implements CacheEventFilterFactory {
      @Override
      public CacheEventFilter<Integer, String> getFilter(final Object[] params) {
         return new StaticCacheEventFilter();
      }

      static class StaticCacheEventFilter implements CacheEventFilter<Integer, String>, Serializable {
         final Integer staticKey = 2;
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


}