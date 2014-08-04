package org.infinispan.server.test.client.hotrod;

import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientCacheFailover;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.event.ClientCacheEntryCreatedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryModifiedEvent;
import org.infinispan.client.hotrod.event.ClientCacheEntryRemovedEvent;
import org.infinispan.client.hotrod.event.ClientCacheFailoverEvent;
import org.infinispan.client.hotrod.event.ClientEvent;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

@ClientListener
public class EventLogListener {
   public BlockingQueue<ClientCacheEntryCreatedEvent> createdEvents =
         new ArrayBlockingQueue<ClientCacheEntryCreatedEvent>(128);
   public BlockingQueue<ClientCacheEntryModifiedEvent> modifiedEvents =
         new ArrayBlockingQueue<ClientCacheEntryModifiedEvent>(128);
   public BlockingQueue<ClientCacheEntryRemovedEvent> removedEvents =
         new ArrayBlockingQueue<ClientCacheEntryRemovedEvent>(128);
   public BlockingQueue<ClientCacheFailoverEvent> failoverEvents =
         new ArrayBlockingQueue<ClientCacheFailoverEvent>(128);

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

}