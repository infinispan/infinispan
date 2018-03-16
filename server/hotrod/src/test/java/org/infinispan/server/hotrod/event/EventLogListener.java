package org.infinispan.server.hotrod.event;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertByteArrayEquals;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.server.hotrod.test.TestClientListener;
import org.infinispan.server.hotrod.test.TestCustomEvent;
import org.infinispan.server.hotrod.test.TestKeyEvent;
import org.infinispan.server.hotrod.test.TestKeyWithVersionEvent;
import org.infinispan.test.TestException;

/**
 * @author Galder Zamarreño
 */
public class EventLogListener extends TestClientListener {
   private ArrayBlockingQueue<TestKeyWithVersionEvent>
         createdEvents = new ArrayBlockingQueue<>(128);
   private ArrayBlockingQueue<TestKeyWithVersionEvent>
         modifiedEvents = new ArrayBlockingQueue<>(128);
   private ArrayBlockingQueue<TestKeyEvent> removedEvents = new ArrayBlockingQueue<>(128);
   private ArrayBlockingQueue<TestCustomEvent> customEvents = new ArrayBlockingQueue<>(128);

   @Override
   public int queueSize(Event.Type eventType) {
      return queue(eventType).size();
   }

   @Override
   public Object pollEvent(Event.Type eventType) {
      try {
         return queue(eventType).poll(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new TestException();
      }
   }

   private <T> BlockingQueue<T> queue(Event.Type eventType) {
      BlockingQueue<?> eventQueue;
      switch (eventType) {
         case CACHE_ENTRY_CREATED:
            eventQueue = createdEvents;
            break;
         case CACHE_ENTRY_MODIFIED:
            eventQueue = modifiedEvents;
            break;
         case CACHE_ENTRY_REMOVED:
            eventQueue = removedEvents;
            break;
         default:
            throw new IllegalStateException("Unexpected event type: " + eventType);
      }
      return (BlockingQueue<T>) eventQueue;
   }

   @Override
   public void onCreated(TestKeyWithVersionEvent event) {
      createdEvents.add(event);
   }

   @Override
   public void onModified(TestKeyWithVersionEvent event) {
      modifiedEvents.add(event);
   }

   @Override
   public void onRemoved(TestKeyEvent event) {
      removedEvents.add(event);
   }

   @Override
   public int customQueueSize() {
      return customEvents.size();
   }

   @Override
   public TestCustomEvent pollCustom() {
      try {
         return customEvents.poll(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new TestException();
      }
   }

   @Override
   public void onCustom(TestCustomEvent event) {
      customEvents.add(event);
   }

   @Override
   public byte[] getId() {
      return new byte[]{1, 2, 3};
   }

   public void expectNoEvents(Optional<Event.Type> eventType) {
      if (eventType.isPresent()) {
         assertEquals(0, queueSize(eventType.get()));
      } else {
         assertEquals(0, queueSize(Event.Type.CACHE_ENTRY_CREATED));
         assertEquals(0, queueSize(Event.Type.CACHE_ENTRY_MODIFIED));
         assertEquals(0, queueSize(Event.Type.CACHE_ENTRY_REMOVED));
         assertEquals(0, customQueueSize());
      }
   }

   public void expectOnlyRemovedEvent(Cache cache, byte[] k) {
      expectSingleEvent(cache, k, Event.Type.CACHE_ENTRY_REMOVED);
      expectNoEvents(Optional.of(Event.Type.CACHE_ENTRY_CREATED));
      expectNoEvents(Optional.of(Event.Type.CACHE_ENTRY_MODIFIED));
   }

   public void expectOnlyModifiedEvent(Cache cache, byte[] k) {
      expectSingleEvent(cache, k, Event.Type.CACHE_ENTRY_MODIFIED);
      expectNoEvents(Optional.of(Event.Type.CACHE_ENTRY_CREATED));
      expectNoEvents(Optional.of(Event.Type.CACHE_ENTRY_REMOVED));
   }

   public void expectOnlyCreatedEvent(Cache cache, byte[] k) {
      expectSingleEvent(cache, k, Event.Type.CACHE_ENTRY_CREATED);
      expectNoEvents(Optional.of(Event.Type.CACHE_ENTRY_MODIFIED));
      expectNoEvents(Optional.of(Event.Type.CACHE_ENTRY_REMOVED));
   }

   public void expectSingleEvent(Cache cache, byte[] k, Event.Type eventType) {
      expectEvent(cache, k, eventType);
      assertEquals(0, queueSize(eventType));
   }

   public void expectEvent(Cache cache, byte[] k, Event.Type eventType) {
      Object event = pollEvent(eventType);
      assertNotNull(event);
      if (event instanceof TestKeyWithVersionEvent) {
         TestKeyWithVersionEvent t = (TestKeyWithVersionEvent) event;
         assertByteArrayEquals(k, t.key);
         assertEquals(serverDataVersion(k, cache), t.dataVersion);
      } else if (event instanceof TestKeyEvent) {
         assertByteArrayEquals(k, ((TestKeyEvent) event).key);
      }
   }

   public void expectUnorderedEvents(Cache cache, Collection<byte[]> keys, Event.Type eventType) {
      List<byte[]> assertedKeys = new ArrayList<>();

      for (int i = 0; i < keys.size(); i++) {
         Object event = pollEvent(eventType);
         assertNotNull(event);
         int initialSize = assertedKeys.size();
         keys.forEach(key -> {
            if (event instanceof TestKeyWithVersionEvent) {
               TestKeyWithVersionEvent t = (TestKeyWithVersionEvent) event;
               boolean keyMatched = checkUnorderedKeyEvent(assertedKeys, key, t.key);
               if (keyMatched)
                  assertEquals(serverDataVersion(key, cache), t.dataVersion);
            } else if (event instanceof TestKeyEvent) {
               checkUnorderedKeyEvent(assertedKeys, key, ((TestKeyEvent) event).key);
            }
         });
         int finalSize = assertedKeys.size();
         assertEquals(initialSize + 1, finalSize);
      }
   }

   private boolean checkUnorderedKeyEvent(List<byte[]> assertedKeys, byte[] key, byte[] eventKey) {
      if (java.util.Arrays.equals(key, eventKey)) {
         assertFalse(assertedKeys.contains(key));
         assertedKeys.add(key);
         return true;
      } else {
         return false;
      }
   }

   public void expectSingleCustomEvent(Cache cache, byte[] eventData) {
      TestCustomEvent event = pollCustom();
      assertNotNull(event);
      assertByteArrayEquals(eventData, event.eventData);
      int remaining = customQueueSize();
      assertEquals(0, remaining);
   }

   private long serverDataVersion(byte[] k, Cache cache) {
      CacheEntry cacheEntry = cache.getAdvancedCache().getCacheEntry(k);
      Metadata metadata = cacheEntry.getMetadata();
      EntryVersion version = metadata.version();
      return ((NumericVersion) version).getVersion();
   }

}
