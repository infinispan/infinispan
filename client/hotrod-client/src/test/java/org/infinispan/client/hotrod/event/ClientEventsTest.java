package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.test.RemoteCacheManagerCallable;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.Set;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withClientListener;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * @author Galder Zamarreño
 */
@Test(groups = "functional", testName = "client.hotrod.event.ClientEventsTest")
public class ClientEventsTest extends SingleHotRodServerTest {

   public void testCreatedEvent() {
      final EventLogListener<Integer> eventListener = new EventLogListener<>();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<Integer, String> cache = rcm.getCache();
            eventListener.expectNoEvents();
            cache.put(1, "one");
            eventListener.expectOnlyCreatedEvent(1, cache());
            cache.put(2, "two");
            eventListener.expectOnlyCreatedEvent(2, cache());
         }
      });
   }

   public void testModifiedEvent() {
      final EventLogListener<Integer> eventListener = new EventLogListener<>();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<Integer, String> cache = rcm.getCache();
            eventListener.expectNoEvents();
            cache.put(1, "one");
            eventListener.expectOnlyCreatedEvent(1, cache());
            cache.put(1, "newone");
            eventListener.expectOnlyModifiedEvent(1, cache());
         }
      });
   }

   public void testRemovedEvent() {
      final EventLogListener<Integer> eventListener = new EventLogListener<>();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<Integer, String> cache = rcm.getCache();
            eventListener.expectNoEvents();
            cache.remove(1);
            eventListener.expectNoEvents();
            cache.put(1, "one");
            eventListener.expectOnlyCreatedEvent(1, cache());
            cache.remove(1);
            eventListener.expectOnlyRemovedEvent(1, cache());
         }
      });
   }

   public void testReplaceEvents() {
      final EventLogListener<Integer> eventListener = new EventLogListener<>();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<Integer, String> cache = rcm.getCache();
            eventListener.expectNoEvents();
            cache.replace(1, "one");
            eventListener.expectNoEvents();
            cache.put(1, "one");
            eventListener.expectOnlyCreatedEvent(1, cache());
            cache.replace(1, "newone");
            eventListener.expectOnlyModifiedEvent(1, cache());
         }
      });
   }

   public void testPutIfAbsentEvents() {
      final EventLogListener<Integer> eventListener = new EventLogListener<>();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<Integer, String> cache = rcm.getCache();
            eventListener.expectNoEvents();
            cache.putIfAbsent(1, "one");
            eventListener.expectOnlyCreatedEvent(1, cache());
            cache.putIfAbsent(1, "newone");
            eventListener.expectNoEvents();
         }
      });
   }

   public void testReplaceIfUnmodifiedEvents() {
      final EventLogListener<Integer> eventListener = new EventLogListener<>();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<Integer, String> cache = rcm.getCache();
            eventListener.expectNoEvents();
            cache.replaceWithVersion(1, "one", 0);
            eventListener.expectNoEvents();
            cache.putIfAbsent(1, "one");
            eventListener.expectOnlyCreatedEvent(1, cache());
            cache.replaceWithVersion(1, "one", 0);
            eventListener.expectNoEvents();
            VersionedValue<String> versioned = cache.getVersioned(1);
            cache.replaceWithVersion(1, "one", versioned.getVersion());
            eventListener.expectOnlyModifiedEvent(1, cache());
         }
      });
   }

   public void testRemoveIfUnmodifiedEvents() {
      final EventLogListener<Integer> eventListener = new EventLogListener<>();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<Integer, String> cache = rcm.getCache();
            eventListener.expectNoEvents();
            cache.removeWithVersion(1, 0);
            eventListener.expectNoEvents();
            cache.putIfAbsent(1, "one");
            eventListener.expectOnlyCreatedEvent(1, cache());
            cache.removeWithVersion(1, 0);
            eventListener.expectNoEvents();
            VersionedValue<String> versioned = cache.getVersioned(1);
            cache.removeWithVersion(1, versioned.getVersion());
            eventListener.expectOnlyRemovedEvent(1, cache());
         }
      });
   }

   public void testClearEvents() {
      final EventLogListener<Integer> eventListener = new EventLogListener<>();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<Integer, String> cache = rcm.getCache();
            eventListener.expectNoEvents();
            cache.put(1, "one");
            eventListener.expectOnlyCreatedEvent(1, cache());
            cache.put(2, "two");
            eventListener.expectOnlyCreatedEvent(2, cache());
            cache.put(3, "three");
            eventListener.expectOnlyCreatedEvent(3, cache());
            cache.clear();
            eventListener.expectUnorderedEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED, 1, 2, 3);
         }
      });
   }

   public void testNoEventsBeforeAddingListener() {
      final EventLogListener<Integer> eventListener = new EventLogListener<>();
      RemoteCache<Integer, String> rcache = remoteCacheManager.getCache();
      rcache.put(1, "one");
      eventListener.expectNoEvents();
      rcache.put(1, "newone");
      eventListener.expectNoEvents();
      rcache.remove(1);
      eventListener.expectNoEvents();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<Integer, String> cache = rcm.getCache();
            cache.put(1, "one");
            eventListener.expectOnlyCreatedEvent(1, cache());
            cache.put(1, "newone");
            eventListener.expectOnlyModifiedEvent(1, cache());
            cache.remove(1);
            eventListener.expectOnlyRemovedEvent(1, cache());
         }
      });
   }

   public void testNoEventsAfterRemovingListener() {
      final EventLogListener<Integer> eventListener = new EventLogListener<>();
      final RemoteCache<Integer, String> rcache = remoteCacheManager.getCache();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            rcache.put(1, "one");
            eventListener.expectOnlyCreatedEvent(1, cache());
            rcache.put(1, "newone");
            eventListener.expectOnlyModifiedEvent(1, cache());
            rcache.remove(1);
            eventListener.expectOnlyRemovedEvent(1, cache());
         }
      });
      rcache.put(1, "one");
      eventListener.expectNoEvents();
      rcache.put(1, "newone");
      eventListener.expectNoEvents();
      rcache.remove(1);
      eventListener.expectNoEvents();
   }

   public void testSetListeners() {
      final EventLogListener eventListener1 = new EventLogListener();
      final RemoteCache<Integer, String> rcache = remoteCacheManager.getCache();
      withClientListener(eventListener1, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            Set<Object> listeners = rcache.getListeners();
            assertEquals(1, listeners.size());
            assertEquals(eventListener1, listeners.iterator().next());
            final EventLogListener eventListener2 = new EventLogListener();
            withClientListener(eventListener2, new RemoteCacheManagerCallable(remoteCacheManager) {
               @Override
               public void call() {
                  Set<Object> listeners = rcache.getListeners();
                  assertEquals(2, listeners.size());
                  assertTrue(listeners.contains(eventListener1));
                  assertTrue(listeners.contains(eventListener2));
               }
            });
         }
      });
      Set<Object> listeners = rcache.getListeners();
      assertEquals(0, listeners.size());
   }

   public void testCustomTypeEvents() {
      final EventLogListener<CustomKey> eventListener = new EventLogListener<>();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<CustomKey, String> cache = rcm.getCache();
            eventListener.expectNoEvents();
            CustomKey key = new CustomKey(1);
            cache.put(key, "one");
            eventListener.expectOnlyCreatedEvent(key, cache());
            cache.replace(key, "newone");
            eventListener.expectOnlyModifiedEvent(key, cache());
            cache.remove(key);
            eventListener.expectOnlyRemovedEvent(key, cache());
         }
      });
   }

   public void testEventReplayAfterAddingListener() {
      final WithStateEventLogListener<Integer> eventListener = new WithStateEventLogListener<>();
      RemoteCache<Integer, String> cache = remoteCacheManager.getCache();
      cache.put(1, "one");
      cache.put(2, "two");
      eventListener.expectNoEvents();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            eventListener.expectUnorderedEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED, 1, 2);
         }
      });
   }

   public void testNoEventReplayAfterAddingListener() {
      final EventLogListener<Integer> eventListener = new EventLogListener<>();
      RemoteCache<Integer, String> cache = remoteCacheManager.getCache();
      cache.put(1, "one");
      cache.put(2, "two");
      eventListener.expectNoEvents();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            eventListener.expectNoEvents();
         }
      });
   }

   @ClientListener(includeCurrentState = true)
   public static class WithStateEventLogListener<K> extends EventLogListener<K> {}

   static final class CustomKey implements Serializable {
      final int id;
      CustomKey(int id) {
         this.id = id;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         CustomKey customKey = (CustomKey) o;
         return id == customKey.id;
      }

      @Override
      public int hashCode() {
         return id;
      }
   }

}
