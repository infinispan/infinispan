package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.test.RemoteCacheManagerCallable;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.Set;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.*;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * @author Galder Zamarreño
 */
@Test(groups = "functional", testName = "client.hotrod.event.ClientEventsTest")
public class ClientEventsTest extends SingleHotRodServerTest {

   public void testCreatedEvent() {
      final EventLogListener eventListener = new EventLogListener();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<Integer, String> cache = rcm.getCache();
            expectNoEvents(eventListener);
            cache.put(1, "one");
            expectOnlyCreatedEvent(1, eventListener, cache());
            cache.put(2, "two");
            expectOnlyCreatedEvent(2, eventListener, cache());
         }
      });
   }

   public void testModifiedEvent() {
      final EventLogListener eventListener = new EventLogListener();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<Integer, String> cache = rcm.getCache();
            expectNoEvents(eventListener);
            cache.put(1, "one");
            expectOnlyCreatedEvent(1, eventListener, cache());
            cache.put(1, "newone");
            expectOnlyModifiedEvent(1, eventListener, cache());
         }
      });
   }

   public void testRemovedEvent() {
      final EventLogListener eventListener = new EventLogListener();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<Integer, String> cache = rcm.getCache();
            expectNoEvents(eventListener);
            cache.put(1, "one");
            expectOnlyCreatedEvent(1, eventListener, cache());
            cache.remove(1);
            expectOnlyRemovedEvent(1, eventListener, cache());
         }
      });
   }

   public void testReplaceEvents() {
      final EventLogListener eventListener = new EventLogListener();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<Integer, String> cache = rcm.getCache();
            expectNoEvents(eventListener);
            cache.replace(1, "one");
            expectNoEvents(eventListener);
            cache.put(1, "one");
            expectOnlyCreatedEvent(1, eventListener, cache());
            cache.replace(1, "newone");
            expectOnlyModifiedEvent(1, eventListener, cache());
         }
      });
   }

   public void testPutIfAbsentEvents() {
      final EventLogListener eventListener = new EventLogListener();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<Integer, String> cache = rcm.getCache();
            expectNoEvents(eventListener);
            cache.putIfAbsent(1, "one");
            expectOnlyCreatedEvent(1, eventListener, cache());
            cache.putIfAbsent(1, "newone");
            expectNoEvents(eventListener);
         }
      });
   }

   public void testReplaceIfUnmodifiedEvents() {
      final EventLogListener eventListener = new EventLogListener();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<Integer, String> cache = rcm.getCache();
            expectNoEvents(eventListener);
            cache.replaceWithVersion(1, "one", 0);
            expectNoEvents(eventListener);
            cache.putIfAbsent(1, "one");
            expectOnlyCreatedEvent(1, eventListener, cache());
            cache.replaceWithVersion(1, "one", 0);
            expectNoEvents(eventListener);
            VersionedValue<String> versioned = cache.getVersioned(1);
            cache.replaceWithVersion(1, "one", versioned.getVersion());
            expectOnlyModifiedEvent(1, eventListener, cache());
         }
      });
   }

   public void testRemoveIfUnmodifiedEvents() {
      final EventLogListener eventListener = new EventLogListener();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<Integer, String> cache = rcm.getCache();
            expectNoEvents(eventListener);
            cache.removeWithVersion(1, 0);
            expectNoEvents(eventListener);
            cache.putIfAbsent(1, "one");
            expectOnlyCreatedEvent(1, eventListener, cache());
            cache.removeWithVersion(1, 0);
            expectNoEvents(eventListener);
            VersionedValue<String> versioned = cache.getVersioned(1);
            cache.removeWithVersion(1, versioned.getVersion());
            expectOnlyRemovedEvent(1, eventListener, cache());
         }
      });
   }

   public void testClearEvents() {
      final EventLogListener eventListener = new EventLogListener();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<Integer, String> cache = rcm.getCache();
            expectNoEvents(eventListener);
            cache.put(1, "one");
            expectOnlyCreatedEvent(1, eventListener, cache());
            cache.put(2, "two");
            expectOnlyCreatedEvent(2, eventListener, cache());
            cache.put(3, "three");
            expectOnlyCreatedEvent(3, eventListener, cache());
            cache.clear();
            expectUnorderedEvents(eventListener, ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED, 1, 2, 3);
         }
      });
   }

   public void testNoEventsBeforeAddingListener() {
      final EventLogListener eventListener = new EventLogListener();
      RemoteCache<Integer, String> rcache = remoteCacheManager.getCache();
      rcache.put(1, "one");
      expectNoEvents(eventListener);
      rcache.put(1, "newone");
      expectNoEvents(eventListener);
      rcache.remove(1);
      expectNoEvents(eventListener);
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<Integer, String> cache = rcm.getCache();
            cache.put(1, "one");
            expectOnlyCreatedEvent(1, eventListener, cache());
            cache.put(1, "newone");
            expectOnlyModifiedEvent(1, eventListener, cache());
            cache.remove(1);
            expectOnlyRemovedEvent(1, eventListener, cache());
         }
      });
   }

   public void testNoEventsAfterRemovingListener() {
      final EventLogListener eventListener = new EventLogListener();
      final RemoteCache<Integer, String> rcache = remoteCacheManager.getCache();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            rcache.put(1, "one");
            expectOnlyCreatedEvent(1, eventListener, cache());
            rcache.put(1, "newone");
            expectOnlyModifiedEvent(1, eventListener, cache());
            rcache.remove(1);
            expectOnlyRemovedEvent(1, eventListener, cache());
         }
      });
      rcache.put(1, "one");
      expectNoEvents(eventListener);
      rcache.put(1, "newone");
      expectNoEvents(eventListener);
      rcache.remove(1);
      expectNoEvents(eventListener);
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
      final EventLogListener eventListener = new EventLogListener();
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            RemoteCache<CustomKey, String> cache = rcm.getCache();
            expectNoEvents(eventListener);
            CustomKey key = new CustomKey(1);
            cache.put(key, "one");
            expectOnlyCreatedEvent(key, eventListener, cache());
            cache.replace(key, "newone");
            expectOnlyModifiedEvent(key, eventListener, cache());
            cache.remove(key);
            expectOnlyRemovedEvent(key, eventListener, cache());
         }
      });
   }

   public void testEventReplayAfterAddingListener() {
      final EventLogListener eventListener = new EventLogListener();
      RemoteCache<Integer, String> cache = remoteCacheManager.getCache();
      cache.put(1, "one");
      cache.put(2, "two");
      expectNoEvents(eventListener);
      withClientListener(eventListener, new RemoteCacheManagerCallable(remoteCacheManager) {
         @Override
         public void call() {
            expectUnorderedEvents(eventListener, ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED, 1, 2);
         }
      });
   }

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
         if (id != customKey.id) return false;
         return true;
      }

      @Override
      public int hashCode() {
         return id;
      }
   }

}
