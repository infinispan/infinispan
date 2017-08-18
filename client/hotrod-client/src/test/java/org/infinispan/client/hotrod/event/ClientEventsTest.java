package org.infinispan.client.hotrod.event;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withClientListener;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.Serializable;
import java.util.Set;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.testng.annotations.Test;

/**
 * @author Galder Zamarreño
 */
@Test(groups = {"functional", "smoke"}, testName = "client.hotrod.event.ClientEventsTest")
public class ClientEventsTest extends SingleHotRodServerTest {

   public void testCreatedEvent() {
      final EventLogListener<Integer> l = new EventLogListener<>(remoteCacheManager.getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.put(1, "one");
         l.expectOnlyCreatedEvent(1);
         remote.put(2, "two");
         l.expectOnlyCreatedEvent(2);
      });
   }

   public void testModifiedEvent() {
      final EventLogListener<Integer> l = new EventLogListener<>(remoteCacheManager.getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.put(1, "one");
         l.expectOnlyCreatedEvent(1);
         remote.put(1, "newone");
         l.expectOnlyModifiedEvent(1);
      });
   }

   public void testRemovedEvent() {
      final EventLogListener<Integer> l = new EventLogListener<>(remoteCacheManager.getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.remove(1);
         l.expectNoEvents();
         remote.put(1, "one");
         l.expectOnlyCreatedEvent(1);
         remote.remove(1);
         l.expectOnlyRemovedEvent(1);
      });
   }

   public void testReplaceEvents() {
      final EventLogListener<Integer> l = new EventLogListener<>(remoteCacheManager.getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.replace(1, "one");
         l.expectNoEvents();
         remote.put(1, "one");
         l.expectOnlyCreatedEvent(1);
         remote.replace(1, "newone");
         l.expectOnlyModifiedEvent(1);
      });
   }

   public void testPutIfAbsentEvents() {
      final EventLogListener<Integer> l = new EventLogListener<>(remoteCacheManager.getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.putIfAbsent(1, "one");
         l.expectOnlyCreatedEvent(1);
         remote.putIfAbsent(1, "newone");
         l.expectNoEvents();
      });
   }

   public void testReplaceIfUnmodifiedEvents() {
      final EventLogListener<Integer> l = new EventLogListener<>(remoteCacheManager.getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.replaceWithVersion(1, "one", 0);
         l.expectNoEvents();
         remote.putIfAbsent(1, "one");
         l.expectOnlyCreatedEvent(1);
         remote.replaceWithVersion(1, "one", 0);
         l.expectNoEvents();
         VersionedValue<?> versioned = remote.getVersioned(1);
         remote.replaceWithVersion(1, "one", versioned.getVersion());
         l.expectOnlyModifiedEvent(1);
      });
   }

   public void testRemoveIfUnmodifiedEvents() {
      final EventLogListener<Integer> l = new EventLogListener<>(remoteCacheManager.getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.removeWithVersion(1, 0);
         l.expectNoEvents();
         remote.putIfAbsent(1, "one");
         l.expectOnlyCreatedEvent(1);
         remote.removeWithVersion(1, 0);
         l.expectNoEvents();
         VersionedValue<?> versioned = remote.getVersioned(1);
         remote.removeWithVersion(1, versioned.getVersion());
         l.expectOnlyRemovedEvent(1);
      });
   }

   public void testClearEvents() {
      final EventLogListener<Integer> l = new EventLogListener<>(remoteCacheManager.getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.put(1, "one");
         l.expectOnlyCreatedEvent(1);
         remote.put(2, "two");
         l.expectOnlyCreatedEvent(2);
         remote.put(3, "three");
         l.expectOnlyCreatedEvent(3);
         remote.clear();
         l.expectUnorderedEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED, 1, 2, 3);
      });
   }

   public void testNoEventsBeforeAddingListener() {
      RemoteCache<Integer, String> rcache = remoteCacheManager.getCache();
      final EventLogListener<Integer> l = new EventLogListener<>(rcache);
      rcache.put(1, "one");
      l.expectNoEvents();
      rcache.put(1, "newone");
      l.expectNoEvents();
      rcache.remove(1);
      l.expectNoEvents();
      createUpdateRemove(l);
   }

   private void createUpdateRemove(EventLogListener<Integer> l) {
      withClientListener(l, remote -> {
         remote.put(1, "one");
         l.expectOnlyCreatedEvent(1);
         remote.put(1, "newone");
         l.expectOnlyModifiedEvent(1);
         remote.remove(1);
         l.expectOnlyRemovedEvent(1);
      });
   }

   public void testNoEventsAfterRemovingListener() {
      final RemoteCache<Integer, String> rcache = remoteCacheManager.getCache();
      final EventLogListener<Integer> l = new EventLogListener<>(rcache);
      createUpdateRemove(l);
      rcache.put(1, "one");
      l.expectNoEvents();
      rcache.put(1, "newone");
      l.expectNoEvents();
      rcache.remove(1);
      l.expectNoEvents();
   }

   public void testSetListeners() {
      final RemoteCache<Integer, String> rcache = remoteCacheManager.getCache();
      final EventLogListener l1 = new EventLogListener<>(rcache);
      withClientListener(l1, remote1 -> {
         Set<?> listeners1 = remote1.getListeners();
         assertEquals(1, listeners1.size());
         assertEquals(l1, listeners1.iterator().next());
         final EventLogListener l2 = new EventLogListener<>(rcache);
         withClientListener(l2, remote2 -> {
            Set<?> listeners2 = remote2.getListeners();
            assertEquals(2, listeners2.size());
            assertTrue(listeners2.contains(l1));
            assertTrue(listeners2.contains(l2));
         });
      });
      Set<Object> listeners = rcache.getListeners();
      assertEquals(0, listeners.size());
   }

   public void testCustomTypeEvents() {
      final EventLogListener<CustomKey> l = new EventLogListener<>(remoteCacheManager.getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         CustomKey key = new CustomKey(1);
         remote.put(key, "one");
         l.expectOnlyCreatedEvent(key);
         remote.replace(key, "newone");
         l.expectOnlyModifiedEvent(key);
         remote.remove(key);
         l.expectOnlyRemovedEvent(key);
      });
   }

   public void testEventReplayAfterAddingListener() {
      RemoteCache<Integer, String> cache = remoteCacheManager.getCache();
      final WithStateEventLogListener<Integer> l = new WithStateEventLogListener<>(cache);
      cache.put(1, "one");
      cache.put(2, "two");
      l.expectNoEvents();
      withClientListener(l, remote ->
            l.expectUnorderedEvents(ClientEvent.Type.CLIENT_CACHE_ENTRY_CREATED, 1, 2));
   }

   public void testNoEventReplayAfterAddingListener() {
      RemoteCache<Integer, String> cache = remoteCacheManager.getCache();
      final EventLogListener<Integer> l = new EventLogListener<>(cache);
      cache.put(1, "one");
      cache.put(2, "two");
      l.expectNoEvents();
      withClientListener(l, remote -> l.expectNoEvents());
   }

   @ClientListener(includeCurrentState = true)
   public static class WithStateEventLogListener<K> extends EventLogListener<K> {
      public WithStateEventLogListener(RemoteCache<K, ?> remote) {
         super(remote);
      }
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
         return id == customKey.id;
      }

      @Override
      public int hashCode() {
         return id;
      }
   }

}
