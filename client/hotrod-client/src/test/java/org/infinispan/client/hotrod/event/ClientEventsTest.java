package org.infinispan.client.hotrod.event;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryCreated;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryModified;
import org.infinispan.client.hotrod.annotation.ClientCacheEntryRemoved;
import org.infinispan.client.hotrod.annotation.ClientListener;
import org.infinispan.client.hotrod.test.RemoteCacheManagerCallable;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.metadata.Metadata;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.*;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

/**
 * @author Galder Zamarreño
 */
@Test(groups = "functional", testName = "client.hotrod.event.ClientEventsTest")
public class ClientEventsTest extends SingleHotRodServerTest {

   // TODO: test listener list...
   // TODO: test listeners are removed on disconnection
   // TODO: test custom types

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
            expectUnorderedRemovedEvents(eventListener, 1, 2, 3);
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

   <K> void expectUnorderedRemovedEvents(EventLogListener eventListener, K... keys) {
      List<K> assertedKeys = new ArrayList<K>();
      for (int i = 0; i < keys.length; i++) {
         ClientCacheEntryRemovedEvent<K> event =
               eventListener.pollEvent(ClientEvent.Type.CLIENT_CACHE_ENTRY_REMOVED);
         int initialSize = assertedKeys.size();
         for (K key : keys) checkUnorderedKeyEvent(assertedKeys, key, event.getKey());
         int finalSize = assertedKeys.size();
         assertEquals(initialSize + 1, finalSize);
      }
   }

   <K> boolean checkUnorderedKeyEvent(List<K> assertedKeys, K key, K eventKey) {
      if (key.equals(eventKey)) {
         assertFalse(assertedKeys.contains(key));
         assertedKeys.add(key);
         return true;
      }
      return false;
   }

}
