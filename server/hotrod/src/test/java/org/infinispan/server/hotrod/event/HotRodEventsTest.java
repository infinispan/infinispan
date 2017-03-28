package org.infinispan.server.hotrod.event;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertSuccess;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.k;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.v;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.withClientListener;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.server.hotrod.HotRodSingleNodeTest;
import org.infinispan.server.hotrod.test.TestGetWithVersionResponse;
import org.testng.annotations.Test;

/**
 * @author Galder Zamarreño
 */
@Test(groups = "functional", testName = "server.hotrod.event.HotRodEventsTest")
public class HotRodEventsTest extends HotRodSingleNodeTest {

   public void testCreatedEvent(Method m) {
      EventLogListener eventListener = new EventLogListener();
      withClientListener(client(), eventListener, Optional.empty(), Optional.empty(), () -> {
         eventListener.expectNoEvents(Optional.empty());
         byte[] key = k(m);
         client().put(key, 0, 0, v(m));
         eventListener.expectOnlyCreatedEvent(cache, key);
      });
   }

   public void testModifiedEvent(Method m) {
      EventLogListener eventListener = new EventLogListener();
      withClientListener(client(), eventListener, Optional.empty(), Optional.empty(), () -> {
         eventListener.expectNoEvents(Optional.empty());
         byte[] key = k(m);
         client().put(key, 0, 0, v(m));
         eventListener.expectOnlyCreatedEvent(cache, key);
         client().put(key, 0, 0, v(m, "v2-"));
         eventListener.expectOnlyModifiedEvent(cache, key);
      });
   }

   public void testRemovedEvent(Method m) {
      EventLogListener eventListener = new EventLogListener();
      withClientListener(client(), eventListener, Optional.empty(), Optional.empty(), () -> {
         eventListener.expectNoEvents(Optional.empty());
         byte[] key = k(m);
         client().remove(key);
         eventListener.expectNoEvents(Optional.empty());
         client().put(key, 0, 0, v(m));
         eventListener.expectOnlyCreatedEvent(cache, key);
         client().remove(key);
         eventListener.expectOnlyRemovedEvent(cache, key);
      });
   }

   public void testReplaceEvents(Method m) {
      EventLogListener eventListener = new EventLogListener();
      withClientListener(client(), eventListener, Optional.empty(), Optional.empty(), () -> {
         eventListener.expectNoEvents(Optional.empty());
         byte[] key = k(m);
         client().replace(key, 0, 0, v(m));
         eventListener.expectNoEvents(Optional.empty());
         client().put(key, 0, 0, v(m));
         eventListener.expectOnlyCreatedEvent(cache, key);
         client().replace(key, 0, 0, v(m, "v2-"));
         eventListener.expectOnlyModifiedEvent(cache, key);
      });
   }

   public void testPutIfAbsentEvents(Method m) {
      EventLogListener eventListener = new EventLogListener();
      withClientListener(client(), eventListener, Optional.empty(), Optional.empty(), () -> {
         eventListener.expectNoEvents(Optional.empty());
         byte[] key = k(m);
         client().putIfAbsent(key, 0, 0, v(m));
         eventListener.expectOnlyCreatedEvent(cache, key);
         client().putIfAbsent(key, 0, 0, v(m, "v2-"));
         eventListener.expectNoEvents(Optional.empty());
      });
   }

   public void testReplaceIfUnmodifiedEvents(Method m) {
      EventLogListener eventListener = new EventLogListener();
      withClientListener(client(), eventListener, Optional.empty(), Optional.empty(), () -> {
         eventListener.expectNoEvents(Optional.empty());
         byte[] key = k(m);
         client().replaceIfUnmodified(key, 0, 0, v(m), 0);
         eventListener.expectNoEvents(Optional.empty());
         client().put(key, 0, 0, v(m));
         eventListener.expectOnlyCreatedEvent(cache, key);
         client().replaceIfUnmodified(key, 0, 0, v(m), 0);
         eventListener.expectNoEvents(Optional.empty());
         TestGetWithVersionResponse resp = client().getWithVersion(k(m), 0);
         assertSuccess(resp, v(m), 0);
         client().replaceIfUnmodified(k(m), 0, 0, v(m, "v1-"), resp.dataVersion);
         eventListener.expectOnlyModifiedEvent(cache, key);
      });
   }

   public void testRemoveIfUnmodifiedEvents(Method m) {
      EventLogListener eventListener = new EventLogListener();
      withClientListener(client(), eventListener, Optional.empty(), Optional.empty(), () -> {
         eventListener.expectNoEvents(Optional.empty());
         byte[] key = k(m);
         client().removeIfUnmodified(key, 0, 0, v(m), 0);
         eventListener.expectNoEvents(Optional.empty());
         client().put(key, 0, 0, v(m));
         eventListener.expectOnlyCreatedEvent(cache, key);
         client().removeIfUnmodified(key, 0, 0, v(m), 0);
         eventListener.expectNoEvents(Optional.empty());
         TestGetWithVersionResponse resp = client().getWithVersion(k(m), 0);
         assertSuccess(resp, v(m), 0);
         client().removeIfUnmodified(k(m), 0, 0, v(m, "v1-"), resp.dataVersion);
         eventListener.expectOnlyRemovedEvent(cache, key);
      });
   }

   public void testClearEvents(Method m) {
      EventLogListener eventListener = new EventLogListener();
      withClientListener(client(), eventListener, Optional.empty(), Optional.empty(), () -> {
         eventListener.expectNoEvents(Optional.empty());
         byte[] key1 = k(m, "k1");
         byte[] key2 = k(m, "k2");
         byte[] key3 = k(m, "k3");
         client().put(key1, 0, 0, v(m));
         eventListener.expectOnlyCreatedEvent(cache, key1);
         client().put(key2, 0, 0, v(m));
         eventListener.expectOnlyCreatedEvent(cache, key2);
         client().put(key3, 0, 0, v(m));
         eventListener.expectOnlyCreatedEvent(cache, key3);
         client().clear();
         // Order in which clear operates cannot be guaranteed
         List<byte[]> keys = Arrays.asList(key1, key2, key3);
         eventListener.expectUnorderedEvents(cache, keys, Event.Type.CACHE_ENTRY_REMOVED);
      });
   }

   public void testNoEventsBeforeAddingListener(Method m) {
      EventLogListener eventListener = new EventLogListener();
      byte[] key = k(m);
      client().put(key, 0, 0, v(m));
      eventListener.expectNoEvents(Optional.empty());
      client().put(key, 0, 0, v(m, "v2-"));
      eventListener.expectNoEvents(Optional.empty());
      client().remove(key);
      withClientListener(client(), eventListener, Optional.empty(), Optional.empty(), () -> {
         byte[] key2 = k(m, "k2-");
         client().put(key2, 0, 0, v(m));
         eventListener.expectSingleEvent(cache, key2, Event.Type.CACHE_ENTRY_CREATED);
         client().put(key2, 0, 0, v(m, "v2-"));
         eventListener.expectSingleEvent(cache, key2, Event.Type.CACHE_ENTRY_MODIFIED);
         client().remove(key2);
         eventListener.expectSingleEvent(cache, key2, Event.Type.CACHE_ENTRY_REMOVED);
      });
   }

   public void testNoEventsAfterRemovingListener(Method m) {
      EventLogListener eventListener = new EventLogListener();
      withClientListener(client(), eventListener, Optional.empty(), Optional.empty(), () -> {
         byte[] key = k(m);
         client().put(key, 0, 0, v(m));
         eventListener.expectSingleEvent(cache, key, Event.Type.CACHE_ENTRY_CREATED);
         client().put(key, 0, 0, v(m, "v2-"));
         eventListener.expectSingleEvent(cache, key, Event.Type.CACHE_ENTRY_MODIFIED);
         client().remove(key);
         eventListener.expectSingleEvent(cache, key, Event.Type.CACHE_ENTRY_REMOVED);
      });
      byte[] key = k(m, "k2-");
      client().put(key, 0, 0, v(m));
      eventListener.expectNoEvents(Optional.empty());
      client().put(key, 0, 0, v(m, "v2-"));
      eventListener.expectNoEvents(Optional.empty());
      client().remove(key);
      eventListener.expectNoEvents(Optional.empty());
   }

   public void testEventReplayAfterAddingListener(Method m) {
      EventLogListener eventListener = new EventLogListener();
      byte[] k1 = k(m, "k1-");
      byte[] v1 = v(m, "v1-");
      byte[] k2 = k(m, "k2-");
      byte[] v2 = v(m, "v2-");
      byte[] k3 = k(m, "k3-");
      byte[] v3 = v(m, "v3-");
      client().put(k1, 0, 0, v1);
      client().put(k2, 0, 0, v2);
      client().put(k3, 0, 0, v3);
      withClientListener(client(), eventListener, Optional.empty(), Optional.empty(), true, true, () -> {
         eventListener.expectUnorderedEvents(cache, Arrays.asList(k1, k2, k3), Event.Type.CACHE_ENTRY_CREATED);
      });
   }

}
