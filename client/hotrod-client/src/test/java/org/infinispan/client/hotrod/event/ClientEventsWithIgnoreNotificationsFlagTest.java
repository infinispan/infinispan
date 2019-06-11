package org.infinispan.client.hotrod.event;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.withClientListener;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.testng.annotations.Test;

/**
 * @author Katia Aresti
 * @since 10
 */
@Test(groups = {"functional", "smoke"}, testName = "client.hotrod.event.ClientEventsWithIgnoreNotificationsFlagTest")
public class ClientEventsWithIgnoreNotificationsFlagTest extends SingleHotRodServerTest {

   public void testCreatedEvent() {
      final EventLogListener<Integer> l = new EventLogListener<>(remoteCacheManager.getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).put(1, "one");
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).put(2, "two");
         l.expectNoEvents();
      });
   }

   public void testModifiedEvent() {
      final EventLogListener<Integer> l = new EventLogListener<>(remoteCacheManager.getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).put(1, "one");
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).put(1, "newone");
         l.expectNoEvents();
      });
   }

   public void testRemovedEvent() {
      final EventLogListener<Integer> l = new EventLogListener<>(remoteCacheManager.getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).remove(1);
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).put(1, "one");
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).remove(1);
         l.expectNoEvents();
      });
   }

   public void testReplaceEvents() {
      final EventLogListener<Integer> l = new EventLogListener<>(remoteCacheManager.getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).replace(1, "one");
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).put(1, "one");
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).replace(1, "newone");
         l.expectNoEvents();
      });
   }

   public void testPutIfAbsentEvents() {
      final EventLogListener<Integer> l = new EventLogListener<>(remoteCacheManager.getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).putIfAbsent(1, "one");
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).putIfAbsent(1, "newone");
         l.expectNoEvents();
      });
   }

   public void testReplaceIfUnmodifiedEvents() {
      final EventLogListener<Integer> l = new EventLogListener<>(remoteCacheManager.getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).replaceWithVersion(1, "one", 0);
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).putIfAbsent(1, "one");
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).replaceWithVersion(1, "one", 0);
         l.expectNoEvents();
      });
   }

   public void testRemoveIfUnmodifiedEvents() {
      final EventLogListener<Integer> l = new EventLogListener<>(remoteCacheManager.getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).removeWithVersion(1, 0);
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).putIfAbsent(1, "one");
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).removeWithVersion(1, 0);
         l.expectNoEvents();
      });
   }

   public void testClearEvents() {
      final EventLogListener<Integer> l = new EventLogListener<>(remoteCacheManager.getCache());
      withClientListener(l, remote -> {
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).put(1, "one");
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).put(2, "two");
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).put(3, "three");
         l.expectNoEvents();
         remote.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).clear();
         l.expectNoEvents();
      });
   }
}
