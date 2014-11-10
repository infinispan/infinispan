package org.infinispan.server.hotrod.event

import java.lang.reflect.Method
import org.infinispan.notifications.cachelistener.event.Event
import org.infinispan.server.hotrod.HotRodSingleNodeTest
import org.infinispan.server.hotrod.test.HotRodTestingUtil._
import org.testng.annotations.Test

/**
 * @author Galder Zamarreño
 */
@Test(groups = Array("functional"), testName = "server.hotrod.event.HotRodEventsTest")
class HotRodEventsTest extends HotRodSingleNodeTest {

   def testCreatedEvent(m: Method) {
      implicit val eventListener = new EventLogListener
      withClientListener() { () =>
         eventListener.expectNoEvents()
         val key = k(m)
         client.put(key, 0, 0, v(m))
         eventListener.expectOnlyCreatedEvent(key)
      }
   }

   def testModifiedEvent(m: Method) {
      implicit val eventListener = new EventLogListener
      withClientListener() { () =>
         eventListener.expectNoEvents()
         val key = k(m)
         client.put(key, 0, 0, v(m))
         eventListener.expectOnlyCreatedEvent(key)
         client.put(key, 0, 0, v(m, "v2-"))
         eventListener.expectOnlyModifiedEvent(key)
      }
   }

   def testRemovedEvent(m: Method) {
      implicit val eventListener = new EventLogListener
      withClientListener() { () =>
         eventListener.expectNoEvents()
         val key = k(m)
         client.remove(key)
         eventListener.expectNoEvents()
         client.put(key, 0, 0, v(m))
         eventListener.expectOnlyCreatedEvent(key)
         client.remove(key)
         eventListener.expectOnlyRemovedEvent(key)
      }
   }

   def testReplaceEvents(m: Method) {
      implicit val eventListener = new EventLogListener
      withClientListener() { () =>
         eventListener.expectNoEvents()
         val key = k(m)
         client.replace(key, 0, 0, v(m))
         eventListener.expectNoEvents()
         client.put(key, 0, 0, v(m))
         eventListener.expectOnlyCreatedEvent(key)
         client.replace(key, 0, 0, v(m, "v2-"))
         eventListener.expectOnlyModifiedEvent(key)
      }
   }

   def testPutIfAbsentEvents(m: Method) {
      implicit val eventListener = new EventLogListener
      withClientListener() { () =>
         eventListener.expectNoEvents()
         val key = k(m)
         client.putIfAbsent(key, 0, 0, v(m))
         eventListener.expectOnlyCreatedEvent(key)
         client.putIfAbsent(key, 0, 0, v(m, "v2-"))
         eventListener.expectNoEvents()
      }
   }

   def testReplaceIfUnmodifiedEvents(m: Method) {
      implicit val eventListener = new EventLogListener
      withClientListener() { () =>
         eventListener.expectNoEvents()
         val key = k(m)
         client.replaceIfUnmodified(key, 0, 0, v(m), 0)
         eventListener.expectNoEvents()
         client.put(key, 0, 0, v(m))
         eventListener.expectOnlyCreatedEvent(key)
         client.replaceIfUnmodified(key, 0, 0, v(m), 0)
         eventListener.expectNoEvents()
         val resp = client.getWithVersion(k(m), 0)
         assertSuccess(resp, v(m), 0)
         client.replaceIfUnmodified(k(m), 0, 0, v(m, "v1-"), resp.dataVersion)
         eventListener.expectOnlyModifiedEvent(key)
      }
   }

   def testRemoveIfUnmodifiedEvents(m: Method) {
      implicit val eventListener = new EventLogListener
      withClientListener() { () =>
         eventListener.expectNoEvents()
         val key = k(m)
         client.removeIfUnmodified(key, 0, 0, v(m), 0)
         eventListener.expectNoEvents()
         client.put(key, 0, 0, v(m))
         eventListener.expectOnlyCreatedEvent(key)
         client.removeIfUnmodified(key, 0, 0, v(m), 0)
         eventListener.expectNoEvents()
         val resp = client.getWithVersion(k(m), 0)
         assertSuccess(resp, v(m), 0)
         client.removeIfUnmodified(k(m), 0, 0, v(m, "v1-"), resp.dataVersion)
         eventListener.expectOnlyRemovedEvent(key)
      }
   }

   def testClearEvents(m: Method) {
      implicit val eventListener = new EventLogListener
      withClientListener() { () =>
         eventListener.expectNoEvents()
         val key1 = k(m, "k1")
         val key2 = k(m, "k2")
         val key3 = k(m, "k3")
         client.put(key1, 0, 0, v(m))
         eventListener.expectOnlyCreatedEvent(key1)
         client.put(key2, 0, 0, v(m))
         eventListener.expectOnlyCreatedEvent(key2)
         client.put(key3, 0, 0, v(m))
         eventListener.expectOnlyCreatedEvent(key3)
         client.clear
         // Order in which clear operates cannot be guaranteed
         val keys = List(key1, key2, key3)
         eventListener.expectUnorderedEvents(keys, Event.Type.CACHE_ENTRY_REMOVED)
      }
   }

   def testNoEventsBeforeAddingListener(m: Method) {
      implicit val eventListener = new EventLogListener
      val key = k(m)
      client.put(key, 0, 0, v(m))
      eventListener.expectNoEvents()
      client.put(key, 0, 0, v(m, "v2-"))
      eventListener.expectNoEvents()
      client.remove(key)
      withClientListener() { () =>
         val key = k(m, "k2-")
         client.put(key, 0, 0, v(m))
         eventListener.expectSingleEvent(key, Event.Type.CACHE_ENTRY_CREATED)
         client.put(key, 0, 0, v(m, "v2-"))
         eventListener.expectSingleEvent(key, Event.Type.CACHE_ENTRY_MODIFIED)
         client.remove(key)
         eventListener.expectSingleEvent(key, Event.Type.CACHE_ENTRY_REMOVED)
      }
   }

   def testNoEventsAfterRemovingListener(m: Method) {
      implicit val eventListener = new EventLogListener
      withClientListener() { () =>
         val key = k(m)
         client.put(key, 0, 0, v(m))
         eventListener.expectSingleEvent(key, Event.Type.CACHE_ENTRY_CREATED)
         client.put(key, 0, 0, v(m, "v2-"))
         eventListener.expectSingleEvent(key, Event.Type.CACHE_ENTRY_MODIFIED)
         client.remove(key)
         eventListener.expectSingleEvent(key, Event.Type.CACHE_ENTRY_REMOVED)
      }
      val key = k(m, "k2-")
      client.put(key, 0, 0, v(m))
      eventListener.expectNoEvents()
      client.put(key, 0, 0, v(m, "v2-"))
      eventListener.expectNoEvents()
      client.remove(key)
      eventListener.expectNoEvents()
   }

   def testEventReplayAfterAddingListener(m: Method) {
      implicit val eventListener = new EventLogListener
      val (k1, v1) = (k(m, "k1-"), v(m, "v1-"))
      val (k2, v2) = (k(m, "k2-"), v(m, "v2-"))
      val (k3, v3) = (k(m, "k3-"), v(m, "v3-"))
      client.put(k1, 0, 0, v1)
      client.put(k2, 0, 0, v2)
      client.put(k3, 0, 0, v3)
      withClientListener(includeState = true) { () =>
         eventListener.expectUnorderedEvents(List(k1, k2, k3), Event.Type.CACHE_ENTRY_CREATED)
      }
   }

}
