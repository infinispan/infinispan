package org.infinispan.server.hotrod.event

import java.lang.reflect.Method
import java.util
import java.util.{Collections, List, Optional}

import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.metadata.Metadata
import org.infinispan.notifications.cachelistener.event.Event
import org.infinispan.server.hotrod.test.HotRodTestingUtil._
import org.infinispan.server.hotrod.{Bytes, HotRodServer, HotRodSingleNodeTest}
import org.testng.annotations.Test
import org.infinispan.notifications.cachelistener.filter._
import org.infinispan.util.KeyValuePair

/**
 * @author Galder Zamarreño
 */
@Test(groups = Array("functional"), testName = "server.hotrod.event.HotRodFilterEventsTest")
class HotRodFilterEventsTest extends HotRodSingleNodeTest {

   override protected def createStartHotRodServer(cacheManager: EmbeddedCacheManager): HotRodServer = {
      val server = startHotRodServer(cacheManager)
      server.addCacheEventFilterFactory("static-filter-factory", new StaticKeyValueFilterFactory(Array[Byte](1, 2, 3)))
      server.addCacheEventFilterFactory("dynamic-filter-factory", new DynamicKeyValueFilterFactory())
      server
   }

   def testFilteredEvents(m: Method) {
      implicit val eventListener = new EventLogListener
      val acceptedKey = Array[Byte](1, 2, 3)
      withClientListener(filterFactory = Optional.of(new KeyValuePair[String, util.List[Bytes]]("static-filter-factory", Collections.emptyList()))) { () =>
         eventListener.expectNoEvents()
         val key = k(m)
         client.remove(key)
         eventListener.expectNoEvents()
         client.put(key, 0, 0, v(m))
         eventListener.expectNoEvents()
         client.put(acceptedKey, 0, 0, v(m))
         eventListener.expectSingleEvent(acceptedKey, Event.Type.CACHE_ENTRY_CREATED)
         client.put(acceptedKey, 0, 0, v(m, "v2-"))
         eventListener.expectSingleEvent(acceptedKey, Event.Type.CACHE_ENTRY_MODIFIED)
         client.remove(key)
         eventListener.expectNoEvents()
         client.remove(acceptedKey)
         eventListener.expectSingleEvent(acceptedKey, Event.Type.CACHE_ENTRY_REMOVED)
      }
   }

   def testParameterBasedFiltering(m: Method) {
      implicit val eventListener = new EventLogListener
      val acceptedKey = Array[Byte](4, 5, 6)
      withClientListener(filterFactory = Optional.of(new KeyValuePair[String, util.List[Bytes]]("dynamic-filter-factory", Collections.singletonList(Array[Byte](4, 5, 6))))) { () =>
         eventListener.expectNoEvents()
         val key = k(m)
         client.put(key, 0, 0, v(m))
         eventListener.expectNoEvents()
         client.put(acceptedKey, 0, 0, v(m))
         eventListener.expectSingleEvent(acceptedKey, Event.Type.CACHE_ENTRY_CREATED)
         client.put(acceptedKey, 0, 0, v(m, "v2-"))
         eventListener.expectSingleEvent(acceptedKey, Event.Type.CACHE_ENTRY_MODIFIED)
         client.remove(key)
         eventListener.expectNoEvents()
         client.remove(acceptedKey)
         eventListener.expectSingleEvent(acceptedKey, Event.Type.CACHE_ENTRY_REMOVED)
      }
   }

   def testFilteredEventsReplay(m: Method) {
      implicit val eventListener = new EventLogListener
      val staticAcceptedKey = Array[Byte](1, 2, 3)
      val dynamicAcceptedKey = Array[Byte](7, 8, 9)
      val key = k(m)
      client.put(key, 0, 0, v(m))
      client.put(staticAcceptedKey, 0, 0, v(m))
      client.put(dynamicAcceptedKey, 0, 0, v(m))
      withClientListener(filterFactory = Optional.of(new KeyValuePair[String, util.List[Bytes]]("static-filter-factory", Collections.emptyList())), includeState = true) { () =>
         eventListener.expectSingleEvent(staticAcceptedKey, Event.Type.CACHE_ENTRY_CREATED)
      }
      withClientListener(filterFactory = Optional.of(new KeyValuePair[String, util.List[Bytes]]("dynamic-filter-factory", Collections.singletonList(Array[Byte](7, 8, 9)))), includeState = true) { () =>
         eventListener.expectSingleEvent(dynamicAcceptedKey, Event.Type.CACHE_ENTRY_CREATED)
      }
   }

   def testFilteredEventsNoReplay(m: Method) {
      implicit val eventListener = new EventLogListener
      val staticAcceptedKey = Array[Byte](1, 2, 3)
      val dynamicAcceptedKey = Array[Byte](7, 8, 9)
      val key = k(m)
      client.put(key, 0, 0, v(m))
      client.put(staticAcceptedKey, 0, 0, v(m))
      client.put(dynamicAcceptedKey, 0, 0, v(m))
      withClientListener(filterFactory = Optional.of(new KeyValuePair[String, util.List[Bytes]]("static-filter-factory", Collections.emptyList())), includeState = false) { () =>
         eventListener.expectNoEvents()
      }
      withClientListener(filterFactory = Optional.of(new KeyValuePair[String, util.List[Bytes]]("dynamic-filter-factory", Collections.singletonList(Array[Byte](7, 8, 9)))), includeState = false) { () =>
         eventListener.expectNoEvents()
      }
   }

   class StaticKeyValueFilterFactory(staticKey: Bytes) extends CacheEventFilterFactory {
      override def getFilter[K, V](params: Array[AnyRef]): CacheEventFilter[K, V] = {
         new CacheEventFilter[Bytes, Bytes] {
            override def accept(key: Bytes, prevValue: Bytes, prevMetadata: Metadata, value: Bytes, metadata: Metadata,
                                eventType: EventType): Boolean = {
               if (util.Arrays.equals(key, staticKey)) true else false
            }

         }
      }.asInstanceOf[CacheEventFilter[K, V]]
   }

   class DynamicKeyValueFilterFactory extends CacheEventFilterFactory {
      override def getFilter[K, V](params: Array[AnyRef]): CacheEventFilter[K, V] = {
         new CacheEventFilter[Bytes, Bytes] {
            override def accept(key: Bytes, prevValue: Bytes, prevMetadata: Metadata, value: Bytes, metadata: Metadata,
                                eventType: EventType): Boolean = {
               val acceptedKey = params.head.asInstanceOf[Bytes]
               if (util.Arrays.equals(key, acceptedKey)) true else false
            }

         }
      }.asInstanceOf[CacheEventFilter[K, V]]
   }

}
