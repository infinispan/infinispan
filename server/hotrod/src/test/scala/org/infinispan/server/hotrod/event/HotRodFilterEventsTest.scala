package org.infinispan.server.hotrod.event

import java.lang.reflect.Method
import java.util
import org.infinispan.filter.KeyValueFilter
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.metadata.Metadata
import org.infinispan.notifications.cachelistener.event.Event
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder
import org.infinispan.server.hotrod.test.HotRodTestingUtil._
import org.infinispan.server.hotrod.{Bytes, HotRodServer, HotRodSingleNodeTest}
import org.testng.annotations.Test

/**
 * @author Galder Zamarreño
 */
@Test(groups = Array("functional"), testName = "server.hotrod.event.HotRodFilterEventsTest")
class HotRodFilterEventsTest extends HotRodSingleNodeTest {

   val staticKey = Array[Byte](1, 2, 3)
   val keyValueFilterFactory = new AcceptedKeyValueFilterFactory()

   override protected def createStartHotRodServer(cacheManager: EmbeddedCacheManager): HotRodServer = {
      val builder = new HotRodServerConfigurationBuilder
      builder.keyValueFilterFactory("test-filter-factory", keyValueFilterFactory).marshaller(null)
      startHotRodServer(cacheManager, builder)
   }

   def testFilteredEvents(m: Method) {
      implicit val eventListener = new EventLogListener
      val acceptedKey = Array[Byte](1, 2, 3)
      keyValueFilterFactory.dynamic = false
      withClientListener(Some(("test-filter-factory", List.empty))) { () =>
         expectNoEvents()
         val key = k(m)
         client.put(key, 0, 0, v(m))
         expectNoEvents()
         client.put(acceptedKey, 0, 0, v(m))
         expectSingleEvent(acceptedKey, Event.Type.CACHE_ENTRY_CREATED)
         client.put(acceptedKey, 0, 0, v(m, "v2-"))
         expectSingleEvent(acceptedKey, Event.Type.CACHE_ENTRY_MODIFIED)
         client.remove(key)
         expectNoEvents()
         client.remove(acceptedKey)
         expectSingleEvent(acceptedKey, Event.Type.CACHE_ENTRY_REMOVED)
      }
   }

   def testParameterBasedFiltering(m: Method) {
      implicit val eventListener = new EventLogListener
      val acceptedKey = Array[Byte](4, 5, 6)
      keyValueFilterFactory.dynamic = true
      withClientListener(Some(("test-filter-factory", List(Array[Byte](4, 5, 6))))) { () =>
         expectNoEvents()
         val key = k(m)
         client.put(key, 0, 0, v(m))
         expectNoEvents()
         client.put(acceptedKey, 0, 0, v(m))
         expectSingleEvent(acceptedKey, Event.Type.CACHE_ENTRY_CREATED)
         client.put(acceptedKey, 0, 0, v(m, "v2-"))
         expectSingleEvent(acceptedKey, Event.Type.CACHE_ENTRY_MODIFIED)
         client.remove(key)
         expectNoEvents()
         client.remove(acceptedKey)
         expectSingleEvent(acceptedKey, Event.Type.CACHE_ENTRY_REMOVED)
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
      withClientListener(Some(("test-filter-factory", List.empty))) { () =>
         expectSingleEvent(staticAcceptedKey, Event.Type.CACHE_ENTRY_CREATED)
      }
      keyValueFilterFactory.dynamic = true
      withClientListener(Some(("test-filter-factory", List(Array[Byte](7, 8, 9))))) { () =>
         expectSingleEvent(dynamicAcceptedKey, Event.Type.CACHE_ENTRY_CREATED)
      }
   }

   class AcceptedKeyValueFilterFactory extends KeyValueFilterFactory {
      var dynamic = false
      override def getKeyValueFilter[K, V](params: Array[AnyRef]): KeyValueFilter[K, V] = {
         new KeyValueFilter[Bytes, Bytes] {
            override def accept(key: Bytes, value: Bytes, metadata: Metadata): Boolean = {
               val acceptedKey = if (dynamic) params.head.asInstanceOf[Bytes] else staticKey
               if (util.Arrays.equals(key, acceptedKey)) true else false
            }

         }
      }.asInstanceOf[KeyValueFilter[K, V]]
   }

}
