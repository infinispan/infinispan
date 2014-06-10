package org.infinispan.server.hotrod.event

import java.lang.reflect.Method
import java.util
import org.infinispan.configuration.cache.{CacheMode, ConfigurationBuilder}
import org.infinispan.filter.{Converter, KeyValueFilter}
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.metadata.Metadata
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.server.hotrod._
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder
import org.infinispan.server.hotrod.test.HotRodTestingUtil._
import org.infinispan.server.hotrod.test._
import org.infinispan.test.AbstractCacheTest._
import org.infinispan.test.TestingUtil
import org.testng.annotations.Test
import scala.collection.mutable.ListBuffer
import org.infinispan.notifications.cachelistener.event.Event

/**
 * @author Galder Zamarreño
 */
@Test(groups = Array("functional"))
abstract class AbstractHotRodClusterEventsTest extends HotRodMultiNodeTest {

   import AbstractHotRodClusterEventsTest._

   private[this] val filters = ListBuffer[AcceptedKeyFilterFactory]()
   private[this] val converters = ListBuffer[AcceptedKeyValueConverterFactory]()

   protected def cacheMode: CacheMode

   override protected def cacheName: String = "remote-clustered-events"

   override protected def nodeCount: Int = 3

   override protected def createCacheConfig: ConfigurationBuilder =
      hotRodCacheConfiguration(getDefaultClusteredCacheConfig(cacheMode, false))

   override protected def startTestHotRodServer(cacheManager: EmbeddedCacheManager, port: Int) = {
      val builder = new HotRodServerConfigurationBuilder
      builder.marshaller(null)
      filters += new AcceptedKeyFilterFactory()
      builder.keyValueFilterFactory("accepted-key-filter-factory", filters.head)
      converters += new AcceptedKeyValueConverterFactory()
      builder.converterFactory("accepted-keyvalue-converter-factory", converters.head)
      HotRodTestingUtil.startHotRodServer(cacheManager, port, builder)
   }

   def testEventForwarding(m: Method) {
      // Registering listener in one node and executing operations against
      // different nodes should still result in events received
      val client1 = clients.head
      val client2 = clients.tail.head
      val client3 = clients.tail.tail.head
      val listener1 = new EventLogListener
      withClientListener(client1, listener1, None, None) { () =>
         val key = k(m)
         client2.put(key, 0, 0, v(m))
         expectOnlyCreatedEvent(key)(listener1, anyCache())
         client3.put(key, 0, 0, v(m, "v2-"))
         expectOnlyModifiedEvent(key)(listener1, anyCache())
         client2.remove(key)
         expectOnlyRemovedEvent(key)(listener1, anyCache())
      }
   }

   def testNoEventsAfterRemovingListener(m: Method) {
      val client1 = clients.head
      val listener1 = new EventLogListener
      val key = k(m)
      withClientListener(client1, listener1, None, None) { () =>
         client1.put(key, 0, 0, v(m))
         expectOnlyCreatedEvent(key)(listener1, anyCache())
         client1.put(key, 0, 0, v(m, "v2-"))
         expectOnlyModifiedEvent(key)(listener1, anyCache())
         client1.remove(key)
         expectOnlyRemovedEvent(key)(listener1, anyCache())
      }
      client1.put(key, 0, 0, v(m))
      expectNoEvents()(listener1)
      client1.remove(key)
      expectNoEvents()(listener1)
   }

   def testNoEventsAfterRemovingListenerInDifferentNode(m: Method) {
      val client1 = clients.head
      val client2 = clients.tail.head
      val listener1 = new EventLogListener
      val key = k(m)
      assertStatus(client1.addClientListener(listener1, None, None), Success)
      try {
         client1.put(key, 0, 0, v(m))
         expectOnlyCreatedEvent(key)(listener1, anyCache())
         client1.put(key, 0, 0, v(m, "v2-"))
         expectOnlyModifiedEvent(key)(listener1, anyCache())
         client1.remove(key)
         expectOnlyRemovedEvent(key)(listener1, anyCache())
         // Use a client connected to a different node to attempt trying to remove listener
         client2.removeClientListener(listener1.getId)
         // The removal has no effect since the listener information is not clustered
         // Removal needs to be done in the node where the listener was added
         client1.put(key, 0, 0, v(m))
         expectOnlyCreatedEvent(key)(listener1, anyCache())
         client1.remove(key)
         expectOnlyRemovedEvent(key)(listener1, anyCache())
      } finally {
         assertStatus(client1.removeClientListener(listener1.getId), Success)
      }
   }

   def testClientDisconnectListenerCleanup(m: Method) {
      val client1 = clients.head
      val newClient = new HotRodClient("127.0.0.1", servers.tail.head.getPort, cacheName, 60, protocolVersion)
      val listener = new EventLogListener
      assertStatus(newClient.addClientListener(listener, None, None), Success)
      val key = k(m)
      client1.put(key, 0, 0, v(m))
      expectOnlyCreatedEvent(key)(listener, anyCache())
      newClient.stop.await()
      client1.put(k(m, "k2-"), 0, 0, v(m))
      expectNoEvents()(listener)
      client1.remove(key)
      client1.remove(k(m, "k2-"))
   }

   def testFailoverSendsEventsForNewContent(m: Method) {
      val client1 = clients.head
      val client2 = clients.tail.head
      val client3 = clients.tail.tail.head
      val listener1 = new EventLogListener
      val listener2 = new EventLogListener
      withClientListener(client1, listener1, None, None) { () =>
         val key = k(m)
         client2.put(key, 0, 0, v(m))
         expectOnlyCreatedEvent(key)(listener1, anyCache())
         client2.remove(key)
         expectOnlyRemovedEvent(key)(listener1, anyCache())
         val newServer = startClusteredServer(servers.last.getPort + 50)
         try {
            val client2 = new HotRodClient("127.0.0.1", newServer.getPort, cacheName, 60, protocolVersion)
            withClientListener(client2, listener2, None, None) { () =>
               val newKey = k(m, "k2-")
               client3.put(newKey, 0, 0, v(m))
               expectOnlyCreatedEvent(newKey)(listener1, anyCache())
               expectOnlyCreatedEvent(newKey)(listener2, anyCache())
               client1.put(newKey, 0, 0, v(m, "v2-"))
               expectOnlyModifiedEvent(newKey)(listener1, anyCache())
               expectOnlyModifiedEvent(newKey)(listener2, anyCache())
               client2.remove(newKey)
               expectOnlyRemovedEvent(newKey)(listener1, anyCache())
               expectOnlyRemovedEvent(newKey)(listener2, anyCache())
            }
         } finally {
            stopClusteredServer(newServer)
            TestingUtil.waitForRehashToComplete(
               cache(0, cacheName), cache(1, cacheName), cache(2, cacheName))
         }

         client3.put(key, 0, 0, v(m, "v2-"))
         expectOnlyCreatedEvent(key)(listener1, anyCache())
         expectNoEvents()(listener2)
         client3.put(key, 0, 0, v(m, "v3-"))
         expectOnlyModifiedEvent(key)(listener1, anyCache())
         expectNoEvents()(listener2)
         client2.remove(key)
         expectOnlyRemovedEvent(key)(listener1, anyCache())
         expectNoEvents()(listener2)
      }
   }

   def testFilteringInCluster(m: Method) {
      val client1 = clients(0)
      val client2 = clients(1)
      val listener1 = new EventLogListener
      val filterFactory = Some(("accepted-key-filter-factory", List.empty))
      val key1 = k(m, "k1-")
      withClusterClientListener(client1, listener1, filterFactory, None, Some(key1)) { () =>
         client2.put(k(m, "k-99"), 0, 0, v(m))
         expectNoEvents()(listener1)
         client2.remove(k(m, "k-99"))
         expectNoEvents()(listener1)
         client2.put(key1, 0, 0, v(m))
         expectOnlyCreatedEvent(key1)(listener1, anyCache())
         client1.remove(key1)
         expectOnlyRemovedEvent(key1)(listener1, anyCache())
      }
   }

   def testParameterBasedFilteringInCluster(m: Method) {
      val client1 = clients(0)
      val client2 = clients(1)
      val listener1 = new EventLogListener
      val dynamicAcceptedKey = Array[Byte](4, 5, 6)
      val filterFactory = Some(("accepted-key-filter-factory", List(dynamicAcceptedKey)))
      withClusterClientListener(client1, listener1, filterFactory, None) { () =>
         val key1 = k(m, "k1-")
         client2.put(k(m, "k-99"), 0, 0, v(m))
         expectNoEvents()(listener1)
         client2.remove(k(m, "k-99"))
         expectNoEvents()(listener1)
         client2.put(key1, 0, 0, v(m))
         expectNoEvents()(listener1)
         client2.put(dynamicAcceptedKey, 0, 0, v(m))
         expectOnlyCreatedEvent(dynamicAcceptedKey)(listener1, anyCache())
         client1.remove(dynamicAcceptedKey)
         expectOnlyRemovedEvent(dynamicAcceptedKey)(listener1, anyCache())
      }
   }

   def testConversionInCluster(m: Method) {
      val client1 = clients(0)
      val client2 = clients(1)
      val listener1 = new EventLogListener
      val converterFactory = Some(("accepted-keyvalue-converter-factory", List.empty))
      val key1 = k(m, "k1-")
      withClusterClientListener(client1, listener1, None, converterFactory, Some(key1)) { () =>
         val key1 = k(m, "k1-")
         val key1Length = key1.length.toByte
         val value = v(m)
         val valueLength = value.length.toByte

         val key99 = k(m, "k-99")
         client2.put(key99, 0, 0, v(m))
         expectSingleCustomEvent(Array(key99.length.toByte) ++ key99)(listener1, anyCache())
         client2.put(key1, 0, 0, v(m))
         expectSingleCustomEvent(Array(key1Length) ++ key1 ++ Array(valueLength) ++ value)(listener1, anyCache())
         client2.remove(key99)
         expectSingleCustomEvent(Array(key99.length.toByte) ++ key99)(listener1, anyCache())
         client2.remove(key1)
         expectSingleCustomEvent(Array(key1Length) ++ key1)(listener1, anyCache())
      }
   }

   def testParameterBasedConversionInCluster(m: Method) {
      val client1 = clients(0)
      val client2 = clients(1)
      val listener1 = new EventLogListener
      val convertedKey = Array[Byte](4, 5, 6)
      val convertedKeyLength = convertedKey.length.toByte
      val converteFactory = Some(("accepted-keyvalue-converter-factory", List(Array[Byte](4, 5, 6))))
      withClusterClientListener(client1, listener1, None, converteFactory) { () =>
         val key1 = k(m, "k1-")
         val key1Length = key1.length.toByte
         val value = v(m)
         val valueLength = value.length.toByte

         val key99 = k(m, "k-99")
         client2.put(key99, 0, 0, v(m))
         expectSingleCustomEvent(Array(key99.length.toByte) ++ key99)(listener1, anyCache())
         client2.put(key1, 0, 0, v(m))
         expectSingleCustomEvent(Array(key1Length) ++ key1)(listener1, anyCache())
         client2.put(convertedKey, 0, 0, v(m))
         expectSingleCustomEvent(Array(convertedKeyLength) ++ convertedKey ++ Array(valueLength) ++ value)(listener1, anyCache())
         client1.remove(convertedKey)
         expectSingleCustomEvent(Array(convertedKeyLength) ++ convertedKey)(listener1, anyCache())
      }
   }

   def testEventReplayAfterAddingListenerInCluster(m: Method) {
      val client1 = clients.head
      val client2 = clients.tail.head
      val client3 = clients.tail.tail.head
      val (k1, v1) = (k(m, "k1-"), v(m, "v1-"))
      val (k2, v2) = (k(m, "k2-"), v(m, "v2-"))
      val (k3, v3) = (k(m, "k3-"), v(m, "v3-"))
      client1.put(k1, 0, 0, v1)
      client2.put(k2, 0, 0, v2)
      client3.put(k3, 0, 0, v3)
      val listener1 = new EventLogListener
      withClientListener(client1, listener1, None, None) { () =>
         val keys = List(k1, k2, k3)
         expectUnorderedEvents(keys, Event.Type.CACHE_ENTRY_CREATED)(listener1, anyCache())
         client1.remove(k1)
         expectOnlyRemovedEvent(k1)(listener1, anyCache())
         client2.remove(k2)
         expectOnlyRemovedEvent(k2)(listener1, anyCache())
         client3.remove(k3)
         expectOnlyRemovedEvent(k3)(listener1, anyCache())
      }
   }

   private def anyCache(): Cache =
      cacheManagers.get(0).getCache[Bytes, Bytes](cacheName).getAdvancedCache

   private def withClusterClientListener(client: HotRodClient, listener: TestClientListener,
           filterFactory: NamedFactory, converterFactory: NamedFactory, 
           staticKey: Option[Bytes] = None)
           (fn: () => Unit): Unit = {
      filters.foreach(_.staticKey = staticKey)
      converters.foreach(_.staticKey = staticKey)
      assertStatus(client.addClientListener(listener, filterFactory, converterFactory), Success)
      try {
         fn()
      } finally {
         assertStatus(client.removeClientListener(listener.getId), Success)
         filters.foreach(_.staticKey = None)
         converters.foreach(_.staticKey = None)
      }
   }

}

object AbstractHotRodClusterEventsTest {

   class AcceptedKeyFilterFactory extends KeyValueFilterFactory with Serializable {
      var staticKey: Option[Bytes] = _
      override def getKeyValueFilter[K, V](params: Array[AnyRef]): KeyValueFilter[K, V] = {
         new KeyValueFilter[Bytes, Bytes] with Serializable {
            override def accept(key: Bytes, value: Bytes, metadata: Metadata): Boolean = {
               val checkKey = staticKey.getOrElse(params.head.asInstanceOf[Bytes])
               util.Arrays.equals(checkKey, key)
            }
         }
      }.asInstanceOf[KeyValueFilter[K, V]]
   }

   class AcceptedKeyValueConverterFactory extends ConverterFactory with Serializable {
      var staticKey: Option[Bytes] = _
      override def getConverter[K, V, C](params: Array[AnyRef]): Converter[K, V, C] = {
         new Converter[Bytes, Bytes, Bytes] with Serializable {
            override def convert(key: Bytes, value: Bytes, metadata: Metadata): Bytes = {
               val keyLength = key.length.toByte
               val checkKey = staticKey.getOrElse(params.head.asInstanceOf[Bytes])
               if (value == null || !util.Arrays.equals(checkKey, key))
                  Array(keyLength) ++ key
               else
                  Array(keyLength) ++ key ++ Array(value.length.toByte) ++ value
            }
         }.asInstanceOf[Converter[K, V, C]] // ugly but it works :|
      }
   }

}
