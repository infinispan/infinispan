package org.infinispan.server.hotrod.test

import java.lang.reflect.Method
import java.net.NetworkInterface
import java.util.concurrent.TimeUnit
import java.util.{Optional, List => JList, Map => JMap}

import org.infinispan.commons.api.BasicCacheContainer
import org.infinispan.configuration.cache.ConfigurationBuilder
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.server.hotrod._
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder

import scala.collection.JavaConversions._

/**
 * Test utils for Hot Rod tests.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
object HotRodTestingUtil {

   val EXPECTED_HASH_FUNCTION_VERSION: Byte = HotRodTestingUtils.EXPECTED_HASH_FUNCTION_VERSION

   def host: String = HotRodTestingUtils.host

   def serverPort: Int = HotRodTestingUtils.serverPort()

   def startHotRodServer(manager: EmbeddedCacheManager): HotRodServer =
      HotRodTestingUtils.startHotRodServer(manager)

   def startHotRodServer(manager: EmbeddedCacheManager, defaultCacheName: String): HotRodServer =
      HotRodTestingUtils.startHotRodServer(manager, defaultCacheName)

   def startHotRodServer(manager: EmbeddedCacheManager, proxyHost: String, proxyPort: Int): HotRodServer =
      HotRodTestingUtils.startHotRodServer(manager, proxyHost, proxyPort)

   def startHotRodServer(manager: EmbeddedCacheManager, port: Int): HotRodServer =
      HotRodTestingUtils.startHotRodServer(manager, port)

   def startHotRodServer(manager: EmbeddedCacheManager, port:Int, proxyHost: String, proxyPort: Int): HotRodServer =
      HotRodTestingUtils.startHotRodServer(manager, port, proxyHost, proxyPort)

   def startHotRodServer(manager: EmbeddedCacheManager, port: Int, idleTimeout: Int): HotRodServer =
      HotRodTestingUtils.startHotRodServer(manager, port, idleTimeout)

   def startHotRodServer(manager: EmbeddedCacheManager, port: Int, idleTimeout: Int, proxyHost: String, proxyPort: Int): HotRodServer =
      HotRodTestingUtils.startHotRodServer(manager, port, idleTimeout, proxyHost, proxyPort)

   def startHotRodServerWithDelay(manager: EmbeddedCacheManager, port: Int, delay: Long): HotRodServer =
      HotRodTestingUtils.startHotRodServerWithDelay(manager, port, delay)

   def startHotRodServer(manager: EmbeddedCacheManager, port: Int, idleTimeout: Int,
                         proxyHost: String, proxyPort: Int, delay: Long, defaultCacheName: String = BasicCacheContainer.DEFAULT_CACHE_NAME): HotRodServer =
      HotRodTestingUtils.startHotRodServer(manager, port, idleTimeout, proxyHost, proxyPort, delay, defaultCacheName)

   def startHotRodServer(manager: EmbeddedCacheManager, port: Int, builder: HotRodServerConfigurationBuilder): HotRodServer =
      HotRodTestingUtils.startHotRodServer(manager, port, builder)

   def startHotRodServer(manager: EmbeddedCacheManager, builder: HotRodServerConfigurationBuilder): HotRodServer =
      HotRodTestingUtils.startHotRodServer(manager, builder)

   def startHotRodServer(manager: EmbeddedCacheManager, port: Int, delay: Long, builder: HotRodServerConfigurationBuilder): HotRodServer =
      HotRodTestingUtils.startHotRodServer(manager, port, delay, builder)

   def startHotRodServer(manager: EmbeddedCacheManager, host: String, port: Int, delay: Long, builder: HotRodServerConfigurationBuilder): HotRodServer =
      HotRodTestingUtils.startHotRodServer(manager, host, port, delay, builder)

   def startHotRodServerWithoutTransport(): HotRodServer =
      HotRodTestingUtils.startHotRodServerWithoutTransport()

   def startHotRodServerWithoutTransport(builder: HotRodServerConfigurationBuilder): HotRodServer =
      HotRodTestingUtils.startHotRodServerWithoutTransport(builder)

   def startHotRodServer(manager: EmbeddedCacheManager, host: String, port: Int, delay: Long, perf: Boolean, builder: HotRodServerConfigurationBuilder): HotRodServer =
      HotRodTestingUtils.startHotRodServer(manager, host, port, delay, perf, builder)

   def getDefaultHotRodConfiguration: HotRodServerConfigurationBuilder =
      HotRodTestingUtils.getDefaultHotRodConfiguration

   def findNetworkInterfaces(loopback: Boolean): Iterator[NetworkInterface] =
      HotRodTestingUtils.findNetworkInterfaces(loopback)

   def startCrashingHotRodServer(manager: EmbeddedCacheManager, port: Int): HotRodServer =
      startHotRodServer(manager, port)

   def k(m: Method, prefix: String): Array[Byte] =
      HotRodTestingUtils.k(m, prefix)

   def v(m: Method, prefix: String): Array[Byte] =
      HotRodTestingUtils.v(m, prefix)

   def k(m: Method): Array[Byte] =
      HotRodTestingUtils.k(m)

   def v(m: Method): Array[Byte] =
      HotRodTestingUtils.v(m)

   def assertStatus(resp: TestResponse, expected: OperationStatus): Boolean =
      HotRodTestingUtils.assertStatus(resp, expected)

   def assertSuccess(resp: TestGetResponse, expected: Array[Byte]): Boolean =
      HotRodTestingUtils.assertSuccess(resp, expected)

   def assertByteArrayEquals(expected: Bytes, actual: Bytes): Unit =
      HotRodTestingUtils.assertByteArrayEquals(expected, actual)

   def assertSuccess(resp: TestGetWithVersionResponse, expected: Array[Byte], expectedVersion: Int): Boolean =
      HotRodTestingUtils.assertSuccess(resp, expected, expectedVersion)

   def assertSuccess(resp: TestGetWithMetadataResponse, expected: Array[Byte], expectedLifespan: Int, expectedMaxIdle: Int): Boolean =
      HotRodTestingUtils.assertSuccess(resp, expected, expectedLifespan, expectedMaxIdle)

   def assertKeyDoesNotExist(resp: TestGetResponse): Boolean =
      HotRodTestingUtils.assertKeyDoesNotExist(resp)

   def assertTopologyReceived(resp: AbstractTestTopologyAwareResponse, servers: List[HotRodServer],
                              expectedTopologyId : Int): Unit =
      HotRodTestingUtils.assertTopologyReceived(resp, servers, expectedTopologyId)

   def assertHashTopology20Received(topoResp: AbstractTestTopologyAwareResponse,
           servers: List[HotRodServer], cacheName: String, expectedTopologyId : Int): Unit =
      HotRodTestingUtils.assertHashTopology20Received(topoResp, servers, cacheName, expectedTopologyId)

   def assertHashTopology10Received(topoResp: AbstractTestTopologyAwareResponse, servers: List[HotRodServer],
                                    cacheName: String, expectedTopologyId : Int): Unit =
      HotRodTestingUtils.assertHashTopology10Received(topoResp, servers, cacheName, expectedTopologyId)

   def assertNoHashTopologyReceived(topoResp: AbstractTestTopologyAwareResponse, servers: List[HotRodServer],
                                    cacheName: String, expectedTopologyId : Int): Unit =
      HotRodTestingUtils.assertNoHashTopologyReceived(topoResp, servers, cacheName, expectedTopologyId)

   def assertHashTopology10Received(topoResp: AbstractTestTopologyAwareResponse,
                                    servers: List[HotRodServer], cacheName: String,
                                    expectedNumOwners: Int, expectedHashFct: Int, expectedHashSpace: Int,
                                    expectedTopologyId : Int): Unit =
      HotRodTestingUtils.assertHashTopology10Received(topoResp, servers, cacheName, expectedNumOwners, expectedHashFct,
         expectedHashSpace, expectedTopologyId)


   def assertHashTopologyReceived(topoResp: AbstractTestTopologyAwareResponse,
                                  servers: List[HotRodServer], cacheName : String,
                                  expectedNumOwners: Int, expectedVirtualNodes: Int,
                                  expectedTopologyId : Int): Unit =
      HotRodTestingUtils.assertHashTopologyReceived(topoResp, servers, cacheName, expectedNumOwners,
         expectedVirtualNodes, expectedTopologyId)

   def assertHashIds(hashIds: JMap[ServerAddress, JList[Integer]], servers: List[HotRodServer], cacheName: String): Unit =
      HotRodTestingUtils.assertHashIds(hashIds, servers, cacheName)

   def assertReplicatedHashIds(hashIds: Map[ServerAddress, JList[Integer]], servers: List[HotRodServer], cacheName: String): Unit =
      HotRodTestingUtils.assertReplicatedHashIds(hashIds, servers, cacheName)

   def getServerTopologyId(cm: EmbeddedCacheManager, cacheName: String): Int =
      HotRodTestingUtils.getServerTopologyId(cm, cacheName)

   def killClient(client: HotRodClient): Unit =
      HotRodTestingUtils.killClient(client).await(10, TimeUnit.SECONDS)

   def hotRodCacheConfiguration(): ConfigurationBuilder =
      HotRodTestingUtils.hotRodCacheConfiguration()

   def hotRodCacheConfiguration(base: ConfigurationBuilder): ConfigurationBuilder =
      HotRodTestingUtils.hotRodCacheConfiguration(base)

   def assertHotRodEquals(cm: EmbeddedCacheManager, key: Bytes, expectedValue: Bytes): InternalCacheEntry =
      HotRodTestingUtils.assertHotRodEquals(cm, key, expectedValue).asInstanceOf[InternalCacheEntry]

   def assertHotRodEquals(cm: EmbeddedCacheManager, cacheName: String,
           key: Bytes, expectedValue: Bytes): InternalCacheEntry =
      HotRodTestingUtils.assertHotRodEquals(cm, cacheName, key, expectedValue).asInstanceOf[InternalCacheEntry]

   def assertHotRodEquals(cm: EmbeddedCacheManager, key: String, expectedValue: String): InternalCacheEntry =
      HotRodTestingUtils.assertHotRodEquals(cm, key, expectedValue).asInstanceOf[InternalCacheEntry]

   def assertHotRodEquals(cm: EmbeddedCacheManager, cacheName: String,
           key: String, expectedValue: String): InternalCacheEntry =
      HotRodTestingUtils.assertHotRodEquals(cm, cacheName, key, expectedValue).asInstanceOf[InternalCacheEntry]

   def marshall(obj: Any): Array[Byte] =
      HotRodTestingUtils.marshall(obj)

   def unmarshall[T](key: Array[Byte]): T =
      HotRodTestingUtils.unmarshall(key)

   def withClientListener(filterFactory: NamedFactory = Optional.empty(), converterFactory: NamedFactory = Optional.empty(),
                          includeState: Boolean = false, useRawData: Boolean = true)(fn: () => Unit)
           (implicit listener: TestClientListener, client: HotRodClient): Unit =
      HotRodTestingUtils.withClientListener(client, listener, filterFactory, converterFactory, includeState, useRawData, fn)

   def withClientListener(client: HotRodClient, listener: TestClientListener,
           includeState: Boolean, filterFactory: NamedFactory, converterFactory: NamedFactory,
           useRawData: Boolean)
           (fn: () => Unit): Unit = {
      HotRodTestingUtils.withClientListener(client, listener, filterFactory, converterFactory, includeState, useRawData, fn)
   }
}
