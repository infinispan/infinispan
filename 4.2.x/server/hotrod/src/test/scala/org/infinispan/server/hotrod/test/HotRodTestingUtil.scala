package org.infinispan.server.hotrod.test

import java.util.concurrent.atomic.AtomicInteger
import java.lang.reflect.Method
import org.infinispan.server.core.Logging
import org.infinispan.server.hotrod.OperationStatus._
import org.testng.Assert._
import org.infinispan.util.Util
import org.infinispan.server.hotrod._
import org.infinispan.config.Configuration.CacheMode
import org.infinispan.config.Configuration
import org.infinispan.manager.EmbeddedCacheManager
import org.infinispan.server.core.Main._
import java.util.{Properties, Arrays}

/**
 * Test utils for Hot Rod tests.
 * 
 * @author Galder Zamarreño
 * @since 4.1
 */
object HotRodTestingUtil extends Logging {

   import HotRodTestingUtil._

   def host = "127.0.0.1"

   def startHotRodServer(manager: EmbeddedCacheManager): HotRodServer =
      startHotRodServer(manager, UniquePortThreadLocal.get.intValue)

   def startHotRodServer(manager: EmbeddedCacheManager, proxyHost: String, proxyPort: Int): HotRodServer =
      startHotRodServer(manager, UniquePortThreadLocal.get.intValue, 0, proxyHost, proxyPort)

   def startHotRodServer(manager: EmbeddedCacheManager, port: Int): HotRodServer =
      startHotRodServer(manager, port, 0)

   def startHotRodServer(manager: EmbeddedCacheManager, port:Int, proxyHost: String, proxyPort: Int): HotRodServer =
      startHotRodServer(manager, port, 0, proxyHost, proxyPort)

   def startHotRodServer(manager: EmbeddedCacheManager, port: Int, idleTimeout: Int): HotRodServer =
      startHotRodServer(manager, port, idleTimeout, host, port)

   def startHotRodServer(manager: EmbeddedCacheManager, port: Int, idleTimeout: Int, proxyHost: String, proxyPort: Int): HotRodServer = {
      val server = new HotRodServer {
         import HotRodServer._
         override protected def defineTopologyCacheConfig(cacheManager: EmbeddedCacheManager) {
            cacheManager.defineConfiguration(TopologyCacheName, createTopologyCacheConfig)
         }
      }
      server.start(getProperties(host, port, idleTimeout, proxyHost, proxyPort), manager)
      server
   }

   private def getProperties(host: String, port: Int, idleTimeout: Int, proxyHost: String, proxyPort: Int): Properties = {
      val properties = new Properties
      properties.setProperty(PROP_KEY_HOST, host)
      properties.setProperty(PROP_KEY_PORT, port.toString)
      properties.setProperty(PROP_KEY_IDLE_TIMEOUT, idleTimeout.toString)
      properties.setProperty(PROP_KEY_PROXY_HOST, proxyHost)
      properties.setProperty(PROP_KEY_PROXY_PORT, proxyPort.toString)
      properties
   }

   def startCrashingHotRodServer(manager: EmbeddedCacheManager, port: Int): HotRodServer = {
      val server = new HotRodServer {
         import HotRodServer._
         override protected def defineTopologyCacheConfig(cacheManager: EmbeddedCacheManager) {
            cacheManager.defineConfiguration(TopologyCacheName, createTopologyCacheConfig)
         }

         override protected def removeSelfFromTopologyView {
            // Empty to emulate a member that's crashed/unresponsive and has not executed removal,
            // but has been evicted from JGroups cluster.
         }
      }
      server.start(getProperties(host, port, 0, host, port), manager)
      server
   }

   def k(m: Method, prefix: String): Array[Byte] = {
      val bytes: Array[Byte] = (prefix + m.getName).getBytes
      trace("String {0} is converted to {1} bytes", prefix + m.getName, Util.printArray(bytes, true))
      bytes
   }

   def v(m: Method, prefix: String): Array[Byte] = k(m, prefix)

   def k(m: Method): Array[Byte] = k(m, "k-")

   def v(m: Method): Array[Byte] = v(m, "v-")

   def assertStatus(status: OperationStatus, expected: OperationStatus): Boolean = {
      val isSuccess = status == expected
      assertTrue(isSuccess, "Status should have been '" + expected + "' but instead was: " + status)
      isSuccess
   }

   def assertSuccess(resp: TestGetResponse, expected: Array[Byte]): Boolean = {
      assertStatus(resp.status, Success)
      val isArrayEquals = Arrays.equals(expected, resp.data.get)
      assertTrue(isArrayEquals, "Retrieved data should have contained " + Util.printArray(expected, true)
            + " (" + new String(expected) + "), but instead we received " + Util.printArray(resp.data.get, true) + " (" +  new String(resp.data.get) +")")
      isArrayEquals
   }

   def assertSuccess(resp: TestGetWithVersionResponse, expected: Array[Byte], expectedVersion: Int): Boolean = {
      assertTrue(resp.version != expectedVersion)
      assertSuccess(resp, expected)
   }

   def assertSuccess(resp: TestResponseWithPrevious, expected: Array[Byte]): Boolean = {
      assertStatus(resp.status, Success)
      val isSuccess = Arrays.equals(expected, resp.previous.get)
      assertTrue(isSuccess)
      isSuccess
   }

   def assertKeyDoesNotExist(resp: TestGetResponse): Boolean = {
      val status = resp.status
      assertTrue(status == KeyDoesNotExist, "Status should have been 'KeyDoesNotExist' but instead was: " + status)
      assertEquals(resp.data, None)
      status == KeyDoesNotExist
   }

   def assertTopologyReceived(topoResp: AbstractTopologyResponse, servers: List[HotRodServer]) {
      assertEquals(topoResp.view.topologyId, 2)
      assertEquals(topoResp.view.members.size, 2)
      assertAddressEquals(topoResp.view.members.head, servers.head.getAddress)
      assertAddressEquals(topoResp.view.members.tail.head, servers.tail.head.getAddress)
   }

   def assertAddressEquals(actual: TopologyAddress, expected: TopologyAddress) {
      assertEquals(actual.host, expected.host)
      assertEquals(actual.port, expected.port)
   }

   def assertHashTopologyReceived(topoResp: AbstractTopologyResponse, servers: List[HotRodServer], hashIds: List[Map[String, Int]]) {
      val hashTopologyResp = topoResp.asInstanceOf[HashDistAwareResponse]
      assertEquals(hashTopologyResp.view.topologyId, 2)
      assertEquals(hashTopologyResp.view.members.size, 2)
      assertAddressEquals(hashTopologyResp.view.members.head, servers.head.getAddress, hashIds.head)
      assertAddressEquals(hashTopologyResp.view.members.tail.head, servers.tail.head.getAddress, hashIds.tail.head)
      assertEquals(hashTopologyResp.numOwners, 2)
      assertEquals(hashTopologyResp.hashFunction, 1)
      assertEquals(hashTopologyResp.hashSpace, 10240)
   }

   def assertNoHashTopologyReceived(topoResp: AbstractTopologyResponse, servers: List[HotRodServer], hashIds: List[Map[String, Int]]) {
      val hashTopologyResp = topoResp.asInstanceOf[HashDistAwareResponse]
      assertEquals(hashTopologyResp.view.topologyId, 2)
      assertEquals(hashTopologyResp.view.members.size, 2)
      assertAddressEquals(hashTopologyResp.view.members.head, servers.head.getAddress, hashIds.head)
      assertAddressEquals(hashTopologyResp.view.members.tail.head, servers.tail.head.getAddress, hashIds.tail.head)
      assertEquals(hashTopologyResp.numOwners, 0)
      assertEquals(hashTopologyResp.hashFunction, 0)
      assertEquals(hashTopologyResp.hashSpace, 0)
   }

   def assertAddressEquals(actual: TopologyAddress, expected: TopologyAddress, expectedHashIds: Map[String, Int]) {
      assertEquals(actual.host, expected.host)
      assertEquals(actual.port, expected.port)
      assertEquals(actual.hashIds, expectedHashIds)
   }

   private def createTopologyCacheConfig: Configuration = {
      val topologyCacheConfig = new Configuration
      topologyCacheConfig.setCacheMode(CacheMode.REPL_SYNC)
      topologyCacheConfig.setSyncReplTimeout(10000) // Milliseconds
      topologyCacheConfig.setFetchInMemoryState(true) // State transfer required
      topologyCacheConfig.setSyncCommitPhase(true) // Only for testing, so that asserts work fine.
      topologyCacheConfig.setSyncRollbackPhase(true) // Only for testing, so that asserts work fine.
      topologyCacheConfig
   }
   
} 

object UniquePortThreadLocal extends ThreadLocal[Int] {
   private val uniqueAddr = new AtomicInteger(12311)
   override def initialValue: Int = uniqueAddr.getAndAdd(100)
}
