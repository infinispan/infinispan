package org.infinispan.server.hotrod

import org.infinispan.config.Configuration
import java.lang.reflect.Method
import test.HotRodClient
import test.HotRodTestingUtil._
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.config.Configuration.CacheMode
import org.testng.Assert._
import org.testng.annotations.{AfterMethod, AfterClass, Test}
import org.infinispan.test.{TestingUtil, MultipleCacheManagersTest}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

@Test(groups = Array("functional"), testName = "server.hotrod.HotRodReplicationTest")
class HotRodReplicationTest extends MultipleCacheManagersTest {

   import HotRodServer._

   private val cacheName = "hotRodReplSync"
   private[this] var servers: List[HotRodServer] = List()
   private[this] var clients: List[HotRodClient] = List()

   @Test(enabled=false) // Disable explicitly to avoid TestNG thinking this is a test!!
   override def createCacheManagers {
      for (i <- 0 until 2) {
         val cm = addClusterEnabledCacheManager()
         cm.defineConfiguration(cacheName, createCacheConfig)
         cm.defineConfiguration(TopologyCacheName, createTopologyCacheConfig)
      }
      servers = servers ::: List(startHotRodServer(cacheManagers.get(0))) 
      servers = servers ::: List(startHotRodServer(cacheManagers.get(1), servers.head.getPort + 50))
      servers.foreach {s =>
         clients = new HotRodClient("127.0.0.1", s.getPort, cacheName, 60) :: clients
      }
   }

   @AfterClass(alwaysRun = true)
   override def destroy {
      log.debug("Test finished, close Hot Rod server", null)
      clients.foreach(_.stop)
      servers.foreach(_.stop)
      super.destroy // Stop the caches last so that at stoppage time topology cache can be updated properly
   }

   @AfterMethod(alwaysRun=true)
   override def clearContent() {
      // Do not clear cache between methods so that topology cache does not get cleared
   }

   private def createCacheConfig: Configuration = {
      val config = getDefaultClusteredConfig(Configuration.CacheMode.REPL_SYNC)
      config.setFetchInMemoryState(true)
      config
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

   def testReplicatedPut(m: Method) {
      val putSt = clients.head.put(k(m) , 0, 0, v(m)).status
      assertStatus(putSt, Success)
      assertSuccess(clients.tail.head.get(k(m), 0), v(m))
   }

   def testReplicatedPutIfAbsent(m: Method) {
      assertKeyDoesNotExist(clients.head.assertGet(m))
      assertKeyDoesNotExist(clients.tail.head.assertGet(m))
      var putSt = clients.head.putIfAbsent(k(m) , 0, 0, v(m)).status
      assertStatus(putSt, Success)
      assertSuccess(clients.tail.head.get(k(m), 0), v(m))
      assertStatus(clients.tail.head.putIfAbsent(k(m) , 0, 0, v(m, "v2-")).status, OperationNotExecuted)
   }

   def testReplicatedReplace(m: Method) {
      var status = clients.head.replace(k(m), 0, 0, v(m)).status
      assertStatus(status, OperationNotExecuted)
      status = clients.tail.head.replace(k(m), 0, 0, v(m)).status
      assertStatus(status , OperationNotExecuted)
      clients.tail.head.assertPut(m)
      status = clients.tail.head.replace(k(m), 0, 0, v(m, "v1-")).status
      assertStatus(status, Success)
      assertSuccess(clients.head.assertGet(m), v(m, "v1-"))
      status = clients.head.replace(k(m), 0, 0, v(m, "v2-")).status
      assertStatus(status, Success)
      assertSuccess(clients.tail.head.assertGet(m), v(m, "v2-"))
   }

   def testPingWithTopologyAwareClient(m: Method) {
      var resp = clients.head.ping
      assertStatus(resp.status, Success)
      assertEquals(resp.topologyResponse, None)
      resp = clients.tail.head.ping(1, 0)
      assertStatus(resp.status, Success)
      assertEquals(resp.topologyResponse, None)
      resp = clients.head.ping(2, 0)
      assertStatus(resp.status, Success)
      assertTopologyReceived(resp.topologyResponse.get)
      resp = clients.tail.head.ping(2, 1)
      assertStatus(resp.status, Success)
      assertTopologyReceived(resp.topologyResponse.get)
      resp = clients.tail.head.ping(2, 2)
      assertStatus(resp.status, Success)
      assertEquals(resp.topologyResponse, None)
   }

   private def assertTopologyReceived(topologyResp: AbstractTopologyResponse) {
      assertEquals(topologyResp.view.topologyId, 2)
      assertEquals(topologyResp.view.members.size, 2)
      assertAddressEquals(topologyResp.view.members.head, servers.head.getAddress)
      assertAddressEquals(topologyResp.view.members.tail.head, servers.tail.head.getAddress)
   }

   def testReplicatedPutWithTopologyChanges(m: Method) {
      var resp = clients.head.put(k(m) , 0, 0, v(m), 1, 0)
      assertStatus(resp.status, Success)
      assertEquals(resp.topologyResponse, None)
      assertSuccess(clients.tail.head.get(k(m), 0), v(m))
      resp = clients.head.put(k(m) , 0, 0, v(m, "v1-"), 2, 0)
      assertStatus(resp.status, Success)
      assertTopologyReceived(resp.topologyResponse.get)
      resp = clients.tail.head.put(k(m) , 0, 0, v(m, "v2-"), 2, 1)
      assertStatus(resp.status, Success)
      assertTopologyReceived(resp.topologyResponse.get)
      resp = clients.head.put(k(m) , 0, 0, v(m, "v3-"), 2, 2)
      assertStatus(resp.status, Success)
      assertEquals(resp.topologyResponse, None)
      assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v3-"))

      var cm = addClusterEnabledCacheManager()
      cm.defineConfiguration(cacheName, createCacheConfig)
      cm.defineConfiguration(TopologyCacheName, createTopologyCacheConfig)
      val newServer = startHotRodServer(cm, servers.tail.head.getPort + 25)
      servers = servers ::: List(newServer)

      resp = clients.head.put(k(m) , 0, 0, v(m, "v4-"), 2, 2)
      assertStatus(resp.status, Success)
      assertEquals(resp.topologyResponse.get.view.topologyId, 3)
      assertEquals(resp.topologyResponse.get.view.members.size, 3)
      assertAddressEquals(resp.topologyResponse.get.view.members.head, servers.head.getAddress)
      assertAddressEquals(resp.topologyResponse.get.view.members.tail.head, servers.tail.head.getAddress)
      assertAddressEquals(resp.topologyResponse.get.view.members.tail.tail.head, servers.tail.tail.head.getAddress)
      assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v4-"))

      servers.tail.tail.head.stop
      servers = servers.filterNot(_ == newServer)
      cm.stop

      resp = clients.head.put(k(m) , 0, 0, v(m, "v5-"), 2, 3)
      assertStatus(resp.status, Success)
      assertEquals(resp.topologyResponse.get.view.topologyId, 4)
      assertEquals(resp.topologyResponse.get.view.members.size, 2)
      assertAddressEquals(resp.topologyResponse.get.view.members.head, servers.head.getAddress)
      assertAddressEquals(resp.topologyResponse.get.view.members.tail.head, servers.tail.head.getAddress)
      assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v5-"))

      cm = addClusterEnabledCacheManager()
      cm.defineConfiguration(cacheName, createCacheConfig)
      cm.defineConfiguration(TopologyCacheName, createTopologyCacheConfig)
      val crashingServer = startCrashingHotRodServer(cm, servers.tail.head.getPort + 11)
      servers = servers ::: List(crashingServer)

      resp = clients.head.put(k(m) , 0, 0, v(m, "v6-"), 2, 4)
      assertStatus(resp.status, Success)
      assertEquals(resp.topologyResponse.get.view.topologyId, 5)
      assertEquals(resp.topologyResponse.get.view.members.size, 3)
      assertAddressEquals(resp.topologyResponse.get.view.members.head, servers.head.getAddress)
      assertAddressEquals(resp.topologyResponse.get.view.members.tail.head, servers.tail.head.getAddress)
      assertAddressEquals(resp.topologyResponse.get.view.members.tail.tail.head, servers.tail.tail.head.getAddress)
      assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v6-"))

      crashingServer.stop
      servers = servers.filterNot(_ == crashingServer)
      cm.stop
      TestingUtil.blockUntilViewsReceived(10000, true, manager(0), manager(1))

      resp = clients.head.put(k(m) , 0, 0, v(m, "v7-"), 2, 5)
      assertStatus(resp.status, Success)
      assertEquals(resp.topologyResponse.get.view.topologyId, 6)
      assertEquals(resp.topologyResponse.get.view.members.size, 2)
      assertAddressEquals(resp.topologyResponse.get.view.members.head, servers.head.getAddress)
      assertAddressEquals(resp.topologyResponse.get.view.members.tail.head, servers.tail.head.getAddress)
      assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v7-"))
   }

   private def assertAddressEquals(actual: TopologyAddress, expected: TopologyAddress) {
      assertEquals(actual.host, expected.host)
      assertEquals(actual.port, expected.port)
      assertEquals(actual.hostHashCode, expected.hostHashCode)
   }
}