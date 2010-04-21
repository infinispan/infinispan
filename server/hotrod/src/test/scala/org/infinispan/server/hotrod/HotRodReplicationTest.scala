package org.infinispan.server.hotrod

import org.infinispan.test.MultipleCacheManagersTest
import org.infinispan.config.Configuration
import java.lang.reflect.Method
import test.HotRodClient
import test.HotRodTestingUtil._
import org.infinispan.server.hotrod.OperationStatus._
import org.infinispan.config.Configuration.CacheMode
import org.testng.Assert._
import org.testng.annotations.{AfterMethod, AfterClass, Test}

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
      super.destroy
      log.debug("Test finished, close Hot Rod server", null)
      clients.foreach(_.stop)
      servers.foreach(_.stop)
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
      assertEquals(topologyResp.view.members.head, TopologyAddress("127.0.0.1", servers.head.getPort, 0))
      assertEquals(topologyResp.view.members.tail.head, TopologyAddress("127.0.0.1", servers.tail.head.getPort, 0))
   }

   def testReplicatedPutWithTopologyAwareClient(m: Method) {
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

      val cm = addClusterEnabledCacheManager()
      cm.defineConfiguration(cacheName, createCacheConfig)
      cm.defineConfiguration(TopologyCacheName, createTopologyCacheConfig)
      servers = servers ::: List(startHotRodServer(cacheManagers.get(2), servers.tail.head.getPort + 25)) 

      resp = clients.head.put(k(m) , 0, 0, v(m, "v4-"), 2, 2)
      assertStatus(resp.status, Success)
      assertEquals(resp.topologyResponse.get.view.topologyId, 3)
      assertEquals(resp.topologyResponse.get.view.members.size, 3)
      assertEquals(resp.topologyResponse.get.view.members.head, TopologyAddress("127.0.0.1", servers.head.getPort, 0))
      assertEquals(resp.topologyResponse.get.view.members.tail.head, TopologyAddress("127.0.0.1", servers.tail.head.getPort, 0))
      assertEquals(resp.topologyResponse.get.view.members.tail.tail.head, TopologyAddress("127.0.0.1", servers.tail.tail.head.getPort, 0))
      assertSuccess(clients.tail.head.get(k(m), 0), v(m, "v4-"))

//      // TODO: Add stopping a server
//      servers.tail.tail.head.stop
   }

}